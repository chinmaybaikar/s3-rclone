import boto3
import subprocess
import os
import json
import time
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

NUM_PODS = 4
SECRET_NAME = "aws-credentials"
PVC_NAME = "sharedfs-mount"
S3_BUCKET = "crusoe-solutions-engineering/c4-dataset/en"
MASTER_POD_NAME = "rclone-master"
MANIFEST_DIR = "/manifests"
INSTANCE_CLASS = "s1a"

def create_k8s_secret():
    
    logging.info(f"Creating or updating Kubernetes secret '{SECRET_NAME}'...")
    cmd = [
        "kubectl", "create", "secret", "generic", SECRET_NAME,
        f"--from-literal=AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}",
        f"--from-literal=AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}",
        "--dry-run=client", "-o", "yaml"
    ]
    proc = subprocess.run(cmd, capture_output=True, check=True)
    subprocess.run(["kubectl", "apply", "-f", "-"], input=proc.stdout, check=True)
    logging.info("Kubernetes secret created or updated.")

def list_s3_files(bucket_name, prefix=''):
    logging.info(f"Listing files in S3 bucket {bucket_name}, prefix: {prefix}...")
    try:
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        s3 = session.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        files = []
        for page in pages:
            for obj in page.get('Contents', []):
                files.append({'key': obj['Key'], 'size': obj['Size']})
        logging.info(f"Found {len(files)} files in S3 bucket {bucket_name}, prefix: {prefix}.")
        return files
    except ClientError as e:
        logging.error(f"Error listing files: {e}")
        return []

def split_manifests(files, num_manifests, bucket_name):
    logging.info(f"Splitting into {num_manifests} balanced manifests...")
    files.sort(key=lambda x: x['size'], reverse=True)
    manifests = [[] for _ in range(num_manifests)]
    manifest_sizes = [0] * num_manifests
    manifest_files_content = {}

    for file in files:
        idx = manifest_sizes.index(min(manifest_sizes))
        manifests[idx].append(file['key'])
        manifest_sizes[idx] += file['size']

    for i, manifest in enumerate(manifests):
        content = "\n".join(manifest) + "\n"
        filename = f"manifest-part-{i}.txt"
        manifest_files_content[filename] = content
        logging.info(f"{filename}: {len(manifest)} files, {manifest_sizes[i]} bytes")
    return manifest_files_content

def create_master_pod_and_copy_manifests(manifest_files_content, bucket_name):
    logging.info(f"Creating rclone-master pod '{MASTER_POD_NAME}'...")
    master_pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": MASTER_POD_NAME
        },
        "spec": {
            "volumes": [
                {
                    "name": "myvolume",
                    "persistentVolumeClaim": {
                        "claimName": PVC_NAME,
                        "readOnly": False
                    }
                }
            ],
            "containers": [
                {
                    "name": "rclone-master",
                    "image": "busybox",
                    "command": ["/bin/sh", "-c", "while true; do sleep 3600; done"],
                    "volumeMounts": [
                        {
                            "mountPath": MANIFEST_DIR,
                            "name": "myvolume"
                        }
                    ]
                }
            ]
        }
    }

    subprocess.run([
        "kubectl", "apply", "-f", "-",
    ], input=json.dumps(master_pod_manifest).encode(), check=True)
    logging.info(f"rclone-master pod '{MASTER_POD_NAME}' created.")

    logging.info(f"Waiting for pod '{MASTER_POD_NAME}' to be running...")
    subprocess.run([
        "kubectl", "wait", "--for=condition=Ready", f"pod/{MASTER_POD_NAME}", "--timeout=600s"
    ], check=True)
    logging.info(f"Pod '{MASTER_POD_NAME}' is running.")

    logging.info(f"Copying manifest files to pod '{MASTER_POD_NAME}:{MANIFEST_DIR}'...")
    for filename, content in manifest_files_content.items():
        with open(filename, "w") as tmp_file:
            tmp_file.write(content)
        subprocess.run([
            "kubectl", "cp", filename, f"{MASTER_POD_NAME}:{MANIFEST_DIR}/{filename}"
        ], check=True)
        os.remove(filename)
        print(f"  Copied '{filename}' to '{MASTER_POD_NAME}:{MANIFEST_DIR}'")
    logging.info("Manifest files copied to the rclone-master pod.")

def launch_worker_pods(num_pods, bucket_name):
    logging.info(f"Launching {num_pods} rclone-worker pods...")
    for i in range(num_pods):
        pod_name = f"rclone-worker-{i}"
        manifest_file = f"/data/manifest-part-{i}.txt"

        # Compose the rclone command for this worker
        # S3 remote is 's3:{bucket_name}' (rclone uses AWS env vars)
        rclone_command = [
            "sh", "-c",
            (
                "mkdir -p /root/.config/rclone && "
                "echo '[s3]\n"
                "type = s3\n"
                "provider = AWS\n"
                "env_auth = true' > /root/.config/rclone/rclone.conf && "
                f"rclone copy --files-from {manifest_file} s3:{bucket_name} /data "
                "--s3-chunk-size=64M --fast-list --buffer-size=32M --transfers=80 --multi-thread-streams=80 -P"
            )
        ]

        overrides = {
            "spec": {
                "hostNetwork": True,
                "nodeSelector": {
                    "crusoe.ai/instance.class": INSTANCE_CLASS
                },
                "containers": [
                    {
                        "name": "rclone-worker",
                        "image": "rclone/rclone",
                        "command": rclone_command,
                        "volumeMounts": [
                            {
                                "mountPath": "/data",
                                "name": "myvolume"
                            }
                        ],
                        "env": [
                            {
                                "name": "AWS_ACCESS_KEY_ID",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": SECRET_NAME,
                                        "key": "AWS_ACCESS_KEY_ID"
                                    }
                                }
                            },
                            {
                                "name": "AWS_SECRET_ACCESS_KEY",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": SECRET_NAME,
                                        "key": "AWS_SECRET_ACCESS_KEY"
                                    }
                                }
                            }
                        ]
                    }
                ],
                "volumes": [
                    {
                        "name": "myvolume",
                        "persistentVolumeClaim": {
                            "claimName": PVC_NAME,
                            "readOnly": False
                        }
                    }
                ]
            }
        }

        logging.info(f"Launching pod {pod_name} with manifest '{manifest_file}'...")
        subprocess.run([
            "kubectl", "run", pod_name,
            "--restart=Never",
            "--overrides", json.dumps(overrides),
            "--image=rclone/rclone",
            "--labels=app=rclone-worker"
        ], check=True)
    logging.info("All worker pods launched.")

def wait_for_pods_completion(label_selector="app=rclone-worker", poll_interval=15):
    logging.info("Waiting for rclone-worker pods to complete...")
    backoff = poll_interval
    while True:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-l", label_selector, "-o", "json"],
            capture_output=True, text=True, check=True
        )
        pods = json.loads(result.stdout)["items"]
        incomplete = [p["metadata"]["name"] for p in pods if p["status"]["phase"] not in ("Succeeded", "Failed")]
        if not incomplete:
            logging.info("All pods have completed.")
            break
        logging.info(f"Still running: {incomplete}")
        time.sleep(backoff)
        backoff = min(backoff * 2, 10)  # exponential backoff, max 60s

def delete_pods(label_selector="app=rclone-worker"):
    logging.info("Deleting all rclone-worker pods...")
    subprocess.run(
        ["kubectl", "delete", "pod", "-l", label_selector],
        check=True
    )

def delete_master_pod():
    logging.info(f"Deleting rclone-master pod '{MASTER_POD_NAME}'...")
    try:
        subprocess.run(
            ["kubectl", "delete", "pod", MASTER_POD_NAME],
            check=True
        )
        logging.info(f"rclone-master pod '{MASTER_POD_NAME}' deleted.")
    except subprocess.CalledProcessError as e:
        if b"not found" in e.stderr:
            logging.info(f"rclone-master pod '{MASTER_POD_NAME}' not found.")
        else:
            raise

def main():
    create_k8s_secret()
    parts = S3_BUCKET.split('/')
    bucket_name = parts[0]
    prefix = '/'.join(parts[1:]) + '/' if len(parts) > 1 else ''
    files = list_s3_files(bucket_name, prefix)
    manifest_files_content = split_manifests(files, NUM_PODS, prefix)
    create_master_pod_and_copy_manifests(manifest_files_content, bucket_name)
    time.sleep(5)
    launch_worker_pods(NUM_PODS, bucket_name)
    wait_for_pods_completion()
    delete_master_pod()
    # delete_pods()
    

if __name__ == "__main__":
    main()
