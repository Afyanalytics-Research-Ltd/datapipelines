#!/usr/bin/env python3
"""
ec2_ami_s3_backup_pipeline.py

Backs up a specific EC2 server to S3 as a portable AMI export, without
needing to already know its instance ID: the pipeline SSHes into the box,
resolves its instance-id and region from the EC2 instance-metadata service
(IMDSv2), then creates a no-reboot AMI and exports it to an S3 bucket.

This generalizes the "create_image -> wait -> create_store_image_task ->
poll" pattern (given an instance ID and bucket) into a pipeline driven by
SSH connection details for a single named host — useful for backing up a
production box you can reach over SSH but don't have the AWS console open
for.

PIPELINE
  1. SSH into the target host and query IMDSv2 for instance-id + region.
  2. Create a no-reboot AMI of that instance (Name: Backup-<id>-<epoch>).
  3. Wait for the AMI to reach 'available'.
  4. Export the AMI to the S3 bucket via create_store_image_task.
  5. Poll create/export progress until Completed / Failed / Cancelled.

REQUIREMENTS
  - `ssh` on PATH, with network access to the target host on port 22.
  - AWS credentials for the account that owns the instance, resolvable by
    boto3's default credential chain (~/.aws/credentials, env vars, or an
    instance profile if this script itself runs on EC2) — NOT sent over
    SSH; boto3 calls run locally, only instance-id/region discovery uses SSH.
  - IAM permissions: ec2:CreateImage, ec2:DescribeImages,
    ec2:CreateStoreImageTask, ec2:DescribeStoreImageTasks, plus S3 write
    access to the target bucket (AMI store/restore is a managed EC2
    feature — you do not need s3:PutObject yourself).

USAGE
  python ec2_ami_s3_backup_pipeline.py --yes
  python ec2_ami_s3_backup_pipeline.py --host 98.90.139.248 --user ubuntu \\
      --pem /mnt/c/Users/luthe/Downloads/AfyaOneProdNew.pem \\
      --bucket afya-snapshots-bucket --yes
  python ec2_ami_s3_backup_pipeline.py --instance-id i-0123456789abcdef0 \\
      --region us-east-1 --yes   # skip SSH discovery entirely

Omit --yes to only resolve and print what WOULD be backed up (dry run) —
creating/exporting an AMI of a production instance costs time and storage,
so the pipeline requires explicit confirmation before doing either.
"""

from __future__ import annotations

import argparse
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# --- Defaults for the AfyaOneProdNew host --------------------------------
# Override any of these with the matching CLI flag; nothing here is secret
# (the private key itself lives only in the .pem file on disk, never in
# this script or in source control).
SSH_HOST = "98.90.139.248"
SSH_USER = "ubuntu"
SSH_PEM_PATH = "/mnt/c/Users/luthe/Downloads/AfyaOneProdNew.pem"
BUCKET_NAME = "afya-snapshots-bucket"
POLL_INTERVAL_SECONDS = 30

IMDS_TOKEN_CMD = (
    'curl -s -X PUT "http://169.254.169.254/latest/api/token" '
    '-H "X-aws-ec2-metadata-token-ttl-seconds: 21600"'
)
IMDS_METADATA_CMD_TEMPLATE = (
    "TOKEN=$({token_cmd}); "
    'if [ -n "$TOKEN" ]; then HDR="-H \\"X-aws-ec2-metadata-token: $TOKEN\\""; else HDR=""; fi; '
    'INSTANCE_ID=$(eval curl -s $HDR http://169.254.169.254/latest/meta-data/instance-id); '
    'REGION=$(eval curl -s $HDR http://169.254.169.254/latest/meta-data/placement/region); '
    'echo "$INSTANCE_ID|$REGION"'
)


# ─── SSH discovery ───────────────────────────────────────────────────────────

def ensure_secure_pem(pem_path: str) -> str:
    """OpenSSH refuses to use a private key with group/other permission bits
    set. A .pem living on a /mnt/c (or similar drvfs/NTFS) mount under WSL
    always reports as world-readable — chmod on it doesn't stick, since NTFS
    doesn't carry real Unix permission bits. Copy it into a private, real
    Linux-filesystem location with 600 perms instead, and use that copy."""
    pem = Path(pem_path).expanduser().resolve()
    if not pem.exists():
        raise FileNotFoundError(f"PEM key not found: {pem_path}")

    try:
        os.chmod(pem, 0o600)
    except OSError:
        pass
    mode = stat.S_IMODE(pem.stat().st_mode)
    if mode & 0o077 == 0:
        return str(pem)  # already private enough (chmod worked, or it always was)

    secure_dir = Path.home() / ".ssh" / "backup-pipeline-keys"
    secure_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    secure_path = secure_dir / pem.name
    shutil.copy2(pem, secure_path)
    os.chmod(secure_path, 0o600)
    print(f"NOTE: {pem} has open permissions SSH won't accept (mode {oct(mode)}), likely "
          f"because it's on a Windows-mounted path. Copied it to {secure_path} (mode 0600) "
          f"and will use that copy instead.")
    return str(secure_path)


def run_ssh_command(host: str, user: str, pem_path: str, remote_cmd: str, timeout: int = 20) -> str:
    pem = ensure_secure_pem(pem_path)

    result = subprocess.run(
        [
            "ssh",
            "-i", pem,
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10",
            "-o", "BatchMode=yes",
            f"{user}@{host}",
            remote_cmd,
        ],
        capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"SSH command failed (exit {result.returncode}) on {user}@{host}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout.strip()


def resolve_instance_via_ssh(host: str, user: str, pem_path: str) -> tuple[str, str]:
    """SSH into the box and ask its own EC2 instance-metadata service (IMDSv2,
    falling back to unauthenticated IMDSv1 if the token request is blocked)
    for its instance-id and region — no hardcoded instance ID required."""
    remote_cmd = IMDS_METADATA_CMD_TEMPLATE.format(token_cmd=IMDS_TOKEN_CMD)
    output = run_ssh_command(host, user, pem_path, remote_cmd)
    if "|" not in output:
        raise RuntimeError(f"Unexpected metadata response from {host}: {output!r}")
    instance_id, region = (p.strip() for p in output.split("|", 1))
    if not instance_id or not region:
        raise RuntimeError(
            f"Could not resolve instance-id/region from {host} "
            f"(got instance_id={instance_id!r}, region={region!r}). "
            "Is this actually an EC2 instance with IMDS reachable?"
        )
    return instance_id, region


# ─── AMI create + S3 export ──────────────────────────────────────────────────

def create_ami(ec2_client, instance_id: str, host_label: str) -> str:
    image_name = f"Backup-{instance_id}-{int(time.time())}"
    print(f"Step: Creating AMI '{image_name}' from instance {instance_id} ({host_label})...")
    resp = ec2_client.create_image(
        InstanceId=instance_id,
        Name=image_name,
        NoReboot=True,  # avoid downtime on the production instance
        TagSpecifications=[{
            "ResourceType": "image",
            "Tags": [
                {"Key": "Name", "Value": image_name},
                {"Key": "SourceHost", "Value": host_label},
                {"Key": "Purpose", "Value": "s3-backup-pipeline"},
            ],
        }],
    )
    ami_id = resp["ImageId"]
    print(f"-> AMI creation started. Allocated AMI ID: {ami_id}")
    return ami_id


def wait_for_ami_available(ec2_client, ami_id: str) -> None:
    print("Step: Waiting for AMI to become available (this can take several minutes)...")
    waiter = ec2_client.get_waiter("image_available")
    waiter.wait(ImageIds=[ami_id])
    print("-> AMI is now available.")


def export_ami_to_s3(ec2_client, ami_id: str, bucket: str) -> None:
    print(f"Step: Exporting AMI {ami_id} to S3 bucket '{bucket}'...")
    ec2_client.create_store_image_task(ImageId=ami_id, Bucket=bucket)
    print("-> S3 export task triggered.")


def monitor_export(ec2_client, ami_id: str, poll_interval: int) -> bool:
    print("Step: Monitoring S3 export progress...")
    while True:
        tasks = ec2_client.describe_store_image_tasks(ImageIds=[ami_id])
        results = tasks.get("StoreImageTaskResults") or []
        if not results:
            print("-> No export task found yet, retrying...")
            time.sleep(poll_interval)
            continue

        task_info = results[0]
        status = task_info["StoreImageTaskStatus"]
        progress = task_info.get("ProgressPercentage", 0)
        print(f"-> Status: {status} | Progress: {progress}%")

        if status == "Completed":
            print(f"\nSuccess! Backup saved to s3://{task_info.get('Bucket', '?')}/")
            print(f"S3 object key: {ami_id}.bin")
            return True
        if status in ("Failed", "Cancelled"):
            print(f"\nError: export task ended with status: {status} "
                  f"({task_info.get('StoreTaskFailureReason', 'no reason given')})")
            return False

        time.sleep(poll_interval)


# ─── Orchestration ───────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> int:
    if args.instance_id and args.region:
        instance_id, region = args.instance_id, args.region
        host_label = args.host or instance_id
        print(f"Using explicit instance ID {instance_id} in {region} (skipped SSH discovery).")
    else:
        print(f"Resolving instance metadata over SSH from {args.user}@{args.host}...")
        try:
            instance_id, region = resolve_instance_via_ssh(args.host, args.user, args.pem)
        except (FileNotFoundError, RuntimeError, subprocess.TimeoutExpired) as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 1
        host_label = args.host
        print(f"-> Resolved instance_id={instance_id}, region={region}")

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    try:
        identity = session.client("sts", region_name=region).get_caller_identity()
        print(f"-> AWS identity: account={identity['Account']}, arn={identity['Arn']}"
              + (f" (profile={args.profile})" if args.profile else ""))
    except (BotoCoreError, ClientError) as e:
        print(f"ERROR: could not resolve AWS credentials"
              + (f" for profile '{args.profile}'" if args.profile else "")
              + f": {e}", file=sys.stderr)
        return 1

    print(f"\nPlan: back up {instance_id} ({host_label}) in {region} -> s3://{args.bucket}/")
    print("NOTE: if the AWS account above doesn't own this instance, CreateImage will fail with "
          "InvalidInstanceID.NotFound even though the instance ID itself is correct — "
          "use --profile to pick the right credentials.")
    if not args.yes:
        print("\nDry run only (no AMI created). Re-run with --yes to actually perform the backup.")
        return 0

    ec2_client = session.client("ec2", region_name=region)
    try:
        ami_id = create_ami(ec2_client, instance_id, host_label)
        wait_for_ami_available(ec2_client, ami_id)
        export_ami_to_s3(ec2_client, ami_id, args.bucket)
        ok = monitor_export(ec2_client, ami_id, args.poll_interval)
    except (BotoCoreError, ClientError) as e:
        print(f"ERROR: AWS API call failed: {e}", file=sys.stderr)
        return 1

    return 0 if ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=SSH_HOST, help=f"SSH host/IP of the server to back up (default: {SSH_HOST})")
    ap.add_argument("--user", default=SSH_USER, help=f"SSH username (default: {SSH_USER})")
    ap.add_argument("--pem", default=SSH_PEM_PATH, help=f"path to the .pem private key (default: {SSH_PEM_PATH})")
    ap.add_argument("--bucket", default=BUCKET_NAME, help=f"destination S3 bucket (default: {BUCKET_NAME})")
    ap.add_argument("--instance-id", default=None, help="skip SSH discovery and use this instance ID directly")
    ap.add_argument("--region", default=None, help="required if --instance-id is given (no SSH discovery to infer it)")
    ap.add_argument("--profile", default=None,
                     help="AWS CLI profile to use for boto3 calls (default: default credential chain). "
                          "Use this if 'InvalidInstanceID.NotFound' means your default credentials are "
                          "for the wrong AWS account.")
    ap.add_argument("--poll-interval", type=int, default=POLL_INTERVAL_SECONDS,
                     help=f"seconds between export-progress checks (default: {POLL_INTERVAL_SECONDS})")
    ap.add_argument("--yes", action="store_true",
                     help="actually create the AMI and export it (omit for a dry run that only resolves the target)")
    args = ap.parse_args()

    if bool(args.instance_id) != bool(args.region):
        print("ERROR: --instance-id and --region must be given together.", file=sys.stderr)
        sys.exit(2)

    sys.exit(run(args))


if __name__ == "__main__":
    main()
