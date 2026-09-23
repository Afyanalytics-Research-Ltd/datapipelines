# dags/ec2_ami_s3_backup_pipeline.py
"""
EC2 -> AMI -> S3 recurring backup pipeline.

Backs up a specific EC2 server to S3 as a portable AMI export, without
needing to already know its instance ID: the pipeline SSHes into the box,
resolves its instance-id and region from the EC2 instance-metadata service
(IMDSv2, falling back to IMDSv1), then creates a no-reboot AMI and exports
it to an S3 bucket. Old backup AMIs (tagged Purpose=s3-backup-pipeline) are
pruned afterwards so a recurring schedule doesn't accumulate storage
forever.

Adapted from the standalone script ec2_ami_s3_backup_pipeline.py (repo
root) — same discovery/create/export logic, restructured as DAG tasks. The
original script has no retention/cleanup step (it was meant to be run
on-demand); a cleanup_old_backups task was added here since this now runs
on a recurring schedule.

PIPELINE
  1. discover_instance    SSH into the target host and query IMDSv2 for
                           instance-id + region (skippable via Variables).
  2. create_ami            Create a no-reboot AMI of that instance
                           (Name: Backup-<instance_id>-<epoch>).
  3. wait_for_ami_available Wait for the AMI to reach 'available'.
  4. export_ami_to_s3      Export the AMI to the S3 bucket via
                           create_store_image_task and poll until
                           Completed / Failed / Cancelled.
  5. cleanup_old_backups   Deregister+delete-snapshot older backup AMIs of
                           this instance beyond the retention count.

Airflow Variables required (all optional — sensible defaults shown):
  EC2_BACKUP_SSH_HOST                 SSH host/IP of the server to back up
                                       (default: 98.90.139.248)
  EC2_BACKUP_SSH_USER                 SSH username (default: ubuntu)
  EC2_BACKUP_SSH_PEM_PATH             path, on the Airflow worker's
                                       filesystem, to the .pem private key
                                       (default: /mnt/c/Users/luthe/Downloads/AfyaOneProdNew.pem)
  EC2_BACKUP_S3_BUCKET                destination S3 bucket
                                       (default: afya-snapshots-bucket)
  EC2_BACKUP_INSTANCE_ID              optional: skip SSH discovery entirely
                                       and back up this instance ID
  EC2_BACKUP_REGION                   required if EC2_BACKUP_INSTANCE_ID is set
  EC2_BACKUP_POLL_INTERVAL_SECONDS    seconds between AMI-available /
                                       export-progress polls (default: 30)
  EC2_BACKUP_RETENTION_COUNT          number of most-recent backup AMIs to
                                       keep per source instance (default: 7)

Airflow Connections required:
  aws_default   AWS credentials for the account that owns the instance.
                IAM permissions needed: ec2:CreateImage, ec2:DescribeImages,
                ec2:CreateStoreImageTask, ec2:DescribeStoreImageTasks,
                ec2:DeregisterImage, ec2:DeleteSnapshot, ec2:DescribeSnapshots,
                sts:GetCallerIdentity, plus S3 write access to the target
                bucket (AMI store/restore is a managed EC2 feature — no
                s3:PutObject needed directly).

Other requirements:
  `ssh` must be on PATH on the Airflow worker, with network access to the
  target host on port 22 — only needed for the SSH-based discovery step;
  set EC2_BACKUP_INSTANCE_ID / EC2_BACKUP_REGION to skip it entirely.
  boto3 calls run locally on the worker using the aws_default connection's
  credentials; SSH is only ever used to read instance-id/region off IMDS.
"""
from __future__ import annotations

import logging
import os
import shutil
import stat
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook

log = logging.getLogger(__name__)

DAG_ID = "ec2_ami_s3_backup_pipeline"
AWS_CONN_ID = "aws_default"

# ── Defaults (overridable via Airflow Variables of the same purpose) ────
DEFAULT_SSH_HOST = "98.90.139.248"
DEFAULT_SSH_USER = "ubuntu"
DEFAULT_SSH_PEM_PATH = "/mnt/c/Users/luthe/Downloads/AfyaOneProdNew.pem"
DEFAULT_BUCKET_NAME = "afya-snapshots-bucket"
DEFAULT_POLL_INTERVAL_SECONDS = 30
DEFAULT_RETENTION_COUNT = 7

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

BACKUP_PURPOSE_TAG = "s3-backup-pipeline"


# ── SSH discovery helpers ────────────────────────────────────────────────
def _ensure_secure_pem(pem_path: str) -> str:
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
    log.info(
        "%s has open permissions SSH won't accept (mode %s), likely because it's on a "
        "Windows-mounted path. Copied it to %s (mode 0600) and will use that copy instead.",
        pem, oct(mode), secure_path,
    )
    return str(secure_path)


def _run_ssh_command(host: str, user: str, pem_path: str, remote_cmd: str, timeout: int = 20) -> str:
    pem = _ensure_secure_pem(pem_path)

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


def _resolve_instance_via_ssh(host: str, user: str, pem_path: str) -> tuple[str, str]:
    """SSH into the box and ask its own EC2 instance-metadata service (IMDSv2,
    falling back to unauthenticated IMDSv1 if the token request is blocked)
    for its instance-id and region — no hardcoded instance ID required."""
    remote_cmd = IMDS_METADATA_CMD_TEMPLATE.format(token_cmd=IMDS_TOKEN_CMD)
    output = _run_ssh_command(host, user, pem_path, remote_cmd)
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


def _ec2_client(region: str):
    return EC2Hook(aws_conn_id=AWS_CONN_ID, region_name=region).get_conn()


# ── DAG task callables ───────────────────────────────────────────────────
def discover_instance(**context) -> dict:
    """Resolve the target instance-id/region, either from Variables
    (EC2_BACKUP_INSTANCE_ID / EC2_BACKUP_REGION, skipping SSH entirely) or
    by SSHing in and reading IMDS."""
    explicit_instance_id = Variable.get("EC2_BACKUP_INSTANCE_ID", default_var=None)
    explicit_region = Variable.get("EC2_BACKUP_REGION", default_var=None)
    host = Variable.get("EC2_BACKUP_SSH_HOST", default_var=DEFAULT_SSH_HOST)

    if explicit_instance_id and explicit_region:
        instance_id, region = explicit_instance_id, explicit_region
        host_label = host or instance_id
        log.info("Using explicit instance ID %s in %s (skipped SSH discovery).", instance_id, region)
    else:
        user = Variable.get("EC2_BACKUP_SSH_USER", default_var=DEFAULT_SSH_USER)
        pem_path = Variable.get("EC2_BACKUP_SSH_PEM_PATH", default_var=DEFAULT_SSH_PEM_PATH)
        log.info("Resolving instance metadata over SSH from %s@%s...", user, host)
        instance_id, region = _resolve_instance_via_ssh(host, user, pem_path)
        host_label = host
        log.info("Resolved instance_id=%s, region=%s", instance_id, region)

    try:
        sts = EC2Hook(aws_conn_id=AWS_CONN_ID, region_name=region).get_client_type("sts")
        identity = sts.get_caller_identity()
        log.info("AWS identity: account=%s, arn=%s", identity["Account"], identity["Arn"])
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Could not resolve AWS credentials via connection '{AWS_CONN_ID}': {e}") from e

    log.info(
        "Plan: back up %s (%s) in %s. NOTE: if the AWS account above doesn't own this "
        "instance, CreateImage will fail with InvalidInstanceID.NotFound even though the "
        "instance ID is correct — fix the aws_default connection's credentials.",
        instance_id, host_label, region,
    )
    return {"instance_id": instance_id, "region": region, "host_label": host_label}


def create_ami(**context) -> dict:
    ti = context["ti"]
    disc = ti.xcom_pull(task_ids="discover_instance")
    instance_id, region, host_label = disc["instance_id"], disc["region"], disc["host_label"]

    image_name = f"Backup-{instance_id}-{int(time.time())}"
    log.info("Creating AMI '%s' from instance %s (%s)...", image_name, instance_id, host_label)
    ec2 = _ec2_client(region)
    resp = ec2.create_image(
        InstanceId=instance_id,
        Name=image_name,
        NoReboot=True,  # avoid downtime on the production instance
        TagSpecifications=[{
            "ResourceType": "image",
            "Tags": [
                {"Key": "Name", "Value": image_name},
                {"Key": "SourceHost", "Value": host_label},
                {"Key": "SourceInstanceId", "Value": instance_id},
                {"Key": "Purpose", "Value": BACKUP_PURPOSE_TAG},
            ],
        }],
    )
    ami_id = resp["ImageId"]
    log.info("AMI creation started. Allocated AMI ID: %s", ami_id)
    return {"ami_id": ami_id, "instance_id": instance_id, "region": region, "host_label": host_label}


def wait_for_ami_available(**context) -> dict:
    ti = context["ti"]
    created = ti.xcom_pull(task_ids="create_ami")
    ami_id, region = created["ami_id"], created["region"]

    poll_interval = int(Variable.get("EC2_BACKUP_POLL_INTERVAL_SECONDS", default_var=DEFAULT_POLL_INTERVAL_SECONDS))
    log.info("Waiting for AMI %s to become available (this can take several minutes)...", ami_id)
    ec2 = _ec2_client(region)
    waiter = ec2.get_waiter("image_available")
    waiter.wait(ImageIds=[ami_id], WaiterConfig={"Delay": poll_interval, "MaxAttempts": 120})
    log.info("AMI %s is now available.", ami_id)
    return created


def export_ami_to_s3(**context) -> dict:
    ti = context["ti"]
    created = ti.xcom_pull(task_ids="create_ami")
    ami_id, region = created["ami_id"], created["region"]
    bucket = Variable.get("EC2_BACKUP_S3_BUCKET", default_var=DEFAULT_BUCKET_NAME)
    poll_interval = int(Variable.get("EC2_BACKUP_POLL_INTERVAL_SECONDS", default_var=DEFAULT_POLL_INTERVAL_SECONDS))

    ec2 = _ec2_client(region)
    log.info("Exporting AMI %s to S3 bucket '%s'...", ami_id, bucket)
    ec2.create_store_image_task(ImageId=ami_id, Bucket=bucket)
    log.info("S3 export task triggered.")

    log.info("Monitoring S3 export progress...")
    while True:
        tasks = ec2.describe_store_image_tasks(ImageIds=[ami_id])
        results = tasks.get("StoreImageTaskResults") or []
        if not results:
            log.info("No export task found yet, retrying...")
            time.sleep(poll_interval)
            continue

        task_info = results[0]
        status = task_info["StoreImageTaskStatus"]
        progress = task_info.get("ProgressPercentage", 0)
        log.info("Status: %s | Progress: %s%%", status, progress)

        if status == "Completed":
            s3_bucket = task_info.get("Bucket", bucket)
            s3_key = f"{ami_id}.bin"
            log.info("Success! Backup saved to s3://%s/%s", s3_bucket, s3_key)
            return {**created, "s3_bucket": s3_bucket, "s3_key": s3_key}
        if status in ("Failed", "Cancelled"):
            raise RuntimeError(
                f"Export task for AMI {ami_id} ended with status {status}: "
                f"{task_info.get('StoreTaskFailureReason', 'no reason given')}"
            )

        time.sleep(poll_interval)


def cleanup_old_backups(**context) -> None:
    """Prune backup AMIs of this instance beyond the retention count. The
    original standalone script had no cleanup step (it was run on-demand);
    this is added here so a recurring schedule doesn't accumulate AMIs and
    their backing snapshots indefinitely."""
    ti = context["ti"]
    created = ti.xcom_pull(task_ids="create_ami")
    instance_id, region = created["instance_id"], created["region"]
    retention_count = int(Variable.get("EC2_BACKUP_RETENTION_COUNT", default_var=DEFAULT_RETENTION_COUNT))

    ec2 = _ec2_client(region)
    images = ec2.describe_images(
        Owners=["self"],
        Filters=[
            {"Name": "tag:Purpose", "Values": [BACKUP_PURPOSE_TAG]},
            {"Name": "tag:SourceInstanceId", "Values": [instance_id]},
        ],
    ).get("Images", [])

    images.sort(key=lambda img: img.get("CreationDate", ""), reverse=True)
    stale = images[retention_count:]

    if not stale:
        log.info(
            "Nothing to prune: %d backup AMI(s) for %s, retention is %d.",
            len(images), instance_id, retention_count,
        )
        return

    for img in stale:
        ami_id = img["ImageId"]
        snapshot_ids = [
            bdm["Ebs"]["SnapshotId"]
            for bdm in img.get("BlockDeviceMappings", [])
            if "Ebs" in bdm and bdm["Ebs"].get("SnapshotId")
        ]
        try:
            log.info("Deregistering stale backup AMI %s (created %s)...", ami_id, img.get("CreationDate"))
            ec2.deregister_image(ImageId=ami_id)
            for snap_id in snapshot_ids:
                log.info("Deleting backing snapshot %s for %s...", snap_id, ami_id)
                ec2.delete_snapshot(SnapshotId=snap_id)
        except (BotoCoreError, ClientError) as e:
            log.warning("Failed to prune AMI %s: %s", ami_id, e)

    log.info(
        "Pruned %d stale backup AMI(s) for %s, kept %d most recent.",
        len(stale), instance_id, min(retention_count, len(images)),
    )


# ── DAG definition ────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["ec2", "backup", "s3"],
) as dag:

    t_discover = PythonOperator(
        task_id="discover_instance",
        python_callable=discover_instance,
    )
    t_create = PythonOperator(
        task_id="create_ami",
        python_callable=create_ami,
    )
    t_wait = PythonOperator(
        task_id="wait_for_ami_available",
        python_callable=wait_for_ami_available,
    )
    t_export = PythonOperator(
        task_id="export_ami_to_s3",
        python_callable=export_ami_to_s3,
    )
    t_cleanup = PythonOperator(
        task_id="cleanup_old_backups",
        python_callable=cleanup_old_backups,
    )

    t_discover >> t_create >> t_wait >> t_export >> t_cleanup
