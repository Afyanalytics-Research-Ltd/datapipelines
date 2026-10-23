"""Migrate silverwood users into V3 (org/facility + account from the environment). argv[1] = dry | post."""
import json, logging, sys
sys.path.insert(0, "/opt/airflow/dags")
logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
for noisy in ("snowflake.connector", "botocore", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
from pipelines_common import use_pipelines_dir
use_pipelines_dir(["silverwood"])
import v2_to_v3_api_migration as v2v3
import snowflake_to_v3_migration as m

dry = sys.argv[1] != "post"
out = m.run_one_table("silverwood", "users", dry_run=dry)
print("RESULT", json.dumps(out))
if not dry:
    org = v2v3.v3_login_org_cfg()
    users = v2v3._fetch_v3_records("users", org, service_name="core")
    print(f"V3 org {org.get('organization_id')} users now: {len(users)}")
    for u in users:
        print("  ", u.get("id"), u.get("username"), u.get("employee_number"), u.get("enforce_password_reset"), str(u.get("email"))[:55])
