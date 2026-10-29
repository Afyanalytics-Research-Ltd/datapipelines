#!/usr/bin/env bash
# Full Eagle (Patcom) → V3 load for one target, run in the local airflow worker: base steps, then four groups in parallel
# (same layout as dags/eagle_to_v3_dag.py). Resumable: every step posts only rows V3 doesn't have yet (--only-missing).
#   nohup bash scripts_local_eagle_full.sh eagle > logs/eagle_full_eagle.out 2>&1 &
set -u
target=${1:-eagle}
cd ~/datapipelines
run() {   # run <tag> <steps…>
  local tag=$1; shift
  echo "$(date '+%F %T') start $tag: $*"
  docker exec -w /opt/airflow/pipelines datapipelines-airflow-worker-1 \
    python eagle_to_v3.py --target "$target" --only-missing --workers "${WORKERS:-12}" --steps "$@" > "logs/eagle_full_${target}_${tag}.log" 2>&1
  echo "$(date '+%F %T') done  $tag (exit $?)"
}
[ -n "${SKIP_BASE:-}" ] || run base users user_profiles insurance patients patient_insurance visits wards admissions ward_stays discharges \
         inpatient_notes catalogue procedure_prices stores products suppliers
run clinical vitals doctor_notes diagnoses &
run orders investigations lab_results &
run pharmacy stock prescriptions &
run money payments payment_details invoices appointments &
wait
echo "$(date '+%F %T') all groups finished"
