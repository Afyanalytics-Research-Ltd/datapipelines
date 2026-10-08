#!/bin/bash
# Runs each step only when NO other migration is running (re-checked before every step).
cd /home/luther/datapipelines
LOG=migration_queue.log
wait_clear() { while pgrep -f "python.*(snowflake_to_v3_migration|reingest)\.py" >/dev/null; do sleep 30; done; }
run_step() {  # $1 label, rest = command
  local label="$1"; shift
  wait_clear
  echo "$(date +%H:%M:%S) STEP $label" >> $LOG
  "$@" >> $LOG 2>&1
}
echo "$(date +%H:%M:%S) queue (re)started — waits for other migrations before each step" >> $LOG
run_step "store mapping"        env/bin/python .map_stores.py
run_step "dispensing"           env RECORD_WORKERS=8 env/bin/python snowflake_to_v3_migration.py --facility kisumu_v3 --table inventory_evaluation_dispensing
run_step "visit destinations"   env RECORD_WORKERS=8 env/bin/python snowflake_to_v3_migration.py --facility kisumu_v3 --table evaluation_visit_destinations
echo "$(date +%H:%M:%S) QUEUE DONE" >> $LOG
