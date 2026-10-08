#!/bin/bash
# Resume dispensing, then the rest of visit destinations. Each step: wait until
# no other migration runs, run it, log its exit code, and re-run it (up to 3
# tries) if it died — safe, every step resumes and skips what's already in V3.
cd /home/luther/datapipelines
LOG=migration_queue.log
wait_clear() {
  while pgrep -f "python.*(snowflake_to_v3_migration|reingest|migrate_facility)\.py" >/dev/null; do sleep 30; done
}
run_step() {  # $1 label, $2 table
  for try in 1 2 3; do
    wait_clear
    echo "$(date +%H:%M:%S) STEP $1 (try $try)" >> $LOG
    RECORD_WORKERS=8 env/bin/python snowflake_to_v3_migration.py --facility kisumu_v3 --table "$2" >> $LOG 2>&1
    rc=$?
    echo "$(date +%H:%M:%S) STEP $1 exit code $rc" >> $LOG
    # 0 = done; 1 = finished with failed/held records (re-runnable later, not a crash)
    [ $rc -le 1 ] && return
  done
}
run_step "dispensing" inventory_evaluation_dispensing
run_step "visit destinations" evaluation_visit_destinations
echo "$(date +%H:%M:%S) QUEUE 2 DONE" >> $LOG
