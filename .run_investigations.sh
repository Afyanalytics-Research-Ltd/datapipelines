#!/bin/bash
cd /home/luther/datapipelines
exec env/bin/python facility_to_snowflake_fast_resume.py --facility kisumu_v3 --table-set old_system_history --only-tables evaluation_investigations --no-watermark-update > investigations.log 2>&1
