"""One-off: reload patients + next of kin from V2 now that V2 decrypts at source."""
import logging, os, sys
os.environ["KEYSET_TABLES"] = "patients,reception_patients_nok,evaluation_investigations"
logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
import facility_to_snowflake_fast_resume as loader
import reingest
FAC, TABLES = "kisumu_v3", ["patients", "reception_patients_nok"]
log = logging.getLogger("reload")
log.info("STEP 1 clearing old encrypted rows")
reingest.delete_snowflake_rows(FAC, TABLES)
for t in TABLES:
    for p in (loader.PAGE_STATE_DIR / FAC).glob(f"{t}__*"):
        if p.is_dir():
            for x in p.iterdir(): x.unlink()
            p.rmdir()
        else:
            p.unlink()
log.info("STEP 2 reloading from V2 (keyset)")
for table_set, ts in (("sheet", ["patients"]), ("old_system_history", ["reception_patients_nok"])):
    try:
        loader.run_pipeline(FAC, since="1970-01-01T00:00:00Z", only_tables=set(ts), table_set=table_set,
                            update_watermark=False, resume=True, skip_merge=True)
    except SystemExit as e:
        log.error("RELOAD FAILED for %s (exit %s)", ts, e.code); sys.exit(1)
log.info("STEP 3 rebuilding CLEAN views")
reingest.rebuild_views(FAC, TABLES)
log.info("RELOAD DONE")
