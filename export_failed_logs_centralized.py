import os
from pathlib import Path
import zipfile
from datetime import datetime

WEIRD_COLON = "\uf03a"  # Fix weird copied colon character


def normalize_path(p: str) -> str:
    return p.replace(WEIRD_COLON, ":")


def build_log_path(base_dir, dag_id, run_id, task_id, map_index, attempt=1):
    base_dir = normalize_path(base_dir)
    run_id = normalize_path(run_id)

    return (
        Path(base_dir)
        / f"dag_id={dag_id}"
        / f"run_id={run_id}"
        / f"task_id={task_id}"
        / f"map_index={map_index}"
        / f"attempt={attempt}.log"
    )


def export_and_zip_combined_logs(
    base_dir,
    dag_id,
    run_id,
    task_id,
    failed_map_indices,
    attempt=1,
    output_folder="./failed_mapped_logs",
):
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    combined_file = output_path / f"{dag_id}__{task_id}__COMBINED_FAILED.log"

    with combined_file.open("w", encoding="utf-8") as combined:

        combined.write("=" * 120 + "\n")
        combined.write(f"COMBINED FAILED LOGS\n")
        combined.write(f"DAG: {dag_id}\n")
        combined.write(f"TASK: {task_id}\n")
        combined.write(f"RUN_ID: {run_id}\n")
        combined.write(f"Generated At: {datetime.utcnow()} UTC\n")
        combined.write("=" * 120 + "\n\n")

        for idx in failed_map_indices:
            log_path = build_log_path(
                base_dir, dag_id, run_id, task_id, idx, attempt
            )

            combined.write("\n" + "#" * 120 + "\n")
            combined.write(f"MAP_INDEX: {idx}\n")
            combined.write(f"PATH: {log_path}\n")
            combined.write("#" * 120 + "\n\n")

            if log_path.exists():
                content = log_path.read_text(errors="replace")
                combined.write(content)
                combined.write("\n\n")
                print(f"[OK] Appended map_index={idx}")
            else:
                combined.write("[MISSING LOG FILE]\n\n")
                print(f"[MISSING] {log_path}")

    # Zip the single combined file
    zip_path = output_path / f"{dag_id}__{task_id}__FAILED_LOGS.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(combined_file, arcname=combined_file.name)

    print("\n✅ Combined log created:")
    print(combined_file.resolve())

    print("\n✅ ZIP created:")
    print(zip_path.resolve())

    return zip_path


if __name__ == "__main__":

    BASE_DIR = "logs"
    DAG_ID = "facility_api_to_snowflake"
    # RUN_ID = "manual__2026-03-03T06:37:45.749778+00:00"
    RUN_ID = "manual__2026-03-03T10:08:59.139751+00:00"
    TASK_ID = "extract_to_s3"

    FAILED_MAP_INDICES = [225,221,222,224,
                          220,213,211,210,212,
                          209,207,206,205,204,
                          203,201,202,200,193,
                          181,180,179,169,167,
                          166,164,163,150,66,
                          137,136,135,134,133,
                          129,123,119,122,121,
                          120,118,21,43,50,46,
                          53,54,68,69,67,70,
                          75,78,82,76,77,89,
                          96,101,103,110,108,
                          113,114,10,9,11,18]  # <-- put your failed indices here

    ATTEMPT = 1

    export_and_zip_combined_logs(
        base_dir=BASE_DIR,
        dag_id=DAG_ID,
        run_id=RUN_ID,
        task_id=TASK_ID,
        failed_map_indices=FAILED_MAP_INDICES,
        attempt=ATTEMPT,
    )