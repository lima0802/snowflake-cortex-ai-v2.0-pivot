"""
DIA v2 — Upload semantic files to Snowflake stage
===================================================
Uploads all files from data/semantic_views/ and data/prompts/
to the SEMANTIC_MODELS stage in Snowflake.

Usage:
    python deploy/upload_to_stage.py               # upload all
    python deploy/upload_to_stage.py --dry-run     # preview only
    python deploy/upload_to_stage.py --overwrite   # force re-upload
"""

import os
import sys
import glob
import argparse
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

STAGE = os.getenv("SNOWFLAKE_SEMANTIC_STAGE", "semantic_models")

# Map local folder -> stage subfolder
UPLOAD_MAP = {
    "data/semantic_views": "{stage}/semantic_views",
    "data/prompts":        "{stage}/prompts",
}

FILE_PATTERNS = ["**/*.yaml", "**/*.txt", "**/*.json"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def collect_files(local_dir: str) -> list[str]:
    files = []
    for pattern in FILE_PATTERNS:
        files.extend(glob.glob(f"{local_dir}/{pattern}", recursive=True))
    return sorted(set(files))


def upload_file(cursor, local_path: str, stage_path: str, overwrite: bool, dry_run: bool):
    # Normalize path for PUT command (forward slashes, file:// prefix on Windows)
    abs_path = os.path.abspath(local_path).replace("\\", "/")
    put_cmd = f"PUT 'file://{abs_path}' '{stage_path}' AUTO_COMPRESS=FALSE"
    if overwrite:
        put_cmd += " OVERWRITE=TRUE"

    if dry_run:
        print(f"  [DRY RUN] {put_cmd}")
        return True

    try:
        cursor.execute(put_cmd)
        result = cursor.fetchone()
        status = result[6] if result else "unknown"
        print(f"  [{'OK' if 'UPLOADED' in str(status).upper() or 'SKIPPED' in str(status).upper() else status}] {local_path}")
        return True
    except Exception as e:
        print(f"  [FAIL] {local_path}: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Upload DIA v2 semantic files to Snowflake stage")
    parser.add_argument("--dry-run",   action="store_true", help="Preview without uploading")
    parser.add_argument("--overwrite", action="store_true", help="Re-upload existing files")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  DIA v2 - Snowflake Stage Upload")
    print(f"  Stage: {STAGE}")
    print(f"  Mode:  {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)

    if not args.dry_run:
        try:
            conn = get_connection()
            cursor = conn.cursor()
            print(f"\n  Connected to Snowflake\n")
        except Exception as e:
            print(f"\n  [ERROR] Could not connect to Snowflake: {e}")
            sys.exit(1)
    else:
        cursor = None

    total_ok = total_fail = 0

    for local_dir, stage_template in UPLOAD_MAP.items():
        stage_path = stage_template.format(stage=f"@{STAGE}")
        files = collect_files(local_dir)

        if not files:
            print(f"\n  No files found in {local_dir}/ - skipping")
            continue

        print(f"\n  {local_dir}/ -> {stage_path}  ({len(files)} files)")

        for local_path in files:
            ok = upload_file(cursor, local_path, stage_path, args.overwrite, args.dry_run)
            if ok:
                total_ok += 1
            else:
                total_fail += 1

    print(f"\n{'=' * 60}")
    print(f"  Result: {total_ok} uploaded, {total_fail} failed")

    if not args.dry_run and cursor:
        # List stage contents after upload
        print(f"\n  Stage contents ({STAGE}):")
        try:
            cursor.execute(f"LIST @{STAGE}")
            rows = cursor.fetchall()
            for row in rows:
                print(f"    {row[0]}  ({row[1]} bytes)")
        except Exception as e:
            print(f"  Could not list stage: {e}")
        conn.close()

    print()
    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()
