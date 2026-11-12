"""Runs the idempotent MERGEs that turn staging tables into final tables.

    # once: create final tables and views (DDL, writes to BigQuery)
    python -m dataflow.merge_job --project MY_PROJECT --setup

    # every 5 minutes while the pipeline runs (DML, writes to BigQuery)
    python -m dataflow.merge_job --project MY_PROJECT --every-minutes 5

    # read-only checks (sql/checks.sql)
    python -m dataflow.merge_job --project MY_PROJECT --checks

    # cost preview, nothing runs
    python -m dataflow.merge_job --project MY_PROJECT --dry-run

In production you would paste the same SQL into BigQuery scheduled queries.

Why running every 5 min with a 3 h look-back is safe: each run re-reads rows
an earlier run already merged. That is fine ONLY because the MERGEs are
idempotent: an already-merged sale or pane changes nothing.
"""

import argparse
import time
from pathlib import Path
from string import Template

SQL_DIR = Path(__file__).parent / "sql"
SETUP_FILES = ["00_create_final_tables.sql", "50_views.sql"]
MERGE_FILES = [
    "10_merge_silver.sql",
    "20_merge_gold_sales_per_minute.sql",
    "30_merge_gold_region_rolling_10m.sql",
    "40_merge_gold_user_sessions.sql",
]


def render(name: str, project: str, dataset: str, lookback_hours: int = 3) -> str:
    text = (SQL_DIR / name).read_text()
    return Template(text).substitute(PROJECT=project, DATASET=dataset, LOOKBACK_HOURS=lookback_hours)


def split_statements(sql: str):
    """checks.sql only: one SELECT per statement, no ';' inside them."""
    for chunk in sql.split(";"):
        body = "\n".join(line for line in chunk.splitlines() if not line.strip().startswith("--")).strip()
        if body:
            yield chunk.strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", default="insightstream_analytics")
    parser.add_argument("--lookback-hours", type=int, default=3)
    parser.add_argument("--setup", action="store_true", help="create final tables and views")
    parser.add_argument("--checks", action="store_true", help="run read-only checks")
    parser.add_argument("--every-minutes", type=float, default=0, help="loop; 0 = run once")
    parser.add_argument("--dry-run", action="store_true", help="estimate bytes, run nothing")
    args = parser.parse_args()

    from google.cloud import bigquery
    client = bigquery.Client(project=args.project)
    config = bigquery.QueryJobConfig(dry_run=args.dry_run, use_query_cache=False)

    def run(name, sql):
        job = client.query(sql, job_config=config)
        if args.dry_run:
            print(f"[dry-run] {name}: ~{job.total_bytes_processed or 0:,} bytes")
            return None
        result = job.result()
        print(f"[done] {name}: job {job.job_id}")
        return result

    if args.checks:
        sql = render("checks.sql", args.project, args.dataset)
        for i, statement in enumerate(split_statements(sql), 1):
            rows = run(f"check {i}", statement)
            for row in rows or []:
                print("   ", dict(row.items()))
        return

    files = SETUP_FILES if args.setup else MERGE_FILES
    while True:
        for name in files:
            run(name, render(name, args.project, args.dataset, args.lookback_hours))
        if not args.every_minutes or args.setup or args.dry_run:
            return
        time.sleep(args.every_minutes * 60)


if __name__ == "__main__":
    main()
