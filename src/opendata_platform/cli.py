from __future__ import annotations

import argparse
import shlex
import subprocess
from datetime import datetime
from pathlib import Path
import shutil

from opendata_platform.config import get_config_value, load_config
from opendata_platform.ingest.generate_data import generate_synthetic_data
from opendata_platform.metrics.metric_runner import run_metrics
from opendata_platform.quality.dq_checks import run_quality_checks
from opendata_platform.quality.report import write_quality_report
from opendata_platform.transform.run_sql import run_transforms
from opendata_platform.warehouse.build_db import build_warehouse


def _resolve_artifact_output(out_arg: str | None, artifact_type: str) -> tuple[Path, Path | None]:
    if out_arg:
        return Path(out_arg), None

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_root = Path("artifacts") / "runs" / timestamp
    return run_root / artifact_type, run_root


def _sync_latest_artifacts(source_dir: Path, artifact_type: str, run_root: Path) -> None:
    artifacts_root = Path("artifacts")
    artifacts_root.mkdir(parents=True, exist_ok=True)

    latest_root = artifacts_root / "latest"
    latest_root.mkdir(parents=True, exist_ok=True)

    latest_target = latest_root / artifact_type
    if latest_target.exists():
        shutil.rmtree(latest_target)
    shutil.copytree(source_dir, latest_target)

    (artifacts_root / "latest_run.txt").write_text(str(run_root), encoding="utf-8")


def cmd_demo_data(args: argparse.Namespace) -> int:
    stats = generate_synthetic_data(
        out_dir=args.out,
        seed=args.seed,
        days=args.days,
        n_orders=args.orders,
        n_customers=args.customers,
        n_products=args.products,
        end_date=args.end_date,
    )
    print(f"Generated synthetic data in {args.out}: {stats}")
    return 0


def cmd_build_warehouse(args: argparse.Namespace) -> int:
    stats = build_warehouse(args.raw, args.db)
    print(f"Built warehouse at {args.db}: {stats}")
    return 0


def cmd_transform(args: argparse.Namespace) -> int:
    stats = run_transforms(
        db_path=args.db,
        sql_root=args.sql_root,
        staging_out=args.staging_out,
        marts_out=args.marts_out,
    )
    print(f"Transform complete: {stats}")
    return 0


def cmd_metrics(args: argparse.Namespace) -> int:
    eval_days: int | None = args.eval_days
    if args.config:
        config = load_config(args.config)
        eval_days = int(get_config_value(config, ["metrics", "eval_days"], eval_days))

    output_dir, run_root = _resolve_artifact_output(args.out, "metrics")
    manifest = run_metrics(
        db_path=args.db,
        sql_dir=Path(args.sql_root) / "metrics",
        out_dir=output_dir,
        eval_days=eval_days,
    )
    if run_root is not None:
        _sync_latest_artifacts(output_dir, "metrics", run_root)

    print(f"Metrics complete. Files written: {len(manifest)}")
    print(f"Metrics output: {output_dir}")
    return 0


def cmd_quality(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    output_dir, run_root = _resolve_artifact_output(args.out, "quality")
    report = run_quality_checks(args.db, config)
    json_path, html_path = write_quality_report(report, output_dir)
    if run_root is not None:
        _sync_latest_artifacts(output_dir, "quality", run_root)

    print(f"Quality report written: {json_path} and {html_path}")
    print(f"Summary: {report['summary']}")
    return 0


def cmd_dashboard(args: argparse.Namespace) -> int:
    app_path = Path(__file__).resolve().parent / "dashboard" / "app.py"
    command = ["streamlit", "run", str(app_path), "--", "--db", str(args.db)]

    if args.run:
        subprocess.run(command, check=True)
    else:
        print("Run the dashboard with:")
        print(" ".join(shlex.quote(part) for part in command))

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="opdata", description="OpenData Analytics Platform CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo = subparsers.add_parser("demo-data", help="Generate deterministic synthetic e-commerce data")
    demo.add_argument("--out", required=True, help="Output raw data folder")
    demo.add_argument("--seed", type=int, default=42)
    demo.add_argument("--days", type=int, default=365)
    demo.add_argument("--orders", type=int, default=None, help="Optional override for order count")
    demo.add_argument("--customers", type=int, default=None, help="Optional override for customer count")
    demo.add_argument("--products", type=int, default=1200, help="Product count")
    demo.add_argument("--end-date", type=str, default=None, help="Optional YYYY-MM-DD")
    demo.set_defaults(func=cmd_demo_data)

    warehouse = subparsers.add_parser("build-warehouse", help="Load raw CSV into DuckDB")
    warehouse.add_argument("--raw", required=True, help="Raw data folder")
    warehouse.add_argument("--db", required=True, help="DuckDB file path")
    warehouse.set_defaults(func=cmd_build_warehouse)

    transform = subparsers.add_parser("transform", help="Run SQL staging + marts models")
    transform.add_argument("--db", required=True, help="DuckDB file path")
    transform.add_argument("--sql-root", default="sql", help="Root folder for SQL models")
    transform.add_argument("--staging-out", default="data/staging", help="Staging CSV export folder")
    transform.add_argument("--marts-out", default="data/marts", help="Marts CSV export folder")
    transform.set_defaults(func=cmd_transform)

    metrics = subparsers.add_parser("metrics", help="Run SQL metrics and export artifacts")
    metrics.add_argument("--db", required=True, help="DuckDB file path")
    metrics.add_argument("--out", default=None, help="Metrics output folder (optional)")
    metrics.add_argument("--sql-root", default="sql", help="Root folder for SQL models")
    metrics.add_argument("--config", default="config.example.yml", help="Config for eval_days")
    metrics.add_argument("--eval-days", type=int, default=None, help="Optional override for eval window")
    metrics.set_defaults(func=cmd_metrics)

    quality = subparsers.add_parser("quality", help="Run data quality checks")
    quality.add_argument("--db", required=True, help="DuckDB file path")
    quality.add_argument("--out", default=None, help="Quality report output folder (optional)")
    quality.add_argument("--config", required=True, help="Quality config file")
    quality.set_defaults(func=cmd_quality)

    dashboard = subparsers.add_parser("dashboard", help="Run or print Streamlit dashboard command")
    dashboard.add_argument("--db", required=True, help="DuckDB file path")
    dashboard.add_argument("--run", action="store_true", help="Launch streamlit directly")
    dashboard.set_defaults(func=cmd_dashboard)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
