from __future__ import annotations

import argparse
import json
import logging
import shlex
import subprocess
import time
from datetime import datetime
from pathlib import Path
import shutil

from opendata_platform.config import get_config_value, load_config
from opendata_platform.ingest.generate_data import generate_synthetic_data
from opendata_platform.ingest.ingest import ingest_data
from opendata_platform.metrics.metric_runner import run_metrics
from opendata_platform.quality.dq_checks import run_quality_checks
from opendata_platform.quality.report import write_quality_report
from opendata_platform.transform.run_sql import run_transforms
from opendata_platform.warehouse.build_db import build_warehouse


def _parse_enabled_metrics(config: dict | None) -> list[str] | None:
    if not config:
        return None
    enabled = get_config_value(config, ["metrics", "enabled"], None)
    if not isinstance(enabled, list):
        return None
    values = [str(item).strip() for item in enabled if str(item).strip()]
    return values or None


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


def _build_run_logger(logs_dir: Path, run_id: str) -> logging.Logger:
    logger = logging.getLogger(f"opdata.run_all.{run_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    log_file = logs_dir / "run.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(file_handler)
    return logger


def _close_logger(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        handler.flush()
        handler.close()
        logger.removeHandler(handler)


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


def cmd_ingest(args: argparse.Namespace) -> int:
    result = ingest_data(
        source=args.source,
        out_dir=args.out,
        seed=args.seed,
        days=args.days,
        n_orders=args.orders,
        n_customers=args.customers,
        n_products=args.products,
        end_date=args.end_date,
        dataset=args.dataset,
    )
    if "fallback_message" in result:
        print(f"[ingest] {result['fallback_message']}")
    print(
        "Ingest complete: "
        f"requested={result['source_requested']} used={result['source_used']} out={args.out} stats={result['stats']}"
    )
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
    config: dict = {}
    if args.config:
        config = load_config(args.config)
        eval_days = int(get_config_value(config, ["metrics", "eval_days"], eval_days))
    enabled_metrics = _parse_enabled_metrics(config)

    output_dir, run_root = _resolve_artifact_output(args.out, "metrics")
    manifest = run_metrics(
        db_path=args.db,
        sql_dir=Path(args.sql_root) / "metrics",
        out_dir=output_dir,
        eval_days=eval_days,
        enabled_metrics=enabled_metrics,
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


def cmd_run_all(args: argparse.Namespace) -> int:
    config = load_config(args.config) if args.config else {}

    run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_root = Path("artifacts") / "runs" / run_id
    metrics_out = run_root / "metrics"
    quality_out = run_root / "quality"
    logs_out = run_root / "logs"
    logs_out.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now()
    step_durations: dict[str, float] = {}
    metrics_manifest: list[dict] = []
    quality_report: dict = {}
    ingest_result: dict[str, object] = {}
    quality_json = quality_out / "report.json"
    quality_html = quality_out / "report.html"
    logger = _build_run_logger(logs_out, run_id)
    logger.info("run-all started run_id=%s db_path=%s config_path=%s", run_id, args.db, args.config)

    try:
        days = args.days if args.days is not None else (60 if args.small else 365)
        orders = args.orders if args.orders is not None else (3000 if args.small else None)
        customers = args.customers if args.customers is not None else (1200 if args.small else None)
        products = args.products if args.products is not None else (250 if args.small else 1200)
        source = args.source or "synthetic"

        start = time.perf_counter()
        ingest_result = ingest_data(
            source=source,
            out_dir=args.raw,
            seed=args.seed,
            days=days,
            n_orders=orders,
            n_customers=customers,
            n_products=products,
            end_date=args.end_date,
            dataset=args.dataset,
        )
        step_durations["ingest"] = round(time.perf_counter() - start, 3)
        logger.info("step=ingest duration_sec=%.3f details=%s", step_durations["ingest"], ingest_result)
        if "fallback_message" in ingest_result:
            print(f"[run-all] {ingest_result['fallback_message']}")
        print(
            "[run-all] ingest: "
            f"requested={ingest_result['source_requested']} used={ingest_result['source_used']} stats={ingest_result['stats']}"
        )

        start = time.perf_counter()
        warehouse_stats = build_warehouse(args.raw, args.db)
        step_durations["build_warehouse"] = round(time.perf_counter() - start, 3)
        logger.info(
            "step=build_warehouse duration_sec=%.3f details=%s",
            step_durations["build_warehouse"],
            warehouse_stats,
        )
        print(f"[run-all] build-warehouse: {warehouse_stats}")

        start = time.perf_counter()
        transform_stats = run_transforms(
            db_path=args.db,
            sql_root=args.sql_root,
            staging_out=args.staging_out,
            marts_out=args.marts_out,
        )
        step_durations["transform"] = round(time.perf_counter() - start, 3)
        logger.info("step=transform duration_sec=%.3f details=%s", step_durations["transform"], transform_stats)
        print(f"[run-all] transform: {transform_stats}")

        eval_days = get_config_value(config, ["metrics", "eval_days"], None)
        enabled_metrics = _parse_enabled_metrics(config)
        start = time.perf_counter()
        metrics_manifest = run_metrics(
            db_path=args.db,
            sql_dir=Path(args.sql_root) / "metrics",
            out_dir=metrics_out,
            eval_days=int(eval_days) if eval_days is not None else None,
            enabled_metrics=enabled_metrics,
        )
        step_durations["metrics"] = round(time.perf_counter() - start, 3)
        logger.info(
            "step=metrics duration_sec=%.3f metrics=%s",
            step_durations["metrics"],
            [item["metric"] for item in metrics_manifest],
        )
        print(f"[run-all] metrics: {len(metrics_manifest)} files")

        start = time.perf_counter()
        quality_report = run_quality_checks(args.db, config)
        quality_json, quality_html = write_quality_report(quality_report, quality_out)
        step_durations["quality"] = round(time.perf_counter() - start, 3)
        logger.info("step=quality duration_sec=%.3f summary=%s", step_durations["quality"], quality_report["summary"])
        print(f"[run-all] quality: {quality_report['summary']}")

        finished_at = datetime.now()
        run_manifest = {
            "run_id": run_id,
            "started_at": started_at.isoformat(timespec="seconds"),
            "finished_at": finished_at.isoformat(timespec="seconds"),
            "durations_sec": step_durations,
            "db_path": str(args.db),
            "config_path": str(args.config) if args.config else None,
            "ingest_source_requested": ingest_result.get("source_requested"),
            "ingest_source_used": ingest_result.get("source_used"),
            "metrics_run": [item["metric"] for item in metrics_manifest],
            "quality_summary": quality_report.get("summary", {}),
        }
        manifest_path = run_root / "run_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")
        logger.info("run_manifest=%s", manifest_path)
        logger.info("run-all finished")
    finally:
        _close_logger(logger)

    _sync_latest_artifacts(metrics_out, "metrics", run_root)
    _sync_latest_artifacts(quality_out, "quality", run_root)
    _sync_latest_artifacts(logs_out, "logs", run_root)

    print("")
    print("Run summary")
    print(f"- run_id: {run_id}")
    print(f"- db_path: {args.db}")
    print(f"- metrics_path: {metrics_out}")
    print(f"- quality_json: {quality_json}")
    print(f"- quality_html: {quality_html}")
    print(f"- logs_path: {logs_out}")
    print(f"- manifest_path: {run_root / 'run_manifest.json'}")
    print(f"- latest_pointer: artifacts/latest_run.txt")

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

    ingest = subparsers.add_parser("ingest", help="Ingest data from synthetic or open source")
    ingest.add_argument("--source", choices=["synthetic", "open"], default="synthetic")
    ingest.add_argument("--out", required=True, help="Output raw data folder")
    ingest.add_argument("--dataset", default="uci_online_retail", help="Open dataset name")
    ingest.add_argument("--seed", type=int, default=42)
    ingest.add_argument("--days", type=int, default=365)
    ingest.add_argument("--orders", type=int, default=None, help="Optional override for order count")
    ingest.add_argument("--customers", type=int, default=None, help="Optional override for customer count")
    ingest.add_argument("--products", type=int, default=1200, help="Product count")
    ingest.add_argument("--end-date", type=str, default=None, help="Optional YYYY-MM-DD")
    ingest.set_defaults(func=cmd_ingest)

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
    metrics.add_argument("--config", default="config.example.yml", help="Config for eval_days and enabled metrics")
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

    run_all = subparsers.add_parser("run-all", help="Run end-to-end pipeline in one command")
    run_all.add_argument("--db", default="data/warehouse.duckdb", help="DuckDB file path")
    run_all.add_argument("--config", default="config.example.yml", help="Pipeline config file")
    run_all.add_argument("--raw", default="data/raw", help="Raw data folder")
    run_all.add_argument("--sql-root", default="sql", help="Root folder for SQL models")
    run_all.add_argument("--staging-out", default="data/staging", help="Staging CSV export folder")
    run_all.add_argument("--marts-out", default="data/marts", help="Marts CSV export folder")
    run_all.add_argument("--source", choices=["synthetic", "open"], default="synthetic", help="Ingestion source")
    run_all.add_argument("--dataset", default="uci_online_retail", help="Open dataset name")
    run_all.add_argument("--generate-data", action="store_true", help="Deprecated alias; ingestion always runs")
    run_all.add_argument("--small", action="store_true", help="Use smaller dataset defaults for faster runs")
    run_all.add_argument("--seed", type=int, default=42, help="Seed used when --generate-data is enabled")
    run_all.add_argument("--days", type=int, default=None, help="Days used when --generate-data is enabled")
    run_all.add_argument("--orders", type=int, default=None, help="Order count used when --generate-data is enabled")
    run_all.add_argument(
        "--customers",
        type=int,
        default=None,
        help="Customer count used when --generate-data is enabled",
    )
    run_all.add_argument(
        "--products",
        type=int,
        default=None,
        help="Product count used when --generate-data is enabled",
    )
    run_all.add_argument("--end-date", type=str, default=None, help="Optional YYYY-MM-DD for generated data")
    run_all.set_defaults(func=cmd_run_all)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
