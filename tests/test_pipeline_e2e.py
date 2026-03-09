from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from opendata_platform.config import load_config
from opendata_platform.ingest.generate_data import generate_synthetic_data
from opendata_platform.metrics.metric_runner import run_metrics
from opendata_platform.quality.dq_checks import run_quality_checks
from opendata_platform.quality.report import write_quality_report
from opendata_platform.transform.run_sql import run_transforms
from opendata_platform.warehouse.build_db import build_warehouse


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_pipeline_e2e(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw"
    staging_dir = tmp_path / "data" / "staging"
    marts_dir = tmp_path / "data" / "marts"
    metrics_dir = tmp_path / "artifacts" / "metrics"
    quality_dir = tmp_path / "artifacts" / "quality"
    db_path = tmp_path / "data" / "warehouse.duckdb"

    stats = generate_synthetic_data(
        out_dir=raw_dir,
        seed=42,
        days=60,
        n_orders=3000,
        n_customers=1200,
        n_products=250,
        end_date=date.today().isoformat(),
    )
    assert stats["orders"] == 3000

    warehouse_stats = build_warehouse(raw_dir, db_path)
    assert warehouse_stats["orders"] == 3000

    transform_stats = run_transforms(
        db_path=db_path,
        sql_root=PROJECT_ROOT / "sql",
        staging_out=staging_dir,
        marts_out=marts_dir,
    )
    assert transform_stats["staging_exports"] >= 4
    assert transform_stats["marts_exports"] >= 4

    manifest = run_metrics(
        db_path=db_path,
        sql_dir=PROJECT_ROOT / "sql" / "metrics",
        out_dir=metrics_dir,
        eval_days=90,
    )
    assert len(manifest) >= 5

    config = load_config(PROJECT_ROOT / "config.example.yml")
    quality_report = run_quality_checks(db_path, config)
    json_path, html_path = write_quality_report(quality_report, quality_dir)

    assert (raw_dir / "orders.csv").exists()
    assert (staging_dir / "stg_orders.csv").exists()
    assert (marts_dir / "fct_orders.csv").exists()
    assert (metrics_dir / "m_gmv_daily.csv").exists()
    assert (metrics_dir / "m_retention_cohort.json").exists()
    assert json_path.exists()
    assert html_path.exists()

    with duckdb.connect(str(db_path)) as conn:
        relations = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }

    required = {"customers", "orders", "fct_orders", "fct_order_items", "dim_customers", "dim_products"}
    assert required.issubset(relations)
