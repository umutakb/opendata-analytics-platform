from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb

from opendata_platform.ingest.generate_data import generate_synthetic_data
from opendata_platform.transform.run_sql import run_transforms
from opendata_platform.warehouse.build_db import build_warehouse


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_cli(tmp_path: Path, cli_args: list[str]) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    subprocess.run(
        [sys.executable, "-m", "opendata_platform.cli", *cli_args],
        check=True,
        cwd=tmp_path,
        env=env,
    )


def _find_artifact_file(artifacts_root: Path, artifact_type: str, file_name: str) -> Path:
    latest_path = artifacts_root / "latest" / artifact_type / file_name
    if latest_path.exists():
        return latest_path

    run_candidates = sorted((artifacts_root / "runs").glob(f"*/{artifact_type}/{file_name}"))
    if run_candidates:
        return run_candidates[-1]

    direct_path = artifacts_root / artifact_type / file_name
    if direct_path.exists():
        return direct_path

    raise AssertionError(f"Artifact file not found: {artifact_type}/{file_name}")


def test_pipeline_e2e(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw"
    staging_dir = tmp_path / "data" / "staging"
    marts_dir = tmp_path / "data" / "marts"
    db_path = tmp_path / "data" / "warehouse.duckdb"
    artifacts_root = tmp_path / "artifacts"

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

    _run_cli(
        tmp_path,
        [
            "metrics",
            "--db",
            str(db_path),
            "--sql-root",
            str(PROJECT_ROOT / "sql"),
            "--config",
            str(PROJECT_ROOT / "config.example.yml"),
        ],
    )
    _run_cli(
        tmp_path,
        [
            "quality",
            "--db",
            str(db_path),
            "--config",
            str(PROJECT_ROOT / "config.example.yml"),
        ],
    )

    assert (raw_dir / "orders.csv").exists()
    assert (staging_dir / "stg_orders.csv").exists()
    assert (marts_dir / "fct_orders.csv").exists()
    assert _find_artifact_file(artifacts_root, "metrics", "m_gmv_daily.csv").exists()
    assert _find_artifact_file(artifacts_root, "metrics", "m_net_gmv_daily.csv").exists()
    assert _find_artifact_file(artifacts_root, "metrics", "m_retention_cohort.json").exists()
    assert _find_artifact_file(artifacts_root, "quality", "report.json").exists()
    assert _find_artifact_file(artifacts_root, "quality", "report.html").exists()
    assert (artifacts_root / "latest_run.txt").exists()

    with duckdb.connect(str(db_path)) as conn:
        relations = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }

    required = {"customers", "orders", "fct_orders", "fct_order_items", "dim_customers", "dim_products"}
    assert required.issubset(relations)
