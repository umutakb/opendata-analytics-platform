from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

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


def _resolve_latest_run_dir(tmp_path: Path) -> Path:
    pointer_path = tmp_path / "artifacts" / "latest_run.txt"
    assert pointer_path.exists()

    pointer_value = pointer_path.read_text(encoding="utf-8").strip()
    run_dir = Path(pointer_value)
    if not run_dir.is_absolute():
        run_dir = (tmp_path / run_dir).resolve()
    return run_dir


def test_pipeline_e2e_run_all_small(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "warehouse.duckdb"
    artifacts_root = tmp_path / "artifacts"

    _run_cli(
        tmp_path,
        [
            "run-all",
            "--db",
            str(db_path),
            "--small",
            "--config",
            str(PROJECT_ROOT / "config.example.yml"),
            "--sql-root",
            str(PROJECT_ROOT / "sql"),
        ],
    )

    _run_cli(
        tmp_path,
        [
            "validate-contract",
            "--db",
            str(db_path),
            "--contract",
            str(PROJECT_ROOT / "contracts" / "schema_v1.yml"),
        ],
    )

    assert db_path.exists()
    assert (tmp_path / "data" / "staging" / "stg_orders.csv").exists()
    assert (tmp_path / "data" / "marts" / "fct_orders.csv").exists()
    assert (artifacts_root / "latest_run.txt").exists()
    assert _find_artifact_file(artifacts_root, "metrics", "m_gmv_daily.csv").exists()
    assert _find_artifact_file(artifacts_root, "metrics", "m_net_gmv_daily.csv").exists()
    assert _find_artifact_file(artifacts_root, "metrics", "m_retention_cohort.json").exists()
    assert _find_artifact_file(artifacts_root, "quality", "report.json").exists()
    assert _find_artifact_file(artifacts_root, "quality", "report.html").exists()

    latest_run_dir = _resolve_latest_run_dir(tmp_path)
    manifest_path = latest_run_dir / "run_manifest.json"
    log_path = latest_run_dir / "logs" / "run.log"
    assert manifest_path.exists()
    assert log_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["run_id"]
    assert manifest["started_at"]
    assert manifest["finished_at"]
    assert "build_warehouse" in manifest["durations_sec"]
    assert "transform" in manifest["durations_sec"]
    assert "metrics" in manifest["durations_sec"]
    assert "quality" in manifest["durations_sec"]
    assert isinstance(manifest.get("metrics_run", []), list)
    assert "m_gmv_daily" in manifest.get("metrics_run", [])
    assert "quality_summary" in manifest
    assert set(manifest["quality_summary"]).issuperset({"pass", "warn", "fail"})

    with duckdb.connect(str(db_path)) as conn:
        relations = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }

    required = {"customers", "orders", "fct_orders", "fct_order_items", "dim_customers", "dim_products"}
    assert required.issubset(relations)
