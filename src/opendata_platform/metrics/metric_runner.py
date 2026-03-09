from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


def _read_metric_sql_files(sql_dir: Path) -> list[Path]:
    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No metric SQL files found in {sql_dir}")
    return files


def _resolve_metric_sql_files(sql_dir: Path, enabled_metrics: list[str] | None) -> list[Path]:
    if not enabled_metrics:
        return _read_metric_sql_files(sql_dir)

    selected_files: list[Path] = []
    seen: set[str] = set()
    missing: list[str] = []

    for metric_name in enabled_metrics:
        normalized = metric_name.strip()
        if not normalized:
            continue
        if normalized.endswith(".sql"):
            normalized = normalized[: -len(".sql")]
        if normalized in seen:
            continue
        seen.add(normalized)

        sql_file = sql_dir / f"{normalized}.sql"
        if not sql_file.exists():
            missing.append(f"{normalized}.sql")
            continue
        selected_files.append(sql_file)

    if missing:
        raise FileNotFoundError(
            f"Enabled metric SQL files not found in {sql_dir}: {', '.join(sorted(missing))}"
        )
    if not selected_files:
        raise FileNotFoundError("No enabled metric SQL files selected.")

    return selected_files


def _apply_eval_days(df: pd.DataFrame, eval_days: int | None) -> pd.DataFrame:
    if not eval_days:
        return df

    date_columns = [col for col in ("metric_date", "order_date", "cohort_month") if col in df.columns]
    if not date_columns:
        return df

    cutoff = pd.Timestamp(date.today() - timedelta(days=eval_days))
    date_col = date_columns[0]
    converted = pd.to_datetime(df[date_col], errors="coerce")
    return df.loc[converted >= cutoff].reset_index(drop=True)


def run_metrics(
    db_path: str | Path,
    sql_dir: str | Path,
    out_dir: str | Path,
    eval_days: int | None = None,
    enabled_metrics: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Execute metric SQL files and write CSV+JSON outputs."""
    sql_path = Path(sql_dir)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    manifest: list[dict[str, Any]] = []

    with duckdb.connect(str(db_path)) as conn:
        for sql_file in _resolve_metric_sql_files(sql_path, enabled_metrics):
            metric_name = sql_file.stem
            query = sql_file.read_text(encoding="utf-8")
            df = conn.execute(query).df()
            df = _apply_eval_days(df, eval_days)

            csv_path = out_path / f"{metric_name}.csv"
            json_path = out_path / f"{metric_name}.json"

            df.to_csv(csv_path, index=False)
            json_path.write_text(df.to_json(orient="records", date_format="iso"), encoding="utf-8")

            manifest.append(
                {
                    "metric": metric_name,
                    "rows": int(df.shape[0]),
                    "csv": str(csv_path),
                    "json": str(json_path),
                }
            )

    (out_path / "metrics_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    return manifest
