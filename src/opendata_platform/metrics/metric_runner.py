from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


def _normalize_metric_name(value: str) -> str:
    metric_name = value.strip()
    if metric_name.endswith(".sql"):
        metric_name = metric_name[: -len(".sql")]
    return metric_name


def discover_metric_sql_files(sql_dir: Path) -> dict[str, Path]:
    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No metric SQL files found in {sql_dir}")
    return {sql_file.stem: sql_file for sql_file in files}


def _resolve_metric_sql_files(
    sql_dir: Path,
    enabled_metrics: list[str] | None,
    disabled_metrics: list[str] | None,
) -> list[Path]:
    discovered = discover_metric_sql_files(sql_dir)
    disabled = {_normalize_metric_name(item) for item in (disabled_metrics or []) if str(item).strip()}

    selected_names: list[str] = []
    if enabled_metrics:
        missing: list[str] = []
        seen: set[str] = set()
        for raw_name in enabled_metrics:
            name = _normalize_metric_name(str(raw_name))
            if not name or name in seen:
                continue
            seen.add(name)
            if name not in discovered:
                missing.append(f"{name}.sql")
                continue
            if name in disabled:
                continue
            selected_names.append(name)
        if missing:
            raise FileNotFoundError(
                f"Enabled metric SQL files not found in {sql_dir}: {', '.join(sorted(missing))}"
            )
    else:
        selected_names = [name for name in sorted(discovered) if name not in disabled]

    if not selected_names:
        raise FileNotFoundError("No metric SQL files selected after enabled/disabled filtering.")

    return [discovered[name] for name in selected_names]


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
    disabled_metrics: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Execute metric SQL files and write CSV+JSON outputs."""
    sql_path = Path(sql_dir)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    manifest: list[dict[str, Any]] = []

    with duckdb.connect(str(db_path)) as conn:
        for sql_file in _resolve_metric_sql_files(sql_path, enabled_metrics, disabled_metrics):
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
