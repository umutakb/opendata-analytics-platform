from __future__ import annotations

from pathlib import Path

import pytest

from opendata_platform.metrics.metric_runner import (
    _resolve_metric_sql_files,
    discover_metric_sql_files,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_discover_metric_sql_files_from_repo() -> None:
    discovered = discover_metric_sql_files(PROJECT_ROOT / "sql" / "metrics")

    assert "m_gmv_daily" in discovered
    assert "m_net_gmv_daily" in discovered
    assert all(path.exists() for path in discovered.values())


def test_resolve_metric_sql_files_enabled_and_disabled(tmp_path: Path) -> None:
    for metric_name in ["m_alpha", "m_beta", "m_gamma"]:
        (tmp_path / f"{metric_name}.sql").write_text("SELECT 1 AS value", encoding="utf-8")

    selected = _resolve_metric_sql_files(
        sql_dir=tmp_path,
        enabled_metrics=["m_alpha", "m_gamma.sql"],
        disabled_metrics=["m_gamma"],
    )

    assert [path.stem for path in selected] == ["m_alpha"]


def test_resolve_metric_sql_files_missing_enabled_raises(tmp_path: Path) -> None:
    (tmp_path / "m_only.sql").write_text("SELECT 1", encoding="utf-8")

    with pytest.raises(FileNotFoundError):
        _resolve_metric_sql_files(
            sql_dir=tmp_path,
            enabled_metrics=["m_only", "m_missing"],
            disabled_metrics=None,
        )
