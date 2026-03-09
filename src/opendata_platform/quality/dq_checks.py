from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb


REQUIRED_RELATIONS = [
    "customers",
    "products",
    "orders",
    "order_items",
    "stg_customers",
    "stg_products",
    "stg_orders",
    "stg_order_items",
    "dim_customers",
    "dim_products",
    "fct_orders",
    "fct_order_items",
]


def _parse_target(target: str) -> tuple[str, str]:
    parts = target.split(".")
    if len(parts) != 2:
        raise ValueError(f"Invalid target format: {target}")
    return parts[0], parts[1]


def _relation_exists(conn: duckdb.DuckDBPyConnection, relation: str) -> bool:
    count = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
          SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'
          UNION ALL
          SELECT table_name FROM information_schema.views WHERE table_schema = 'main'
        ) t
        WHERE table_name = ?
        """,
        [relation],
    ).fetchone()[0]
    return bool(count)


def _status_from_ratio(value: float, warn: float, fail: float) -> str:
    if value > fail:
        return "fail"
    if value > warn:
        return "warn"
    return "pass"


def check_schema_existence(
    conn: duckdb.DuckDBPyConnection,
    required_relations: list[str] | None = None,
) -> list[dict[str, Any]]:
    required = required_relations or REQUIRED_RELATIONS
    rows: list[dict[str, Any]] = []

    for relation in required:
        exists = _relation_exists(conn, relation)
        rows.append(
            {
                "check_type": "schema_existence",
                "target": relation,
                "status": "pass" if exists else "fail",
                "observed": int(exists),
                "warn_threshold": None,
                "fail_threshold": 1,
                "message": "Relation exists" if exists else "Missing relation",
            }
        )

    return rows


def check_null_rate_thresholds(
    conn: duckdb.DuckDBPyConnection,
    thresholds: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for target, limits in thresholds.items():
        table, column = _parse_target(target)
        total, null_count = conn.execute(
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE {column} IS NULL) FROM {table}"
        ).fetchone()

        rate = (null_count / total) if total else 0.0
        warn_threshold = float(limits.get("warn", 0.0))
        fail_threshold = float(limits.get("fail", 1.0))
        status = _status_from_ratio(rate, warn_threshold, fail_threshold)

        rows.append(
            {
                "check_type": "null_rate",
                "target": target,
                "status": status,
                "observed": round(rate, 6),
                "warn_threshold": warn_threshold,
                "fail_threshold": fail_threshold,
                "message": f"nulls={null_count}, total={total}",
            }
        )

    return rows


def check_pk_uniqueness(
    conn: duckdb.DuckDBPyConnection,
    pk_targets: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for target in pk_targets:
        table, column = _parse_target(target)
        total, distinct_count = conn.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT {column}) FROM {table}"
        ).fetchone()
        duplicate_count = int(total - distinct_count)

        rows.append(
            {
                "check_type": "pk_uniqueness",
                "target": target,
                "status": "fail" if duplicate_count > 0 else "pass",
                "observed": duplicate_count,
                "warn_threshold": 0,
                "fail_threshold": 0,
                "message": f"duplicates={duplicate_count}",
            }
        )

    return rows


def check_numeric_ranges(
    conn: duckdb.DuckDBPyConnection,
    ranges: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for target, rule in ranges.items():
        table, column = _parse_target(target)
        min_value = float(rule["min"])
        max_value = float(rule["max"])
        severity = str(rule.get("severity", "fail"))

        violation_count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE {column} IS NULL OR {column} < ? OR {column} > ?
            """,
            [min_value, max_value],
        ).fetchone()[0]

        status = severity if violation_count > 0 else "pass"
        rows.append(
            {
                "check_type": "numeric_range",
                "target": target,
                "status": status,
                "observed": int(violation_count),
                "warn_threshold": min_value,
                "fail_threshold": max_value,
                "message": f"violations={violation_count}",
            }
        )

    return rows


def check_freshness(
    conn: duckdb.DuckDBPyConnection,
    freshness_rules: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for target, rule in freshness_rules.items():
        table, rhs = _parse_target(target)
        suffix = "_max_age_days"
        if not rhs.endswith(suffix):
            raise ValueError(f"Invalid freshness key: {target}")
        column = rhs[: -len(suffix)]

        max_date_value = conn.execute(f"SELECT MAX(CAST({column} AS DATE)) FROM {table}").fetchone()[0]
        warn_days = int(rule.get("warn", 7))
        fail_days = int(rule.get("fail", 30))

        if max_date_value is None:
            rows.append(
                {
                    "check_type": "freshness",
                    "target": target,
                    "status": "fail",
                    "observed": None,
                    "warn_threshold": warn_days,
                    "fail_threshold": fail_days,
                    "message": "No data found for freshness",
                }
            )
            continue

        age_days = (date.today() - max_date_value).days
        if age_days > fail_days:
            status = "fail"
        elif age_days > warn_days:
            status = "warn"
        else:
            status = "pass"

        rows.append(
            {
                "check_type": "freshness",
                "target": target,
                "status": status,
                "observed": int(age_days),
                "warn_threshold": warn_days,
                "fail_threshold": fail_days,
                "message": f"max_date={max_date_value}",
            }
        )

    return rows


def summarize_checks(check_rows: list[dict[str, Any]]) -> dict[str, int]:
    summary = {"pass": 0, "warn": 0, "fail": 0}
    for row in check_rows:
        status = row.get("status", "fail")
        summary[status] = summary.get(status, 0) + 1
    return summary


def run_quality_checks(db_path: str | Path, config: dict[str, Any]) -> dict[str, Any]:
    """Run all quality checks from config on DuckDB."""
    quality_cfg = config.get("quality", {})

    with duckdb.connect(str(db_path)) as conn:
        rows: list[dict[str, Any]] = []
        rows.extend(check_schema_existence(conn, REQUIRED_RELATIONS))
        rows.extend(check_null_rate_thresholds(conn, quality_cfg.get("null_rate_thresholds", {})))
        rows.extend(check_pk_uniqueness(conn, quality_cfg.get("pk_uniqueness", [])))
        rows.extend(check_numeric_ranges(conn, quality_cfg.get("ranges", {})))
        rows.extend(check_freshness(conn, quality_cfg.get("freshness", {})))

    return {
        "generated_at": date.today().isoformat(),
        "summary": summarize_checks(rows),
        "checks": rows,
    }
