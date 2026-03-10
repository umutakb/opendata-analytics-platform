from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import yaml


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


def _column_type(conn: duckdb.DuckDBPyConnection, relation: str, column: str) -> str | None:
    row = conn.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
          AND column_name = ?
        LIMIT 1
        """,
        [relation, column],
    ).fetchone()
    return str(row[0]) if row else None


def _normalize_column_spec(spec: Any) -> tuple[str, str | None]:
    if isinstance(spec, str):
        return spec, None
    if isinstance(spec, dict):
        name = str(spec.get("name", "")).strip()
        expected_type = spec.get("type")
        return name, str(expected_type).strip() if expected_type else None
    raise ValueError(f"Unsupported column spec: {spec}")


def validate_contract(db_path: str | Path, contract_path: str | Path) -> dict[str, Any]:
    contract_file = Path(contract_path)
    contract = yaml.safe_load(contract_file.read_text(encoding="utf-8")) or {}
    relations = contract.get("relations", {})
    if not isinstance(relations, dict):
        raise ValueError("Contract must contain a 'relations' mapping.")

    checks: list[dict[str, Any]] = []

    with duckdb.connect(str(db_path)) as conn:
        for relation_name, relation_spec in relations.items():
            if not isinstance(relation_spec, dict):
                raise ValueError(f"Relation spec must be dict: {relation_name}")

            exists = _relation_exists(conn, relation_name)
            checks.append(
                {
                    "check_type": "contract_relation_exists",
                    "target": relation_name,
                    "status": "pass" if exists else "fail",
                    "message": "Relation exists" if exists else "Missing relation",
                }
            )
            if not exists:
                continue

            column_specs = relation_spec.get("columns", [])
            if not isinstance(column_specs, list):
                raise ValueError(f"columns must be list for relation: {relation_name}")

            for raw_spec in column_specs:
                column_name, expected_type = _normalize_column_spec(raw_spec)
                if not column_name:
                    raise ValueError(f"Invalid column name in relation {relation_name}: {raw_spec}")

                actual_type = _column_type(conn, relation_name, column_name)
                if actual_type is None:
                    checks.append(
                        {
                            "check_type": "contract_column_exists",
                            "target": f"{relation_name}.{column_name}",
                            "status": "fail",
                            "message": "Missing column",
                        }
                    )
                    continue

                checks.append(
                    {
                        "check_type": "contract_column_exists",
                        "target": f"{relation_name}.{column_name}",
                        "status": "pass",
                        "message": "Column exists",
                    }
                )

                if expected_type:
                    status = "pass" if expected_type.lower() in actual_type.lower() else "fail"
                    checks.append(
                        {
                            "check_type": "contract_type_check",
                            "target": f"{relation_name}.{column_name}",
                            "status": status,
                            "message": f"expected={expected_type}, actual={actual_type}",
                        }
                    )

    summary = {"pass": 0, "warn": 0, "fail": 0}
    for row in checks:
        status = row.get("status", "fail")
        if status not in summary:
            summary[status] = 0
        summary[status] += 1

    return {
        "schema_version": contract.get("schema_version"),
        "db_path": str(db_path),
        "contract_path": str(contract_file),
        "summary": summary,
        "checks": checks,
    }
