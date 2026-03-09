from __future__ import annotations

from pathlib import Path

import duckdb


def _read_sql_files(directory: Path) -> list[Path]:
    files = sorted(directory.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No SQL files found in {directory}")
    return files


def _execute_sql_files(conn: duckdb.DuckDBPyConnection, sql_files: list[Path]) -> list[str]:
    executed: list[str] = []
    for sql_file in sql_files:
        conn.execute(sql_file.read_text(encoding="utf-8"))
        executed.append(sql_file.name)
    return executed


def _list_main_relations(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        UNION
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'main'
        """
    ).fetchall()
    return sorted({row[0] for row in rows})


def _export_relations(conn: duckdb.DuckDBPyConnection, relation_names: list[str], out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    exported: list[Path] = []

    for relation in relation_names:
        out_file = out_dir / f"{relation}.csv"
        relation_escaped = relation.replace('"', '""')
        out_escaped = str(out_file).replace("'", "''")
        conn.execute(
            f"COPY (SELECT * FROM \"{relation_escaped}\") "
            f"TO '{out_escaped}' (HEADER, DELIMITER ',');"
        )
        exported.append(out_file)

    return exported


def run_transforms(
    db_path: str | Path,
    sql_root: str | Path = "sql",
    staging_out: str | Path = "data/staging",
    marts_out: str | Path = "data/marts",
) -> dict[str, int]:
    """Run staging + marts SQL models and export outputs."""
    sql_root_path = Path(sql_root)
    staging_sql_dir = sql_root_path / "staging"
    marts_sql_dir = sql_root_path / "marts"

    with duckdb.connect(str(db_path)) as conn:
        staged = _execute_sql_files(conn, _read_sql_files(staging_sql_dir))
        marts = _execute_sql_files(conn, _read_sql_files(marts_sql_dir))

        relations = _list_main_relations(conn)
        staging_relations = [name for name in relations if name.startswith("stg_")]
        mart_relations = [name for name in relations if name.startswith("dim_") or name.startswith("fct_")]

        exported_staging = _export_relations(conn, staging_relations, Path(staging_out))
        exported_marts = _export_relations(conn, mart_relations, Path(marts_out))

    return {
        "staging_sql_files": len(staged),
        "marts_sql_files": len(marts),
        "staging_exports": len(exported_staging),
        "marts_exports": len(exported_marts),
    }
