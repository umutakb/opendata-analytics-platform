from __future__ import annotations

from pathlib import Path

import duckdb


RAW_TABLES = ("customers", "products", "orders", "order_items")


def build_warehouse(raw_dir: str | Path, db_path: str | Path) -> dict[str, int]:
    """Load raw CSV files into DuckDB tables."""
    raw_path = Path(raw_dir)
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_file)) as conn:
        conn.execute("PRAGMA threads=4;")
        counts: dict[str, int] = {}

        for table in RAW_TABLES:
            csv_path = raw_path / f"{table}.csv"
            if not csv_path.exists():
                raise FileNotFoundError(f"Missing raw file: {csv_path}")

            quoted = str(csv_path).replace("'", "''")
            conn.execute(
                f"CREATE OR REPLACE TABLE {table} AS "
                f"SELECT * FROM read_csv_auto('{quoted}', header=true);"
            )
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            counts[table] = int(row_count)

    return counts
