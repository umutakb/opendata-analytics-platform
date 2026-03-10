from __future__ import annotations

from pathlib import Path

import duckdb

from opendata_platform.quality.dq_checks import discover_quality_checks, run_quality_checks


def _seed_minimum_relations(db_path: Path) -> None:
    statements = [
        "CREATE TABLE customers (customer_id INTEGER, signup_date DATE, country VARCHAR, email VARCHAR)",
        "CREATE TABLE products (product_id INTEGER, category VARCHAR, created_at DATE)",
        "CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, order_date DATE, status VARCHAR, total_amount DOUBLE)",
        "CREATE TABLE order_items (order_id INTEGER, product_id INTEGER, quantity INTEGER, unit_price DOUBLE)",
        "CREATE TABLE stg_customers (customer_id INTEGER, signup_date DATE, country VARCHAR, email VARCHAR)",
        "CREATE TABLE stg_products (product_id INTEGER, category VARCHAR, created_at DATE)",
        "CREATE TABLE stg_orders (order_id INTEGER, customer_id INTEGER, order_date DATE, order_status VARCHAR, total_amount DOUBLE)",
        "CREATE TABLE stg_order_items (order_id INTEGER, product_id INTEGER, quantity INTEGER, unit_price DOUBLE)",
        "CREATE TABLE dim_customers (customer_id INTEGER, signup_date DATE, country VARCHAR, email VARCHAR)",
        "CREATE TABLE dim_products (product_id INTEGER, category VARCHAR, created_at DATE)",
        (
            "CREATE TABLE fct_orders ("
            "order_id INTEGER, customer_id INTEGER, order_date DATE, order_status VARCHAR, "
            "order_total_amount DOUBLE, order_amount_from_items DOUBLE, reconciliation_delta DOUBLE, item_lines INTEGER)"
        ),
        "CREATE TABLE fct_order_items (order_id INTEGER, product_id INTEGER, quantity INTEGER, unit_price DOUBLE, item_total DOUBLE)",
    ]

    with duckdb.connect(str(db_path)) as conn:
        for stmt in statements:
            conn.execute(stmt)

        conn.execute("INSERT INTO customers VALUES (1, DATE '2026-01-01', 'TR', 'a@example.com')")
        conn.execute("INSERT INTO products VALUES (10, 'Electronics', DATE '2026-01-01')")
        conn.execute("INSERT INTO orders VALUES (100, 1, DATE '2026-01-02', 'paid', 199.90)")
        conn.execute("INSERT INTO order_items VALUES (100, 10, 1, 199.90)")


def test_discover_quality_checks_sorted() -> None:
    modules = discover_quality_checks()
    names = [module.__name__.rsplit(".", 1)[-1] for module in modules]

    assert names == sorted(names)
    assert {
        "freshness",
        "null_rate",
        "numeric_ranges",
        "pk_uniqueness",
        "referential_integrity",
        "schema_existence",
    }.issubset(set(names))


def test_run_quality_checks_uses_discovered_plugins(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.duckdb"
    _seed_minimum_relations(db_path)

    report = run_quality_checks(
        db_path,
        {
            "quality": {
                "null_rate_thresholds": {},
                "pk_uniqueness": [],
                "ranges": {},
                "freshness": {},
            }
        },
    )

    check_types = {row["check_type"] for row in report["checks"]}

    assert report["generated_at"]
    assert "schema_existence" in check_types
    assert "referential_integrity" in check_types
    assert report["summary"]["fail"] == 0
