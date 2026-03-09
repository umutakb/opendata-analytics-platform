from __future__ import annotations

import duckdb

from opendata_platform.quality.dq_checks import (
    check_null_rate_thresholds,
    check_pk_uniqueness,
    check_referential_integrity,
)


def test_null_rate_check_fail() -> None:
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE customers (customer_id INTEGER, email VARCHAR)")
        conn.execute(
            "INSERT INTO customers VALUES (1, 'a@example.com'), (2, NULL), (3, NULL), (4, 'd@example.com')"
        )

        results = check_null_rate_thresholds(
            conn,
            {"customers.email": {"warn": 0.2, "fail": 0.4}},
        )

    assert len(results) == 1
    assert results[0]["status"] == "fail"


def test_pk_uniqueness_check_fail() -> None:
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE products (product_id INTEGER)")
        conn.execute("INSERT INTO products VALUES (1), (2), (2), (3)")

        results = check_pk_uniqueness(conn, ["products.product_id"])

    assert len(results) == 1
    assert results[0]["status"] == "fail"
    assert results[0]["observed"] == 1


def test_referential_integrity_check_fail() -> None:
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE customers (customer_id INTEGER)")
        conn.execute("CREATE TABLE orders (order_id INTEGER, customer_id INTEGER)")
        conn.execute("INSERT INTO customers VALUES (1)")
        conn.execute("INSERT INTO orders VALUES (100, 1), (101, 999)")

        rules = [
            {
                "name": "orders.customer_id -> customers.customer_id",
                "left_table": "orders",
                "left_column": "customer_id",
                "right_table": "customers",
                "right_column": "customer_id",
            }
        ]
        results = check_referential_integrity(conn, rules)

    assert len(results) == 1
    assert results[0]["status"] == "fail"
    assert results[0]["missing_count"] == 1
    assert 999 in results[0]["sample_rows"]
