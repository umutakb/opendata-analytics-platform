from __future__ import annotations

import duckdb

from opendata_platform.quality.dq_checks import check_null_rate_thresholds, check_pk_uniqueness


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
