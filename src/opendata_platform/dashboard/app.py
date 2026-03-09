from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db", default="data/warehouse.duckdb")
    parser.add_argument("--quality-report", default=None)
    return parser.parse_known_args()[0]


@st.cache_data(show_spinner=False)
def _run_query(db_path: str, query: str, params: list[Any] | None = None) -> pd.DataFrame:
    with duckdb.connect(db_path, read_only=True) as conn:
        if params:
            return conn.execute(query, params).df()
        return conn.execute(query).df()


def _load_quality_report(path: str) -> dict:
    report_path = Path(path)
    if not report_path.exists():
        return {}
    return json.loads(report_path.read_text(encoding="utf-8"))


def _resolve_quality_report_path(db_path: str, quality_report_arg: str | None) -> Path:
    if quality_report_arg:
        return Path(quality_report_arg)

    db_file = Path(db_path)
    project_root = db_file.parent.parent if db_file.parent.name == "data" else Path(".")
    artifacts_root = project_root / "artifacts"

    latest_quality_report = artifacts_root / "latest" / "quality" / "report.json"
    if latest_quality_report.exists():
        return latest_quality_report

    latest_run_pointer = artifacts_root / "latest_run.txt"
    if latest_run_pointer.exists():
        run_path = Path(latest_run_pointer.read_text(encoding="utf-8").strip())
        if not run_path.is_absolute():
            run_path = (project_root / run_path).resolve()
        run_quality_report = run_path / "quality" / "report.json"
        if run_quality_report.exists():
            return run_quality_report

    return artifacts_root / "quality" / "report.json"


def _format_range(min_date: Any, max_date: Any) -> str:
    if min_date is None or max_date is None or pd.isna(min_date) or pd.isna(max_date):
        return "Date range: N/A"
    min_value = pd.to_datetime(min_date).date().isoformat()
    max_value = pd.to_datetime(max_date).date().isoformat()
    return f"Date range: {min_value} to {max_value}"


def _resolve_default_date_range(db_path: str) -> tuple[date, date]:
    date_bounds = _run_query(
        db_path,
        """
        SELECT
          MIN(order_date) AS min_date,
          MAX(order_date) AS max_date
        FROM fct_orders
        WHERE order_status = 'paid';
        """,
    )
    min_date = date_bounds.iloc[0]["min_date"]
    max_date = date_bounds.iloc[0]["max_date"]

    if min_date is None or max_date is None or pd.isna(min_date) or pd.isna(max_date):
        today = date.today()
        return today, today

    return pd.to_datetime(min_date).date(), pd.to_datetime(max_date).date()


def _load_filter_options(db_path: str) -> tuple[list[str], list[str]]:
    countries_df = _run_query(
        db_path,
        """
        SELECT DISTINCT c.country
        FROM fct_orders o
        JOIN dim_customers c ON o.customer_id = c.customer_id
        WHERE o.order_status = 'paid'
        ORDER BY 1;
        """,
    )
    categories_df = _run_query(
        db_path,
        """
        SELECT DISTINCT p.category
        FROM fct_orders o
        JOIN fct_order_items oi ON o.order_id = oi.order_id
        JOIN dim_products p ON oi.product_id = p.product_id
        WHERE o.order_status = 'paid'
        ORDER BY 1;
        """,
    )

    countries = [str(v) for v in countries_df["country"].dropna().tolist()]
    categories = [str(v) for v in categories_df["category"].dropna().tolist()]
    return countries, categories


def _read_dashboard_filters(db_path: str) -> dict[str, Any]:
    default_start, default_end = _resolve_default_date_range(db_path)
    country_options, category_options = _load_filter_options(db_path)

    st.sidebar.header("Filters")
    selected_dates = st.sidebar.date_input(
        "Date range",
        value=(default_start, default_end),
        min_value=default_start,
        max_value=default_end,
    )
    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date = end_date = selected_dates
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    selected_countries = st.sidebar.multiselect("Country", options=country_options, default=[])
    selected_categories = st.sidebar.multiselect("Category", options=category_options, default=[])

    return {
        "start_date": start_date,
        "end_date": end_date,
        "countries": selected_countries,
        "categories": selected_categories,
    }


def _build_order_where_clause(filters: dict[str, Any], order_alias: str = "o", customer_alias: str = "c") -> tuple[str, list[Any]]:
    clauses = [
        f"{order_alias}.order_status = 'paid'",
        f"{order_alias}.order_date BETWEEN ? AND ?",
    ]
    params: list[Any] = [filters["start_date"], filters["end_date"]]

    countries = filters.get("countries", [])
    if countries:
        placeholders = ", ".join(["?"] * len(countries))
        clauses.append(f"{customer_alias}.country IN ({placeholders})")
        params.extend(countries)

    categories = filters.get("categories", [])
    if categories:
        placeholders = ", ".join(["?"] * len(categories))
        clauses.append(
            "EXISTS ("
            "SELECT 1 FROM fct_order_items oi_filter "
            "JOIN dim_products p_filter ON oi_filter.product_id = p_filter.product_id "
            f"WHERE oi_filter.order_id = {order_alias}.order_id AND p_filter.category IN ({placeholders})"
            ")"
        )
        params.extend(categories)

    return " AND ".join(clauses), params


def _build_top_category_where_clause(
    filters: dict[str, Any],
    order_alias: str = "o",
    customer_alias: str = "c",
    category_alias: str = "p",
) -> tuple[str, list[Any]]:
    clauses = [
        f"{order_alias}.order_status = 'paid'",
        f"{order_alias}.order_date BETWEEN ? AND ?",
    ]
    params: list[Any] = [filters["start_date"], filters["end_date"]]

    countries = filters.get("countries", [])
    if countries:
        placeholders = ", ".join(["?"] * len(countries))
        clauses.append(f"{customer_alias}.country IN ({placeholders})")
        params.extend(countries)

    categories = filters.get("categories", [])
    if categories:
        placeholders = ", ".join(["?"] * len(categories))
        clauses.append(f"{category_alias}.category IN ({placeholders})")
        params.extend(categories)

    return " AND ".join(clauses), params


def _compute_kpis(ts_df: pd.DataFrame) -> dict[str, Any]:
    if ts_df.empty:
        return {
            "gmv_30d": 0.0,
            "orders_30d": 0,
            "gmv_90d": 0.0,
            "orders_90d": 0,
            "range_30d": "Date range: N/A",
            "range_90d": "Date range: N/A",
        }

    work = ts_df.copy()
    work["metric_date"] = pd.to_datetime(work["metric_date"])
    reference_date = work["metric_date"].max().date()

    start_30d = reference_date - timedelta(days=29)
    start_90d = reference_date - timedelta(days=89)

    w30 = work.loc[work["metric_date"].dt.date >= start_30d]
    w90 = work.loc[work["metric_date"].dt.date >= start_90d]

    return {
        "gmv_30d": float(w30["gmv"].sum()),
        "orders_30d": int(w30["orders"].sum()),
        "gmv_90d": float(w90["gmv"].sum()),
        "orders_90d": int(w90["orders"].sum()),
        "range_30d": _format_range(w30["metric_date"].min(), w30["metric_date"].max()),
        "range_90d": _format_range(w90["metric_date"].min(), w90["metric_date"].max()),
    }


def _build_chart_df(ts_df: pd.DataFrame, value_col: str, ma_col: str) -> pd.DataFrame:
    ordered = ts_df.sort_values("metric_date").copy()
    if ordered.empty:
        return pd.DataFrame(columns=[value_col, ma_col], index=pd.DatetimeIndex([], name="metric_date"))

    full_index = pd.date_range(ordered["metric_date"].min(), ordered["metric_date"].max(), freq="D")
    series = ordered.set_index("metric_date")[value_col].reindex(full_index)
    series = series.fillna(0)
    ma_series = series.rolling(window=7, min_periods=1).mean()

    out = pd.DataFrame({value_col: series, ma_col: ma_series})
    out.index.name = "metric_date"
    return out


def _build_retention_display(retention_df: pd.DataFrame) -> pd.DataFrame:
    retention_work = retention_df.copy()
    retention_work["cohort_month"] = pd.to_datetime(retention_work["cohort_month"])

    pivot = retention_work.pivot(index="cohort_month", columns="month_n", values="retention_rate")
    pivot = pivot.sort_index()
    formatted = pivot.applymap(lambda x: "—" if pd.isna(x) else f"{x:.1%}")

    cohort_sizes = (
        retention_work.groupby("cohort_month", as_index=True)["cohort_size"]
        .max()
        .astype("Int64")
        .sort_index()
    )
    display = formatted.copy()
    display.insert(0, "cohort_size", cohort_sizes.astype(int))
    display = display.reset_index()
    display["cohort_month"] = display["cohort_month"].dt.date.astype(str)
    return display


def _extract_dq_table_rows(check_rows: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    def _parse_target(target: str) -> tuple[str, str]:
        if "." in target:
            left, right = target.split(".", 1)
            return left, right
        return target, ""

    normalized: list[dict[str, str]] = []
    for row in check_rows:
        target = str(row.get("target", ""))
        check_name = str(row.get("check_type", ""))
        _, column = _parse_target(target)
        normalized.append(
            {
                "check_name": check_name,
                "column": column,
                "status": str(row.get("status", "")),
                "message": str(row.get("message", "")),
            }
        )

    table = pd.DataFrame(normalized)
    if table.empty:
        empty = pd.DataFrame(columns=["check_name", "column", "status", "message"])
        return empty, empty

    failed = table[table["status"].str.lower() == "fail"].reset_index(drop=True)
    warned = table[table["status"].str.lower() == "warn"].reset_index(drop=True)
    return failed, warned


def main() -> None:
    args = _parse_args()
    quality_report_path = _resolve_quality_report_path(args.db, args.quality_report)

    st.set_page_config(page_title="OpenData Analytics Platform", layout="wide")
    filters = _read_dashboard_filters(args.db)
    st.title("OpenData Analytics Platform Dashboard")
    st.caption(f"Warehouse: {args.db}")
    order_where_clause, order_where_params = _build_order_where_clause(filters)
    ts_sql = f"""
    SELECT
      o.order_date AS metric_date,
      ROUND(SUM(o.order_amount_from_items), 2) AS gmv,
      COUNT(*) AS orders
    FROM fct_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE {order_where_clause}
    GROUP BY 1
    ORDER BY 1;
    """
    ts_df = _run_query(args.db, ts_sql, order_where_params)
    if not ts_df.empty:
        ts_df["metric_date"] = pd.to_datetime(ts_df["metric_date"])

    kpi_values = _compute_kpis(ts_df)
    gmv_30d = kpi_values["gmv_30d"]
    orders_30d = kpi_values["orders_30d"]
    gmv_90d = kpi_values["gmv_90d"]
    orders_90d = kpi_values["orders_90d"]
    range_30d = kpi_values["range_30d"]
    range_90d = kpi_values["range_90d"]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("GMV (30d)", f"{gmv_30d:,.2f}")
    col2.metric("Orders (30d)", f"{orders_30d:,}")
    col3.metric("GMV (90d)", f"{gmv_90d:,.2f}")
    col4.metric("AOV (90d)", f"{(gmv_90d / orders_90d) if orders_90d else 0:,.2f}")
    st.caption("Paid orders only")
    st.caption(f"30d window: {range_30d}")
    st.caption(f"90d window: {range_90d}")

    gmv_chart_df = _build_chart_df(ts_df, "gmv", "gmv_ma7")
    orders_chart_df = _build_chart_df(ts_df, "orders", "orders_ma7")

    left, right = st.columns(2)
    left.subheader("Daily GMV")
    if gmv_chart_df.empty:
        left.info("No data for selected filters.")
    else:
        left.line_chart(gmv_chart_df)

    right.subheader("Daily Orders")
    if orders_chart_df.empty:
        right.info("No data for selected filters.")
    else:
        right.line_chart(orders_chart_df)

    top_categories_where_clause, top_categories_params = _build_top_category_where_clause(filters)
    top_categories_sql = f"""
    SELECT
      p.category,
      ROUND(SUM(oi.item_total), 2) AS gmv
    FROM fct_order_items oi
    JOIN fct_orders o ON oi.order_id = o.order_id
    JOIN dim_products p ON oi.product_id = p.product_id
    JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE {top_categories_where_clause}
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10;
    """
    cat_df = _run_query(args.db, top_categories_sql, top_categories_params)
    st.subheader("Top Categories by GMV")
    if cat_df.empty:
        st.info("No category data for selected filters.")
    else:
        st.bar_chart(cat_df.set_index("category")["gmv"])

    retention_sql = """
    WITH paid_orders AS (
      SELECT customer_id, DATE_TRUNC('month', order_date) AS order_month
      FROM fct_orders
      WHERE order_status = 'paid'
    ),
    first_paid AS (
      SELECT customer_id, MIN(order_month) AS cohort_month
      FROM paid_orders
      GROUP BY 1
    ),
    cohort_activity AS (
      SELECT
        fp.cohort_month,
        DATE_DIFF('month', fp.cohort_month, po.order_month) AS month_n,
        po.customer_id
      FROM first_paid fp
      JOIN paid_orders po USING (customer_id)
    ),
    cohort_size AS (
      SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size
      FROM first_paid
      GROUP BY 1
    )
    SELECT
      ca.cohort_month::DATE AS cohort_month,
      ca.month_n,
      COUNT(DISTINCT ca.customer_id) AS retained_customers,
      cs.cohort_size,
      COUNT(DISTINCT ca.customer_id) * 1.0 / NULLIF(cs.cohort_size, 0) AS retention_rate
    FROM cohort_activity ca
    JOIN cohort_size cs USING (cohort_month)
    GROUP BY 1, 2, 4
    ORDER BY 1, 2;
    """
    retention_df = _run_query(args.db, retention_sql)
    if not retention_df.empty:
        st.subheader("Cohort Retention")
        st.dataframe(_build_retention_display(retention_df), use_container_width=True)

    quality_report = _load_quality_report(str(quality_report_path))
    if not quality_report:
        st.subheader("Data Quality Status")
        st.info(
            "Quality report bulunamadı. Lütfen `make demo` veya "
            "`opdata quality --db data/warehouse.duckdb --out artifacts/quality --config config.example.yml` çalıştırın."
        )
        return

    summary = quality_report.get("summary", {})
    last_run = quality_report.get("generated_at") or quality_report.get("timestamp") or "N/A"

    st.subheader("Data Quality Status")
    q1, q2, q3 = st.columns(3)
    q1.metric("PASS", summary.get("pass", 0))
    q2.metric("WARN", summary.get("warn", 0))
    q3.metric("FAIL", summary.get("fail", 0))
    st.caption(f"Last run: {last_run}")

    failed_df, warn_df = _extract_dq_table_rows(quality_report.get("checks", []))
    if not failed_df.empty:
        st.write("Failed checks")
        st.dataframe(failed_df.head(20), use_container_width=True)
    else:
        st.success("No failed checks.")
        if not warn_df.empty:
            st.write("Warn checks")
            st.dataframe(warn_df.head(20), use_container_width=True)


if __name__ == "__main__":
    main()
