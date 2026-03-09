from __future__ import annotations

import argparse
import json
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
def _run_query(db_path: str, query: str) -> pd.DataFrame:
    with duckdb.connect(db_path, read_only=True) as conn:
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
    if db_file.parent.name == "data":
        return db_file.parent.parent / "artifacts" / "quality" / "report.json"
    return Path("artifacts/quality/report.json")


def _format_range(min_date: Any, max_date: Any) -> str:
    if min_date is None or max_date is None:
        return "Date range: N/A"
    return f"Date range: {min_date} to {max_date}"


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
    st.title("OpenData Analytics Platform Dashboard")
    st.caption(f"Warehouse: {args.db}")

    kpi_sql = """
    WITH paid_orders AS (
      SELECT order_date, order_amount_from_items
      FROM fct_orders
      WHERE order_status = 'paid'
    )
    SELECT
      ROUND(SUM(CASE WHEN order_date >= current_date - INTERVAL 30 DAY THEN order_amount_from_items ELSE 0 END), 2) AS gmv_30d,
      COUNT(CASE WHEN order_date >= current_date - INTERVAL 30 DAY THEN 1 ELSE NULL END) AS orders_30d,
      ROUND(SUM(CASE WHEN order_date >= current_date - INTERVAL 90 DAY THEN order_amount_from_items ELSE 0 END), 2) AS gmv_90d,
      COUNT(CASE WHEN order_date >= current_date - INTERVAL 90 DAY THEN 1 ELSE NULL END) AS orders_90d,
      MIN(CASE WHEN order_date >= current_date - INTERVAL 30 DAY THEN order_date END) AS min_date_30d,
      MAX(CASE WHEN order_date >= current_date - INTERVAL 30 DAY THEN order_date END) AS max_date_30d,
      MIN(CASE WHEN order_date >= current_date - INTERVAL 90 DAY THEN order_date END) AS min_date_90d,
      MAX(CASE WHEN order_date >= current_date - INTERVAL 90 DAY THEN order_date END) AS max_date_90d
    FROM paid_orders;
    """
    kpi_df = _run_query(args.db, kpi_sql)
    gmv_30d = float(kpi_df.iloc[0]["gmv_30d"] or 0)
    orders_30d = int(kpi_df.iloc[0]["orders_30d"] or 0)
    gmv_90d = float(kpi_df.iloc[0]["gmv_90d"] or 0)
    orders_90d = int(kpi_df.iloc[0]["orders_90d"] or 0)
    range_30d = _format_range(kpi_df.iloc[0]["min_date_30d"], kpi_df.iloc[0]["max_date_30d"])
    range_90d = _format_range(kpi_df.iloc[0]["min_date_90d"], kpi_df.iloc[0]["max_date_90d"])

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("GMV (30d)", f"{gmv_30d:,.2f}")
    col2.metric("Orders (30d)", f"{orders_30d:,}")
    col3.metric("GMV (90d)", f"{gmv_90d:,.2f}")
    col4.metric("AOV (90d)", f"{(gmv_90d / orders_90d) if orders_90d else 0:,.2f}")
    st.caption("Paid orders only")
    st.caption(f"30d window: {range_30d}")
    st.caption(f"90d window: {range_90d}")

    ts_sql = """
    SELECT
      order_date AS metric_date,
      ROUND(SUM(order_amount_from_items), 2) AS gmv,
      COUNT(*) AS orders
    FROM fct_orders
    WHERE order_status = 'paid'
    GROUP BY 1
    ORDER BY 1;
    """
    ts_df = _run_query(args.db, ts_sql)
    ts_df["metric_date"] = pd.to_datetime(ts_df["metric_date"])
    gmv_chart_df = _build_chart_df(ts_df, "gmv", "gmv_ma7")
    orders_chart_df = _build_chart_df(ts_df, "orders", "orders_ma7")

    left, right = st.columns(2)
    left.subheader("Daily GMV")
    left.line_chart(gmv_chart_df)

    right.subheader("Daily Orders")
    right.line_chart(orders_chart_df)

    top_categories_sql = """
    SELECT
      p.category,
      ROUND(SUM(oi.item_total), 2) AS gmv
    FROM fct_order_items oi
    JOIN fct_orders o ON oi.order_id = o.order_id
    JOIN dim_products p ON oi.product_id = p.product_id
    WHERE o.order_status = 'paid'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10;
    """
    cat_df = _run_query(args.db, top_categories_sql)
    st.subheader("Top Categories by GMV")
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
