from __future__ import annotations

from pathlib import Path
from urllib.request import urlretrieve

import pandas as pd

from opendata_platform.ingest.generate_data import generate_synthetic_data


OPEN_DATASETS = {
    "uci_online_retail": "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx",
}


def _derive_category(description: str) -> str:
    text = str(description).strip()
    if not text:
        return "General"
    first_token = text.split()[0].strip(".,;:-_")
    return first_token.title() if first_token else "General"


def _build_open_dataset_raw_files(dataset: str, out_dir: Path) -> dict[str, int]:
    if dataset not in OPEN_DATASETS:
        raise ValueError(f"Unsupported open dataset: {dataset}")

    out_dir.mkdir(parents=True, exist_ok=True)
    download_dir = out_dir / "_downloads"
    download_dir.mkdir(parents=True, exist_ok=True)
    source_file = download_dir / f"{dataset}.xlsx"
    urlretrieve(OPEN_DATASETS[dataset], source_file)

    df = pd.read_excel(source_file)
    required_cols = {
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Open dataset missing columns: {sorted(missing)}")

    work = df.copy()
    work["CustomerID"] = pd.to_numeric(work["CustomerID"], errors="coerce")
    work["Quantity"] = pd.to_numeric(work["Quantity"], errors="coerce")
    work["UnitPrice"] = pd.to_numeric(work["UnitPrice"], errors="coerce")
    work["InvoiceDate"] = pd.to_datetime(work["InvoiceDate"], errors="coerce")
    work["InvoiceNo"] = work["InvoiceNo"].astype(str).str.strip()
    work["StockCode"] = work["StockCode"].astype(str).str.strip()

    work = work.dropna(subset=["CustomerID", "Quantity", "UnitPrice", "InvoiceDate", "InvoiceNo", "StockCode"])
    work = work[(work["Quantity"] > 0) & (work["UnitPrice"] > 0)]
    if work.empty:
        raise ValueError("Open dataset has no usable rows after filtering.")

    work["customer_id"] = work["CustomerID"].astype("int64")
    work["order_date"] = work["InvoiceDate"].dt.date
    work["order_key"] = work["InvoiceNo"] + "::" + work["customer_id"].astype(str)

    unique_orders = sorted(work["order_key"].unique().tolist())
    order_id_map = {key: idx + 1 for idx, key in enumerate(unique_orders)}
    work["order_id"] = work["order_key"].map(order_id_map).astype("int64")

    unique_stock = sorted(work["StockCode"].astype(str).unique().tolist())
    product_id_map = {stock: idx + 1 for idx, stock in enumerate(unique_stock)}
    work["product_id"] = work["StockCode"].astype(str).map(product_id_map).astype("int64")

    order_items_df = pd.DataFrame(
        {
            "order_id": work["order_id"].astype("int64"),
            "product_id": work["product_id"].astype("int64"),
            "quantity": work["Quantity"].astype("int64"),
            "unit_price": work["UnitPrice"].round(2),
        }
    )

    orders_df = (
        work.groupby("order_id", as_index=False)
        .agg(
            customer_id=("customer_id", "first"),
            order_date=("order_date", "min"),
            total_amount=("UnitPrice", lambda s: 0.0),
        )
        .sort_values("order_id")
    )
    order_totals = (
        order_items_df.assign(item_total=order_items_df["quantity"] * order_items_df["unit_price"])
        .groupby("order_id", as_index=False)["item_total"]
        .sum()
    )
    orders_df = orders_df.drop(columns=["total_amount"]).merge(order_totals, on="order_id", how="left")
    orders_df = orders_df.rename(columns={"item_total": "total_amount"})
    orders_df["status"] = "paid"
    orders_df["total_amount"] = orders_df["total_amount"].round(2)

    customers_df = (
        work.groupby("customer_id", as_index=False)
        .agg(signup_date=("order_date", "min"), country=("Country", "first"))
        .sort_values("customer_id")
    )
    customers_df["email"] = customers_df["customer_id"].map(lambda cid: f"cust{int(cid):07d}@open-data.local")

    products_df = (
        work.groupby("product_id", as_index=False)
        .agg(category=("Description", lambda s: _derive_category(s.iloc[0])))
        .sort_values("product_id")
    )
    created_at = orders_df["order_date"].min()
    products_df["created_at"] = created_at

    customers_df[["customer_id", "signup_date", "country", "email"]].to_csv(out_dir / "customers.csv", index=False)
    products_df[["product_id", "category", "created_at"]].to_csv(out_dir / "products.csv", index=False)
    orders_df[["order_id", "customer_id", "order_date", "status", "total_amount"]].to_csv(
        out_dir / "orders.csv",
        index=False,
    )
    order_items_df[["order_id", "product_id", "quantity", "unit_price"]].to_csv(out_dir / "order_items.csv", index=False)

    return {
        "customers": int(customers_df.shape[0]),
        "products": int(products_df.shape[0]),
        "orders": int(orders_df.shape[0]),
        "order_items": int(order_items_df.shape[0]),
    }


def ingest_data(
    source: str,
    out_dir: str | Path,
    seed: int = 42,
    days: int = 365,
    n_orders: int | None = None,
    n_customers: int | None = None,
    n_products: int = 1200,
    end_date: str | None = None,
    dataset: str = "uci_online_retail",
) -> dict[str, object]:
    out_path = Path(out_dir)

    if source == "synthetic":
        stats = generate_synthetic_data(
            out_dir=out_path,
            seed=seed,
            days=days,
            n_orders=n_orders,
            n_customers=n_customers,
            n_products=n_products,
            end_date=end_date,
        )
        return {"source_requested": source, "source_used": "synthetic", "dataset": None, "stats": stats}

    if source != "open":
        raise ValueError(f"Unsupported source: {source}")

    try:
        stats = _build_open_dataset_raw_files(dataset=dataset, out_dir=out_path)
        return {"source_requested": source, "source_used": "open", "dataset": dataset, "stats": stats}
    except Exception as exc:
        stats = generate_synthetic_data(
            out_dir=out_path,
            seed=seed,
            days=days,
            n_orders=n_orders,
            n_customers=n_customers,
            n_products=n_products,
            end_date=end_date,
        )
        return {
            "source_requested": source,
            "source_used": "synthetic",
            "dataset": dataset,
            "stats": stats,
            "fallback_message": (
                f"Open dataset ingest failed ({exc}). Falling back to synthetic data generation."
            ),
        }
