from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


def _parse_end_date(value: str | None) -> date:
    if value is None:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def generate_synthetic_data(
    out_dir: str | Path,
    seed: int = 42,
    days: int = 365,
    n_orders: int | None = None,
    n_customers: int | None = None,
    n_products: int = 1200,
    end_date: str | None = None,
) -> dict[str, int]:
    """Generate deterministic synthetic e-commerce CSVs."""
    if days < 1:
        raise ValueError("days must be >= 1")

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)
    end_dt = _parse_end_date(end_date)

    order_target = n_orders if n_orders is not None else max(50000, days * 150)
    customer_target = n_customers if n_customers is not None else max(10000, int(order_target * 0.35))

    date_index = pd.date_range(end=end_dt, periods=days, freq="D")
    weekday_weights = np.array([1.0, 1.05, 1.1, 1.15, 1.25, 1.45, 0.85])
    weights = weekday_weights[date_index.dayofweek.to_numpy()]
    orders_per_day = rng.multinomial(order_target, weights / weights.sum())

    start_date = date_index.min().date()

    countries = np.array(["US", "DE", "TR", "GB", "FR", "CA", "AU", "NL"])
    country_p = np.array([0.45, 0.08, 0.09, 0.12, 0.1, 0.07, 0.05, 0.04])

    customer_ids = np.arange(1, customer_target + 1, dtype=np.int64)
    signup_origin = start_date - timedelta(days=730)
    signup_offsets = rng.integers(0, (end_dt - signup_origin).days + 1, size=customer_target)
    signup_dates = [signup_origin + timedelta(days=int(x)) for x in signup_offsets]
    customer_countries = rng.choice(countries, size=customer_target, p=country_p)

    domains = np.array(["example.com", "mail.com", "shop.org", "retail.co"])
    domain_choices = rng.choice(domains, size=customer_target)
    emails = np.array(
        [f"cust{cid:07d}@{domain}" for cid, domain in zip(customer_ids, domain_choices)],
        dtype=object,
    )
    emails[rng.random(customer_target) < 0.005] = None

    customers_df = pd.DataFrame(
        {
            "customer_id": customer_ids,
            "signup_date": signup_dates,
            "country": customer_countries,
            "email": emails,
        }
    )

    categories = np.array(
        [
            "Electronics",
            "Home",
            "Fashion",
            "Beauty",
            "Sports",
            "Books",
            "Toys",
            "Grocery",
            "Automotive",
            "Office",
        ]
    )
    category_p = np.array([0.14, 0.14, 0.16, 0.08, 0.1, 0.08, 0.08, 0.1, 0.06, 0.06])

    product_ids = np.arange(1, n_products + 1, dtype=np.int64)
    product_categories = rng.choice(categories, size=n_products, p=category_p)

    created_origin = start_date - timedelta(days=400)
    created_offsets = rng.integers(0, (end_dt - created_origin).days + 1, size=n_products)
    created_dates = [created_origin + timedelta(days=int(x)) for x in created_offsets]

    category_price_params = {
        "Electronics": (4.9, 0.35),
        "Home": (4.0, 0.4),
        "Fashion": (3.5, 0.35),
        "Beauty": (3.3, 0.3),
        "Sports": (4.0, 0.35),
        "Books": (2.8, 0.25),
        "Toys": (3.2, 0.3),
        "Grocery": (2.5, 0.25),
        "Automotive": (4.2, 0.35),
        "Office": (3.2, 0.3),
    }

    base_prices = np.zeros(n_products, dtype=float)
    for category, (mu, sigma) in category_price_params.items():
        mask = product_categories == category
        if mask.any():
            draws = np.exp(rng.normal(mu, sigma, mask.sum()))
            base_prices[mask] = np.clip(draws, 0.5, 3000.0)
    base_prices = np.round(base_prices, 2)

    products_df = pd.DataFrame(
        {
            "product_id": product_ids,
            "category": product_categories,
            "created_at": created_dates,
            "base_price": base_prices,
        }
    )

    order_ids = np.arange(1, order_target + 1, dtype=np.int64)
    order_dates = np.repeat(date_index.date, orders_per_day)
    order_customer_ids = rng.integers(1, customer_target + 1, size=order_target)
    statuses = rng.choice(np.array(["paid", "canceled", "refunded"]), size=order_target, p=[0.9, 0.07, 0.03])

    line_count_options = np.array([1, 2, 3, 4, 5])
    line_count_p = np.array([0.5, 0.25, 0.15, 0.07, 0.03])
    line_counts = rng.choice(line_count_options, size=order_target, p=line_count_p)

    expanded_order_ids = np.repeat(order_ids, line_counts)
    total_line_items = expanded_order_ids.shape[0]

    product_popularity = rng.lognormal(mean=0.0, sigma=1.0, size=n_products)
    product_popularity = product_popularity / product_popularity.sum()
    item_product_ids = rng.choice(product_ids, size=total_line_items, p=product_popularity)
    quantities = rng.choice(np.array([1, 2, 3, 4]), size=total_line_items, p=[0.65, 0.22, 0.1, 0.03])

    price_lookup = np.zeros(n_products + 1)
    price_lookup[product_ids] = base_prices

    item_unit_prices = price_lookup[item_product_ids] * rng.uniform(0.85, 1.2, size=total_line_items)
    item_unit_prices = np.clip(item_unit_prices, 0.01, 50000.0)
    item_unit_prices = np.round(item_unit_prices, 2)

    item_totals = np.round(quantities * item_unit_prices, 2)

    order_amount_from_items = np.bincount(
        expanded_order_ids,
        weights=item_totals,
        minlength=order_target + 1,
    )[1:]

    rounding_noise = rng.normal(0.0, 0.003, size=order_target)
    order_total_amount = np.round(np.clip(order_amount_from_items * (1 + rounding_noise), 0.0, None), 2)

    orders_df = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": order_customer_ids,
            "order_date": order_dates,
            "status": statuses,
            "total_amount": order_total_amount,
        }
    )

    order_items_df = pd.DataFrame(
        {
            "order_id": expanded_order_ids,
            "product_id": item_product_ids,
            "quantity": quantities,
            "unit_price": item_unit_prices,
        }
    )

    customers_df.to_csv(out_path / "customers.csv", index=False)
    products_df[["product_id", "category", "created_at"]].to_csv(out_path / "products.csv", index=False)
    orders_df.to_csv(out_path / "orders.csv", index=False)
    order_items_df.to_csv(out_path / "order_items.csv", index=False)

    return {
        "customers": int(customers_df.shape[0]),
        "products": int(products_df.shape[0]),
        "orders": int(orders_df.shape[0]),
        "order_items": int(order_items_df.shape[0]),
    }
