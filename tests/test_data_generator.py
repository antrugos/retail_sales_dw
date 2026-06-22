from data_generator.generate_retail_data import (
    generate_daily_sales,
    generate_products,
    generate_stores,
)
from datetime import date


def test_generate_stores_returns_requested_count():
    stores = generate_stores(n=5)
    assert len(stores) == 5
    assert len({s.store_id for s in stores}) == 5  # ids únicos


def test_generate_products_have_positive_prices():
    products = generate_products(n=20)
    assert all(p.unit_price > 0 for p in products)


def test_daily_sales_total_amount_matches_quantity_times_price():
    stores = generate_stores(n=2)
    products = generate_products(n=3)
    rows = generate_daily_sales(date(2026, 6, 19), stores, products, n_transactions=30)

    assert len(rows) == 30
    for row in rows:
        expected_total = round(row["quantity"] * row["unit_price"], 2)
        assert row["total_amount"] == expected_total


def test_daily_sales_ids_are_unique_within_a_day():
    stores = generate_stores(n=2)
    products = generate_products(n=3)
    rows = generate_daily_sales(date(2026, 6, 19), stores, products, n_transactions=50)

    ids = [row["sale_id"] for row in rows]
    assert len(ids) == len(set(ids))
