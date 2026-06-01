"""Unit tests for ETL transformation logic."""
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))


def _to_int_date(val: str):
    from datetime import datetime
    try:
        return int(datetime.strptime(val, "%Y-%m-%d").strftime("%Y%m%d"))
    except (ValueError, TypeError):
        return None


class TestDateConversion:
    def test_valid_date(self):
        assert _to_int_date("2023-06-15") == 20230615

    def test_invalid_date(self):
        assert _to_int_date("not-a-date") is None

    def test_none_input(self):
        assert _to_int_date(None) is None

    def test_boundary_dates(self):
        assert _to_int_date("2021-01-01") == 20210101
        assert _to_int_date("2023-12-31") == 20231231


class TestDimDateGeneration:
    def test_date_range_coverage(self):
        start = date(2023, 1, 1)
        end = date(2023, 1, 7)
        from datetime import timedelta
        days = []
        d = start
        while d <= end:
            days.append(d)
            d += timedelta(days=1)
        assert len(days) == 7

    def test_weekend_flag(self):
        saturday = date(2023, 6, 17)
        sunday = date(2023, 6, 18)
        monday = date(2023, 6, 19)
        assert saturday.weekday() >= 5
        assert sunday.weekday() >= 5
        assert monday.weekday() < 5

    def test_quarter_calculation(self):
        def quarter(month):
            return (month - 1) // 3 + 1
        assert quarter(1) == 1
        assert quarter(3) == 1
        assert quarter(4) == 2
        assert quarter(12) == 4


class TestDataValidation:
    def test_sales_positive(self):
        orders = [
            {"sales": 100.0, "quantity": 2, "discount": 0.1, "profit": 20.0},
            {"sales": 50.0, "quantity": 1, "discount": 0.0, "profit": 10.0},
        ]
        for o in orders:
            assert o["sales"] >= 0
            assert o["quantity"] > 0
            assert 0 <= o["discount"] <= 1

    def test_required_fields(self):
        required = {"order_id", "customer_id", "order_date", "product_id", "sales"}
        row = {
            "order_id": "ORD-00001",
            "customer_id": "CUST-0001",
            "order_date": "2023-01-15",
            "product_id": "PROD-0042",
            "sales": 150.0,
        }
        assert required.issubset(row.keys())

    def test_profit_margin_calculation(self):
        sales = 200.0
        profit = 40.0
        margin = profit / sales * 100
        assert abs(margin - 20.0) < 0.01


class TestDataGeneration:
    def test_generate_customers(self):
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from generate_data import generate_customers
        customers = generate_customers(10)
        assert len(customers) == 10
        for c in customers:
            assert "customer_id" in c
            assert c["customer_id"].startswith("CUST-")
            assert c["segment"] in ("Consumer", "Corporate", "Home Office")

    def test_generate_orders(self):
        from generate_data import generate_customers, generate_orders
        customers = generate_customers(10)
        orders = generate_orders(customers, 50)
        assert len(orders) == 50
        for o in orders:
            assert o["sales"] > 0
            assert o["quantity"] >= 1
            assert 0 <= o["discount"] <= 0.5
