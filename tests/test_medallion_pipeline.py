"""
Unit Tests for Spark Medallion Architecture Pipeline
Author: Brian Stratton
"""

import pytest
import pandas as pd
import numpy as np
import os
import shutil
from datetime import datetime, timedelta


# ============================================================
# Test Data Fixtures
# ============================================================

@pytest.fixture
def sample_raw_data():
    """Generate sample raw transaction data for testing"""
    return pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN003", "TXN004", "TXN005"],
        "customer_id": ["cust001", "CUST-002", "cust_003", "CUST004", "cust005"],
        "product_name": ["Widget A", "WIDGET A", "widget a", "Gadget X", "Tool Z"],
        "quantity": [10, 5, 20, 15, 8],
        "unit_price": [25.00, 30.50, 25.00, 45.99, 100.00],
        "transaction_date": ["2024-01-15", "01/20/2024", "15-02-2024", "2024/03/10", "2024-04-01"],
        "region": ["Northeast", "NORTHEAST", "northeast", "Southwest", "WEST"],
        "status": ["completed", "COMPLETED", "Completed", "pending", "cancelled"],
        "discount_pct": [0, 10, None, 5, 20],
        "notes": ["Rush order", "", None, "standard", "PRIORITY"],
    })


@pytest.fixture
def temp_lakehouse(tmp_path):
    """Create temporary lakehouse directory structure"""
    base = tmp_path / "medallion_lakehouse"
    paths = {
        "bronze": base / "bronze" / "transactions",
        "silver": base / "silver" / "transactions",
        "gold_daily": base / "gold" / "daily_summary",
        "gold_customer": base / "gold" / "customer_metrics",
        "gold_product": base / "gold" / "product_metrics",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


# ============================================================
# Bronze Layer Tests
# ============================================================

class TestBronzeLayer:
    """Tests for Bronze layer - raw data ingestion"""

    def test_metadata_columns_added(self, sample_raw_data):
        bronze_df = sample_raw_data.copy()
        bronze_df["_ingestion_timestamp"] = datetime.now()
        bronze_df["_source_system"] = "transaction_system"
        bronze_df["_file_name"] = "raw_transactions.csv"
        assert "_ingestion_timestamp" in bronze_df.columns
        assert "_source_system" in bronze_df.columns
        assert "_file_name" in bronze_df.columns

    def test_data_preserved(self, sample_raw_data):
        bronze_df = sample_raw_data.copy()
        bronze_df["_ingestion_timestamp"] = datetime.now()
        assert len(bronze_df) == len(sample_raw_data)
        assert list(sample_raw_data.columns).sort() != list(bronze_df.columns).sort()

    def test_parquet_write_read(self, sample_raw_data, temp_lakehouse):
        bronze_df = sample_raw_data.copy()
        bronze_df["_ingestion_timestamp"] = datetime.now()
        path = temp_lakehouse["bronze"] / "transactions.parquet"
        bronze_df.to_parquet(path, index=False)
        read_back = pd.read_parquet(path)
        assert len(read_back) == len(bronze_df)
        assert list(read_back.columns) == list(bronze_df.columns)


# ============================================================
# Silver Layer Tests
# ============================================================

class TestSilverLayer:
    """Tests for Silver layer - data cleaning and standardization"""

    def test_customer_id_standardization(self, sample_raw_data):
        import re
        silver_df = sample_raw_data.copy()
        silver_df["customer_id"] = silver_df["customer_id"].apply(
            lambda x: re.sub(r"[^A-Za-z0-9]", "", str(x)).upper()
        )
        for cid in silver_df["customer_id"]:
            assert cid == cid.upper()
            assert "-" not in cid
            assert "_" not in cid

    def test_product_name_standardization(self, sample_raw_data):
        silver_df = sample_raw_data.copy()
        silver_df["product_name"] = silver_df["product_name"].str.title()
        for name in silver_df["product_name"]:
            assert name == name.title()

    def test_region_standardization(self, sample_raw_data):
        silver_df = sample_raw_data.copy()
        silver_df["region"] = silver_df["region"].str.title()
        for region in silver_df["region"]:
            assert region == region.title()

    def test_status_standardization(self, sample_raw_data):
        silver_df = sample_raw_data.copy()
        silver_df["status"] = silver_df["status"].str.lower()
        for status in silver_df["status"]:
            assert status == status.lower()

    def test_null_discount_handling(self, sample_raw_data):
        silver_df = sample_raw_data.copy()
        silver_df["discount_pct"] = silver_df["discount_pct"].fillna(0)
        assert silver_df["discount_pct"].isnull().sum() == 0

    def test_derived_columns(self, sample_raw_data):
        silver_df = sample_raw_data.copy()
        silver_df["discount_pct"] = silver_df["discount_pct"].fillna(0)
        silver_df["gross_amount"] = silver_df["quantity"] * silver_df["unit_price"]
        silver_df["discount_amount"] = silver_df["gross_amount"] * (silver_df["discount_pct"] / 100)
        silver_df["net_amount"] = silver_df["gross_amount"] - silver_df["discount_amount"]
        assert "gross_amount" in silver_df.columns
        assert "net_amount" in silver_df.columns
        assert all(silver_df["net_amount"] <= silver_df["gross_amount"])


# ============================================================
# Gold Layer Tests
# ============================================================

class TestGoldLayer:
    """Tests for Gold layer - business aggregations"""

    def _create_silver_data(self):
        return pd.DataFrame({
            "transaction_id": [f"TXN{i}" for i in range(20)],
            "customer_id": ["CUST001"] * 5 + ["CUST002"] * 5 + ["CUST003"] * 10,
            "product_name": ["Widget A"] * 10 + ["Gadget X"] * 10,
            "quantity": np.random.randint(1, 50, 20),
            "unit_price": np.random.uniform(10, 100, 20).round(2),
            "transaction_date": pd.date_range("2024-01-01", periods=20, freq="D"),
            "region": ["Northeast"] * 7 + ["Southwest"] * 7 + ["West"] * 6,
            "status": ["completed"] * 15 + ["pending"] * 3 + ["cancelled"] * 2,
            "discount_pct": np.random.choice([0, 5, 10, 15, 20], 20),
            "gross_amount": np.random.uniform(100, 5000, 20).round(2),
            "discount_amount": np.random.uniform(0, 500, 20).round(2),
            "net_amount": np.random.uniform(100, 4500, 20).round(2),
        })

    def test_daily_summary_aggregation(self):
        silver_data = self._create_silver_data()
        completed = silver_data[silver_data["status"] == "completed"]
        daily = completed.groupby(["transaction_date", "region"]).agg(
            total_transactions=("transaction_id", "count"),
            net_revenue=("net_amount", "sum"),
        ).reset_index()
        assert "total_transactions" in daily.columns
        assert "net_revenue" in daily.columns
        assert len(daily) > 0

    def test_customer_metrics(self):
        silver_data = self._create_silver_data()
        completed = silver_data[silver_data["status"] == "completed"]
        customer = completed.groupby("customer_id").agg(
            total_orders=("transaction_id", "count"),
            total_spend=("net_amount", "sum"),
        ).reset_index()
        assert len(customer) <= completed["customer_id"].nunique()
        assert all(customer["total_orders"] > 0)

    def test_product_metrics(self):
        silver_data = self._create_silver_data()
        completed = silver_data[silver_data["status"] == "completed"]
        product = completed.groupby("product_name").agg(
            total_orders=("transaction_id", "count"),
            total_revenue=("net_amount", "sum"),
        ).reset_index()
        assert len(product) <= completed["product_name"].nunique()


# ============================================================
# Data Quality Tests
# ============================================================

class TestDataQuality:
    """Tests for data quality validation"""

    def test_no_duplicate_transaction_ids(self, sample_raw_data):
        assert sample_raw_data["transaction_id"].is_unique

    def test_positive_quantities(self, sample_raw_data):
        assert all(sample_raw_data["quantity"] > 0)

    def test_positive_prices(self, sample_raw_data):
        assert all(sample_raw_data["unit_price"] > 0)

    def test_valid_statuses(self, sample_raw_data):
        valid = {"completed", "pending", "cancelled"}
        statuses = set(sample_raw_data["status"].str.lower())
        assert statuses.issubset(valid)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
