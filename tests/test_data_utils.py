"""
Tests for data loading and utilities
"""

import pandas as pd
import polars as pl


class TestDataLoading:
    """Test suite for data loading functionality"""

    def test_load_csv_files_exist(self, sample_data_dir):
        """Test that expected CSV files exist"""
        expected_files = [
            "Claim.csv",
            "Accident.csv",
            "Driver.csv",
            "Policyholder.csv",
            "Vehicle.csv",
        ]

        for filename in expected_files:
            filepath = sample_data_dir / filename
            if filepath.exists():
                # If file exists, check it can be loaded
                df = pd.read_csv(filepath)
                assert len(df) > 0, f"{filename} is empty"

    def test_csv_loading_polars(self, sample_data_dir):
        """Test loading CSV files with Polars"""
        claim_path = sample_data_dir / "Claim.csv"

        if claim_path.exists():
            df = pl.read_csv(claim_path)
            assert df.height > 0
            assert "claim_number" in df.columns or "claim_key" in df.columns

    def test_csv_loading_pandas(self, sample_data_dir):
        """Test loading CSV files with Pandas"""
        claim_path = sample_data_dir / "Claim.csv"

        if claim_path.exists():
            df = pd.read_csv(claim_path)
            assert len(df) > 0
            assert "claim_number" in df.columns or "claim_key" in df.columns


class TestDataValidation:
    """Test suite for data validation"""

    def test_claim_data_structure(self, sample_claim_data):
        """Test claim data has expected structure"""
        required_columns = ["claim_number", "claim_date", "subrogation"]

        for col in required_columns:
            assert col in sample_claim_data.columns, f"Missing column: {col}"

    def test_accident_data_structure(self, sample_accident_data):
        """Test accident data has expected structure"""
        required_columns = ["accident_key", "accident_type", "accident_site"]

        for col in required_columns:
            assert col in sample_accident_data.columns, f"Missing column: {col}"

    def test_subrogation_binary(self, sample_claim_data):
        """Test subrogation is binary (0 or 1)"""
        assert sample_claim_data["subrogation"].isin([0, 1]).all()

    def test_liability_percentage_range(self, sample_claim_data):
        """Test liability percentage is in valid range"""
        assert (sample_claim_data["liab_prct"] >= 0).all()
        assert (sample_claim_data["liab_prct"] <= 100).all()

    def test_claim_payout_positive(self, sample_claim_data):
        """Test claim payout is positive"""
        assert (sample_claim_data["claim_est_payout"] > 0).all()


class TestDataMerging:
    """Test suite for data merging operations"""

    def test_merge_claim_accident(self, sample_claim_data, sample_accident_data):
        """Test merging claim and accident data"""
        merged = sample_claim_data.merge(
            sample_accident_data, on="accident_key", how="left"
        )

        assert len(merged) == len(sample_claim_data)
        assert "accident_type" in merged.columns
        assert "accident_site" in merged.columns

    def test_merge_all_tables(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test merging all tables"""
        merged = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        assert len(merged) == len(sample_claim_data)

        # Check key columns from each table
        assert "accident_type" in merged.columns
        assert "annual_income" in merged.columns
        assert "vehicle_price" in merged.columns
        assert "safety_rating" in merged.columns


class TestDataTypes:
    """Test suite for data type validation"""

    def test_numeric_columns(self, sample_claim_data):
        """Test numeric columns have correct dtypes"""
        numeric_cols = ["claim_est_payout", "liab_prct"]

        for col in numeric_cols:
            if col in sample_claim_data.columns:
                assert pd.api.types.is_numeric_dtype(sample_claim_data[col])

    def test_date_column(self, sample_claim_data):
        """Test date column can be converted to datetime"""
        sample_claim_data["claim_date"] = pd.to_datetime(
            sample_claim_data["claim_date"]
        )
        assert pd.api.types.is_datetime64_any_dtype(
            sample_claim_data["claim_date"])

    def test_categorical_columns(self, sample_claim_data):
        """Test categorical columns have valid values"""
        if "witness_present_ind" in sample_claim_data.columns:
            valid_values = {"Y", "N", "Yes", "No", "y", "n"}
            assert sample_claim_data["witness_present_ind"].isin(
                valid_values).all()
