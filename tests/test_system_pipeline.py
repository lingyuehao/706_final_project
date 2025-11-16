"""
Integration tests for the entire system pipeline
"""

from scripts.modeling import create_enhanced_features_v2, SELECTED_FEATURES
import pandas as pd
import numpy as np
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


class TestEndToEndPipeline:
    """Test suite for end-to-end pipeline validation"""

    def test_full_feature_pipeline(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test complete feature engineering pipeline"""
        # Merge all tables
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        # Split train/test
        train_df = df.iloc[:80].copy()
        test_df = df.iloc[80:].copy()

        # Engineer features
        X_train, artifacts = create_enhanced_features_v2(train_df)
        X_test = create_enhanced_features_v2(test_df, artifacts=artifacts)

        # Validate outputs
        assert len(X_train) == 80
        assert len(X_test) == 20
        assert X_train.shape[1] == X_test.shape[1]

        # Check for data leakage - test should use training artifacts
        assert "mileage_median" in artifacts

    def test_selected_features_availability(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test that selected features are available after engineering"""
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        # Count available selected features
        available = sum(1 for f in SELECTED_FEATURES if f in result.columns)
        total = len(SELECTED_FEATURES)
        coverage = available / total

        assert coverage > 0.85, f"Only {coverage:.1%} of selected features available"

    def test_data_quality_checks(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test data quality after full pipeline"""
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        # Check critical features have no NaN
        critical_features = [
            "age_at_claim",
            "period_of_driving",
            "liab_prct",
            "is_weekend",
            "is_single_car",
        ]

        for feat in critical_features:
            if feat in result.columns:
                assert not result[feat].isna().any(
                ), f"{feat} contains NaN values"

    def test_feature_dtypes(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test feature data types are appropriate"""
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        # Binary features should be 0/1
        binary_features = [
            "is_weekend",
            "is_weekday",
            "is_young_driver",
            "is_single_car",
            "has_witness",
            "has_police",
        ]

        for feat in binary_features:
            if feat in result.columns:
                unique_vals = result[feat].dropna().unique()
                assert set(unique_vals).issubset(
                    {0, 1}
                ), f"{feat} is not binary: {unique_vals}"


class TestDataConsistency:
    """Test suite for data consistency across pipeline"""

    def test_row_count_preservation(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test that row counts are preserved through pipeline"""
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        original_count = len(df)
        result, _ = create_enhanced_features_v2(df)

        assert len(result) == original_count

    def test_deterministic_output(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test that pipeline produces deterministic output"""
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result1, _ = create_enhanced_features_v2(df.copy())
        result2, _ = create_enhanced_features_v2(df.copy())

        # Results should be identical
        pd.testing.assert_frame_equal(result1, result2)

    def test_feature_value_ranges(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test that feature values are in expected ranges"""
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        # Age at claim should be reasonable
        if "age_at_claim" in result.columns:
            assert result["age_at_claim"].min() >= 16
            assert result["age_at_claim"].max() <= 100

        # Liability percentage should be 0-100
        if "liab_prct" in result.columns:
            assert result["liab_prct"].min() >= 0
            assert result["liab_prct"].max() <= 100


class TestModelingPreparation:
    """Test suite for model input preparation"""

    def test_no_infinity_values(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test that engineered features contain no infinity values"""
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        # Check for infinity in numeric columns
        numeric_cols = result.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            assert not np.isinf(result[col]).any(
            ), f"{col} contains infinity values"

    def test_feature_names_no_special_chars(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test that feature names don't contain problematic characters"""
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        for col in result.columns:
            # Should not contain spaces or special chars that might break models
            assert " " not in col, f"Column name '{col}' contains spaces"
            assert not col.startswith(
                "_"
            ), f"Column name '{col}' starts with underscore"
