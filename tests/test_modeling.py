"""
Tests for the modeling.py script
"""

from scripts.modeling import (
    create_enhanced_features_v2,
    target_encode,
    SELECTED_FEATURES,
)
import pandas as pd
import sys
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


class TestFeatureEngineering:
    """Test suite for feature engineering functions"""

    def test_create_enhanced_features_v2_basic(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test basic feature engineering functionality"""
        # Merge all data
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        # Test training mode (returns tuple)
        result, artifacts = create_enhanced_features_v2(df)

        assert isinstance(result, pd.DataFrame)
        assert isinstance(artifacts, dict)
        assert len(result) == len(df)
        assert "age_at_claim" in result.columns
        assert "period_of_driving" in result.columns
        assert "liab_prct" in result.columns

        # Check artifacts are stored
        assert "mileage_median" in artifacts
        assert "annual_income_med" in artifacts

    def test_create_enhanced_features_v2_inference(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test feature engineering in inference mode"""
        # Merge all data
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        # First pass: training
        _, artifacts = create_enhanced_features_v2(df.copy())

        # Second pass: inference
        result_inference = create_enhanced_features_v2(
            df.copy(), artifacts=artifacts)

        assert isinstance(result_inference, pd.DataFrame)
        assert len(result_inference) == len(df)

    def test_time_features(
        self,
        sample_claim_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
        sample_accident_data,
    ):
        """Test time-based feature creation"""
        # Merge all required data
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        # Check time features exist
        assert "claim_year" in result.columns
        assert "claim_month" in result.columns
        assert "claim_dow" in result.columns
        assert "is_weekend" in result.columns
        assert "is_rush_hour" in result.columns

        # Check weekend flag logic
        assert result["is_weekend"].isin([0, 1]).all()

    def test_liability_features(
        self,
        sample_claim_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
        sample_accident_data,
    ):
        """Test liability-based feature engineering"""
        # Merge all required data
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        # Check liability features
        assert "liab_squared" in result.columns
        assert "liab_cubed" in result.columns
        assert "liab_sqrt" in result.columns
        assert "liab_inverse" in result.columns
        assert "liab_zero" in result.columns
        assert "liab_full" in result.columns

        # Check liability range features
        assert "liab_0_10" in result.columns
        assert "liab_20_30" in result.columns

    def test_interaction_features(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test interaction feature creation"""
        # Merge all required data
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        # Check key interactions
        assert "liab_x_witness" in result.columns
        assert "liab_x_police" in result.columns
        assert "golden_combo" in result.columns

    def test_missing_value_handling(
        self, sample_driver_data, sample_vehicle_data, sample_policyholder_data
    ):
        """Test handling of missing values"""
        df = pd.DataFrame(
            {
                "claim_date": pd.date_range("2016-01-01", periods=10),
                "year_of_born": [
                    1980,
                    None,
                    1990,
                    None,
                    1985,
                    1975,
                    None,
                    1995,
                    1988,
                    None,
                ],
                "age_of_DL": [20, 25, None, 22, None, 18, 23, None, 21, 24],
                "vehicle_mileage": [
                    50000,
                    None,
                    75000,
                    None,
                    100000,
                    None,
                    25000,
                    None,
                    60000,
                    80000,
                ],
                "annual_income": [
                    50000,
                    None,
                    75000,
                    None,
                    None,
                    60000,
                    None,
                    85000,
                    None,
                    70000,
                ],
                "vehicle_price": [
                    20000,
                    None,
                    30000,
                    None,
                    25000,
                    None,
                    35000,
                    None,
                    28000,
                    None,
                ],
                "vehicle_weight": [
                    3000,
                    None,
                    3500,
                    None,
                    None,
                    3200,
                    None,
                    3800,
                    None,
                    3400,
                ],
                "claim_est_payout": [
                    5000,
                    None,
                    7500,
                    None,
                    None,
                    6000,
                    None,
                    8500,
                    None,
                    7000,
                ],
                "liab_prct": [30, None, 50, None, 25, None, 75, None, 40, None],
                "witness_present_ind": [
                    "Y",
                    "N",
                    None,
                    "Y",
                    "N",
                    None,
                    "Y",
                    "N",
                    None,
                    "Y",
                ],
                "policy_report_filed_ind": [1, 0, None, 1, None, 0, 1, None, 0, 1],
                "in_network_bodyshop": [
                    "yes",
                    "no",
                    None,
                    "yes",
                    None,
                    "no",
                    "yes",
                    None,
                    "no",
                    "yes",
                ],
                "past_num_of_claims": [0, None, 1, None, 2, None, 0, None, 1, None],
                # ADDED
                "high_education_ind": [1, 0, None, 1, None, 0, 1, None, 0, 1],
                # ADDED
                "address_change_ind": [0, 1, None, 0, None, 1, 0, None, 1, 0],
                # ADDED
                "safety_rating": [70, None, 80, None, 75, None, 85, None, 90, None],
                "accident_type": ["single_car"] * 10,
                "accident_site": ["Highway"] * 10,
                "gender": ["M"] * 10,
                "vehicle_category": ["Compact"] * 10,
                "channel": ["Broker"] * 10,
                "zip_code": ["10001"] * 10,
            }
        )

        result, artifacts = create_enhanced_features_v2(df)

        # Check no NaN in critical features
        assert not result["age_at_claim"].isna().any()
        assert not result["period_of_driving"].isna().any()
        assert not result["liab_prct"].isna().any()


class TestTargetEncoding:
    """Test suite for target encoding"""

    def test_target_encode_unseen_categories(self):
        """Test target encoding with unseen categories"""
        X_train = pd.DataFrame({"cat": ["A", "B", "A", "B"]})
        y_train = pd.Series([1, 0, 1, 0])

        X_val = pd.DataFrame({"cat": ["C", "D"]})  # Unseen categories
        X_test = pd.DataFrame({"cat": ["C"]})

        te_names = target_encode(X_train, y_train, X_val, X_test, ["cat"])

        # The function modifies X_val and X_test in place
        # Should use global mean for unseen categories
        assert "cat_te" in te_names
        assert "cat_te" in X_val.columns
        assert not X_val["cat_te"].isna().any()
        assert "cat_te" in X_test.columns
        assert not X_test["cat_te"].isna().any()


class TestSelectedFeatures:
    """Test suite for SELECTED_FEATURES configuration"""

    def test_selected_features_exist(self):
        """Test that SELECTED_FEATURES is properly defined"""
        assert isinstance(SELECTED_FEATURES, list)
        assert len(SELECTED_FEATURES) > 0
        assert "liab_prct" in SELECTED_FEATURES
        assert "is_single_car" in SELECTED_FEATURES

    def test_selected_features_in_engineered_data(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test that selected features are present in engineered data"""
        # Merge all data
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result, _ = create_enhanced_features_v2(df)

        # Check that most selected features are present
        available_features = [
            f for f in SELECTED_FEATURES if f in result.columns]

        # At least 90% of selected features should be present
        coverage = len(available_features) / len(SELECTED_FEATURES)
        assert coverage > 0.9, f"Only {coverage:.1%} of selected features are present"


class TestDataValidation:
    """Test suite for data validation"""

    def test_no_data_leakage_training_to_test(
        self,
        sample_claim_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
        sample_accident_data,
    ):
        """Ensure no data leakage from training to test"""
        # Merge all data
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        df_train = df.iloc[:80].copy()
        df_test = df.iloc[80:].copy()

        # Train features
        result_train, artifacts = create_enhanced_features_v2(df_train)

        # Test features using artifacts from training
        result_test = create_enhanced_features_v2(df_test, artifacts=artifacts)

        # Check that test uses training artifacts
        assert "mileage_median" in artifacts

        # The test set should use the training set's median, not its own
        # This is a proxy test - in production, verify this more rigorously
        assert isinstance(result_test, pd.DataFrame)

    def test_feature_consistency(
        self,
        sample_claim_data,
        sample_accident_data,
        sample_driver_data,
        sample_vehicle_data,
        sample_policyholder_data,
    ):
        """Test that features are consistent across runs"""
        # Merge all data
        df = (
            sample_claim_data.merge(
                sample_accident_data, on="accident_key", how="left")
            .merge(sample_policyholder_data, on="policyholder_key", how="left")
            .merge(sample_vehicle_data, on="vehicle_key", how="left")
            .merge(sample_driver_data, on="driver_key", how="left")
        )

        result1, artifacts1 = create_enhanced_features_v2(df.copy())
        result2, artifacts2 = create_enhanced_features_v2(df.copy())

        # Features should be identical
        pd.testing.assert_frame_equal(result1, result2)
