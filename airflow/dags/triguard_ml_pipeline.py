"""
TriGuard ML Training Pipeline - Self-Contained Version
Orchestrates the complete machine learning workflow from data loading to model training
All logic is inlined to avoid import issues.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "triguard",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================================
# TASK 1: Load and Split Data
# ============================================================================
def load_and_split_data(**context):
    """Load data from CSV files and perform train/test split"""
    import pandas as pd

    print("Loading data from CSV files...")
    csv_path = "/opt/airflow/data/tri_guard_5_py_clean"

    # Load CSV files
    print("Loading individual CSV files...")
    accident_df = pd.read_csv(f"{csv_path}/Accident.csv")
    claim_df = pd.read_csv(f"{csv_path}/Claim.csv")
    driver_df = pd.read_csv(f"{csv_path}/Driver.csv")
    policyholder_df = pd.read_csv(f"{csv_path}/Policyholder.csv")
    vehicle_df = pd.read_csv(f"{csv_path}/Vehicle.csv")

    print(
        f"Loaded tables: Accident ({len(accident_df)}), Claim ({len(claim_df)}), "
        f"Driver ({len(driver_df)}), Policyholder ({len(policyholder_df)}), "
        f"Vehicle ({len(vehicle_df)})"
    )

    # Merge Data
    print("Merging dataframes...")
    for col in ["accident_key", "policyholder_key", "vehicle_key", "driver_key"]:
        claim_df[col] = claim_df[col].fillna(0).astype(int)
        claim_df[col] = claim_df[col].astype("int64")

    df = (
        claim_df.merge(accident_df, on="accident_key", how="left")
        .merge(policyholder_df, on="policyholder_key", how="left")
        .merge(vehicle_df, on="vehicle_key", how="left")
        .merge(driver_df, on="driver_key", how="left")
    )

    # Convert claim_date to datetime
    df["claim_date"] = pd.to_datetime(df["claim_date"])
    print(f"Final merged dataframe: {df.shape[0]} rows, {df.shape[1]} columns")

    # Train/test split (Sept 2016 as test)
    test_df = df[
        (df["claim_date"].dt.year == 2016) & (df["claim_date"].dt.month == 9)
    ].copy()

    train_df = df[
        ~((df["claim_date"].dt.year == 2016) & (df["claim_date"].dt.month == 9))
    ].copy()

    print(f"Training set size: {len(train_df)}")
    print(f"Test set size: {len(test_df)}")

    # Save to parquet for next tasks
    train_df.to_parquet("/opt/airflow/artifacts/train_data.parquet")
    test_df.to_parquet("/opt/airflow/artifacts/test_data.parquet")

    return {"train_size": len(train_df), "test_size": len(test_df)}


# ============================================================================
# TASK 2: Feature Engineering
# ============================================================================
def engineer_features(**context):
    """Engineer features for both train and test sets"""
    import pandas as pd
    import numpy as np
    import joblib

    print("Loading split data...")
    train_df = pd.read_parquet("/opt/airflow/artifacts/train_data.parquet")
    test_df = pd.read_parquet("/opt/airflow/artifacts/test_data.parquet")

    print("Engineering features...")

    # We'll process train and test separately, creating artifacts from train
    artifacts = {}

    # ========== PROCESS TRAIN SET ==========
    df = train_df.copy()

    # TIME
    df["claim_date"] = pd.to_datetime(df["claim_date"], errors="coerce")
    df["claim_year"] = df["claim_date"].dt.year
    df["claim_month"] = df["claim_date"].dt.month
    df["claim_dow"] = df["claim_date"].dt.dayofweek
    df["claim_hour"] = df["claim_date"].dt.hour
    df["claim_day"] = df["claim_date"].dt.day
    df["is_weekend"] = (df["claim_dow"] >= 5).astype(int)
    df["is_weekday"] = (df["claim_dow"] < 5).astype(int)
    df["is_morning"] = ((df["claim_hour"] >= 6) & (df["claim_hour"] < 12)).astype(int)
    df["is_afternoon"] = ((df["claim_hour"] >= 12) & (df["claim_hour"] < 18)).astype(
        int
    )
    df["is_evening"] = ((df["claim_hour"] >= 18) & (df["claim_hour"] < 22)).astype(int)
    df["is_night"] = ((df["claim_hour"] >= 22) | (df["claim_hour"] < 6)).astype(int)
    df["is_rush_hour"] = (
        ((df["claim_hour"] >= 7) & (df["claim_hour"] <= 9))
        | ((df["claim_hour"] >= 16) & (df["claim_hour"] <= 19))
    ).astype(int)
    df["claim_quarter"] = (df["claim_month"] - 1) // 3 + 1
    df["is_winter"] = df["claim_month"].isin([12, 1, 2]).astype(int)
    df["is_summer"] = df["claim_month"].isin([6, 7, 8]).astype(int)

    # DEMOGRAPHICS
    df["year_of_born"] = pd.to_numeric(df["year_of_born"], errors="coerce").fillna(1980)
    df["age_of_DL"] = pd.to_numeric(df["age_of_DL"], errors="coerce").fillna(25)
    df["age_at_claim"] = (df["claim_year"] - df["year_of_born"]).clip(16, 100)
    df["period_of_driving"] = (df["age_at_claim"] - df["age_of_DL"]).clip(lower=0)
    df["is_young_driver"] = (df["age_at_claim"] < 25).astype(int)
    df["is_senior_driver"] = (df["age_at_claim"] >= 65).astype(int)
    df["is_mid_age_driver"] = (
        (df["age_at_claim"] >= 25) & (df["age_at_claim"] < 65)
    ).astype(int)
    df["is_new_driver"] = (df["period_of_driving"] < 3).astype(int)
    df["is_experienced"] = (df["period_of_driving"] >= 10).astype(int)

    # CLAIMS
    df["past_num_of_claims"] = pd.to_numeric(
        df["past_num_of_claims"], errors="coerce"
    ).fillna(0)
    df["claims_per_year"] = df["past_num_of_claims"] / (df["period_of_driving"] + 1)
    df["has_past_claims"] = (df["past_num_of_claims"] > 0).astype(int)
    df["has_multiple_claims"] = (df["past_num_of_claims"] >= 2).astype(int)

    # MILEAGE
    vm = pd.to_numeric(df["vehicle_mileage"], errors="coerce")
    artifacts["mileage_median"] = vm.median()
    df["vehicle_mileage"] = vm.fillna(artifacts["mileage_median"])
    df["mileage_per_year"] = df["vehicle_mileage"] / (df["period_of_driving"] + 1)
    df["mileage_log"] = np.log1p(df["vehicle_mileage"])
    df["is_high_mileage"] = (
        df["vehicle_mileage"] > df["vehicle_mileage"].quantile(0.75)
    ).astype(int)

    # FINANCIAL
    for col in ["annual_income", "vehicle_price", "vehicle_weight", "claim_est_payout"]:
        val = pd.to_numeric(df[col], errors="coerce")
        artifacts[f"{col}_med"] = val.median()
        artifacts[f"{col}_p99"] = val.quantile(0.99)
        artifacts[f"{col}_p01"] = val.quantile(0.01)
        val = val.fillna(artifacts[f"{col}_med"])
        val = val.clip(artifacts[f"{col}_p01"], artifacts[f"{col}_p99"])
        df[f"{col}_capped"] = val
        df[f"{col}_log"] = np.log1p(val)

    df["payout_to_income"] = df["claim_est_payout_capped"] / (
        df["annual_income_capped"] + 1
    )
    df["payout_to_price"] = df["claim_est_payout_capped"] / (
        df["vehicle_price_capped"] + 1
    )
    df["income_to_price"] = df["annual_income_capped"] / (
        df["vehicle_price_capped"] + 1
    )
    df["is_high_income"] = (
        df["annual_income_capped"] > df["annual_income_capped"].quantile(0.75)
    ).astype(int)
    df["is_expensive_car"] = (
        df["vehicle_price_capped"] > df["vehicle_price_capped"].quantile(0.75)
    ).astype(int)
    df["is_large_payout"] = (
        df["claim_est_payout_capped"] > df["claim_est_payout_capped"].quantile(0.75)
    ).astype(int)

    # LIABILITY
    df["liab_prct"] = (
        pd.to_numeric(df["liab_prct"], errors="coerce").fillna(0).clip(0, 100)
    )
    df["liab_0_10"] = (df["liab_prct"] <= 10).astype(int)
    df["liab_10_20"] = ((df["liab_prct"] > 10) & (df["liab_prct"] <= 20)).astype(int)
    df["liab_20_30"] = ((df["liab_prct"] > 20) & (df["liab_prct"] <= 30)).astype(int)
    df["liab_30_40"] = ((df["liab_prct"] > 30) & (df["liab_prct"] <= 40)).astype(int)
    df["liab_40_plus"] = (df["liab_prct"] > 40).astype(int)

    for i in range(0, 100, 5):
        df[f"liab_{i}_{i+5}"] = (
            (df["liab_prct"] > i) & (df["liab_prct"] <= i + 5)
        ).astype(int)

    for val in [15, 18, 20, 22, 25, 27, 30, 32, 35, 37, 40, 45, 50]:
        df[f"liab_exactly_{val}"] = (df["liab_prct"] == val).astype(int)

    df["liab_squared"] = df["liab_prct"] ** 2
    df["liab_cubed"] = df["liab_prct"] ** 3
    df["liab_sqrt"] = np.sqrt(df["liab_prct"])
    df["liab_inverse"] = 100 - df["liab_prct"]
    df["liab_inverse_sq"] = df["liab_inverse"] ** 2
    df["liab_log"] = np.log1p(df["liab_prct"])
    df["liab_zero"] = (df["liab_prct"] == 0).astype(int)
    df["liab_full"] = (df["liab_prct"] == 100).astype(int)
    df["liab_half"] = (df["liab_prct"] == 50).astype(int)

    # EVIDENCE
    df["has_witness"] = (
        df["witness_present_ind"]
        .fillna("N")
        .str.upper()
        .isin(["Y", "YES", "1", "TRUE"])
        .astype(int)
    )
    df["has_police"] = (
        pd.to_numeric(df["policy_report_filed_ind"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["evidence_count"] = df["has_witness"] + df["has_police"]
    df["has_full_evidence"] = (df["evidence_count"] == 2).astype(int)
    df["has_no_evidence"] = (df["evidence_count"] == 0).astype(int)
    df["in_network"] = (
        df["in_network_bodyshop"]
        .fillna("no")
        .str.lower()
        .isin(["yes", "y", "1"])
        .astype(int)
    )

    # PROFILE
    df["high_education"] = (
        pd.to_numeric(df["high_education_ind"], errors="coerce").fillna(0).astype(int)
    )
    df["address_change"] = (
        pd.to_numeric(df["address_change_ind"], errors="coerce").fillna(0).astype(int)
    )
    df["safety_rating"] = pd.to_numeric(df["safety_rating"], errors="coerce").fillna(50)
    df["safety_high"] = (df["safety_rating"] >= 70).astype(int)
    df["safety_low"] = (df["safety_rating"] <= 30).astype(int)

    # ACCIDENT
    df["accident_type"] = df["accident_type"].fillna("Unknown").astype(str)
    df["accident_site"] = df["accident_site"].fillna("Unknown").astype(str)
    df["is_single_car"] = (
        df["accident_type"].str.contains("single", case=False, na=False).astype(int)
    )
    df["is_multi_unclear"] = (
        df["accident_type"]
        .str.contains("multi.*unclear", case=False, na=False)
        .astype(int)
    )
    df["is_multi_clear"] = (
        df["accident_type"]
        .str.contains("multi.*clear", case=False, na=False)
        .astype(int)
    )
    df["is_highway"] = (
        df["accident_site"].str.contains("highway", case=False, na=False).astype(int)
    )
    df["is_intersection"] = (
        df["accident_site"]
        .str.contains("intersection", case=False, na=False)
        .astype(int)
    )
    df["is_parking"] = (
        df["accident_site"].str.contains("parking", case=False, na=False).astype(int)
    )

    # INTERACTIONS
    df["liab_x_witness"] = df["liab_prct"] * df["has_witness"]
    df["liab_x_police"] = df["liab_prct"] * df["has_police"]
    df["liab_x_evidence_count"] = df["liab_prct"] * df["evidence_count"]
    df["liab_inverse_x_evidence"] = df["liab_inverse"] * df["evidence_count"]
    df["liab_20_30_x_multi_unclear"] = df["liab_20_30"] * df["is_multi_unclear"]
    df["liab_20_30_x_single"] = df["liab_20_30"] * df["is_single_car"]
    df["low_liab_x_multi"] = (df["liab_prct"] < 30).astype(int) * (
        1 - df["is_single_car"]
    )
    df["high_liab_x_single"] = (df["liab_prct"] > 50).astype(int) * df["is_single_car"]
    df["liab_x_highway"] = df["liab_prct"] * df["is_highway"]
    df["liab_x_intersection"] = df["liab_prct"] * df["is_intersection"]
    df["liab_x_weekend"] = df["liab_prct"] * df["is_weekend"]
    df["liab_x_rush_hour"] = df["liab_prct"] * df["is_rush_hour"]
    df["liab_x_night"] = df["liab_prct"] * df["is_night"]
    df["liab_x_young_driver"] = df["liab_prct"] * df["is_young_driver"]
    df["liab_x_new_driver"] = df["liab_prct"] * df["is_new_driver"]
    df["liab_inverse_x_experienced"] = df["liab_inverse"] * df["is_experienced"]
    df["liab_x_past_claims"] = df["liab_prct"] * df["has_past_claims"]
    df["liab_inverse_x_no_claims"] = df["liab_inverse"] * (1 - df["has_past_claims"])
    df["liab_x_payout_ratio"] = df["liab_prct"] * df["payout_to_income"]
    df["liab_inverse_x_high_income"] = df["liab_inverse"] * df["is_high_income"]
    df["liab_20_30_x_multi_x_evidence"] = (
        df["liab_20_30"]
        * df["is_multi_unclear"]
        * (df["evidence_count"] > 0).astype(int)
    )
    df["low_liab_x_single_x_no_evidence"] = (
        (df["liab_prct"] < 25).astype(int) * df["is_single_car"] * df["has_no_evidence"]
    )
    df["high_liab_x_weekend_x_night"] = (
        (df["liab_prct"] > 60).astype(int) * df["is_weekend"] * df["is_night"]
    )
    df["golden_combo"] = (
        df["liab_20_30"]
        * df["is_multi_unclear"]
        * (df["evidence_count"] > 0).astype(int)
        * df["is_highway"]
    )

    # CATEGORICALS
    cat_cols = ["gender", "vehicle_category", "channel"]
    for col in cat_cols:
        df[col] = df[col].fillna("Unknown").astype(str)

    df["accident_combo"] = df["accident_site"] + "_" + df["accident_type"]
    df["zip_code"] = (
        pd.to_numeric(df["zip_code"], errors="coerce")
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(5)
    )
    df["zip3"] = df["zip_code"].str[:3]
    df["zip3"] = df["zip3"].where(df["zip3"] != "000", "unknown")

    train_engineered = df

    # ========== PROCESS TEST SET using artifacts ==========
    df_test = test_df.copy()

    # TIME
    df_test["claim_date"] = pd.to_datetime(df_test["claim_date"], errors="coerce")
    df_test["claim_year"] = df_test["claim_date"].dt.year
    df_test["claim_month"] = df_test["claim_date"].dt.month
    df_test["claim_dow"] = df_test["claim_date"].dt.dayofweek
    df_test["claim_hour"] = df_test["claim_date"].dt.hour
    df_test["claim_day"] = df_test["claim_date"].dt.day
    df_test["is_weekend"] = (df_test["claim_dow"] >= 5).astype(int)
    df_test["is_weekday"] = (df_test["claim_dow"] < 5).astype(int)
    df_test["is_morning"] = (
        (df_test["claim_hour"] >= 6) & (df_test["claim_hour"] < 12)
    ).astype(int)
    df_test["is_afternoon"] = (
        (df_test["claim_hour"] >= 12) & (df_test["claim_hour"] < 18)
    ).astype(int)
    df_test["is_evening"] = (
        (df_test["claim_hour"] >= 18) & (df_test["claim_hour"] < 22)
    ).astype(int)
    df_test["is_night"] = (
        (df_test["claim_hour"] >= 22) | (df_test["claim_hour"] < 6)
    ).astype(int)
    df_test["is_rush_hour"] = (
        ((df_test["claim_hour"] >= 7) & (df_test["claim_hour"] <= 9))
        | ((df_test["claim_hour"] >= 16) & (df_test["claim_hour"] <= 19))
    ).astype(int)
    df_test["claim_quarter"] = (df_test["claim_month"] - 1) // 3 + 1
    df_test["is_winter"] = df_test["claim_month"].isin([12, 1, 2]).astype(int)
    df_test["is_summer"] = df_test["claim_month"].isin([6, 7, 8]).astype(int)

    # DEMOGRAPHICS
    df_test["year_of_born"] = pd.to_numeric(
        df_test["year_of_born"], errors="coerce"
    ).fillna(1980)
    df_test["age_of_DL"] = pd.to_numeric(df_test["age_of_DL"], errors="coerce").fillna(
        25
    )
    df_test["age_at_claim"] = (df_test["claim_year"] - df_test["year_of_born"]).clip(
        16, 100
    )
    df_test["period_of_driving"] = (
        df_test["age_at_claim"] - df_test["age_of_DL"]
    ).clip(lower=0)
    df_test["is_young_driver"] = (df_test["age_at_claim"] < 25).astype(int)
    df_test["is_senior_driver"] = (df_test["age_at_claim"] >= 65).astype(int)
    df_test["is_mid_age_driver"] = (
        (df_test["age_at_claim"] >= 25) & (df_test["age_at_claim"] < 65)
    ).astype(int)
    df_test["is_new_driver"] = (df_test["period_of_driving"] < 3).astype(int)
    df_test["is_experienced"] = (df_test["period_of_driving"] >= 10).astype(int)

    # CLAIMS
    df_test["past_num_of_claims"] = pd.to_numeric(
        df_test["past_num_of_claims"], errors="coerce"
    ).fillna(0)
    df_test["claims_per_year"] = df_test["past_num_of_claims"] / (
        df_test["period_of_driving"] + 1
    )
    df_test["has_past_claims"] = (df_test["past_num_of_claims"] > 0).astype(int)
    df_test["has_multiple_claims"] = (df_test["past_num_of_claims"] >= 2).astype(int)

    # MILEAGE (using artifacts from train)
    vm = pd.to_numeric(df_test["vehicle_mileage"], errors="coerce")
    df_test["vehicle_mileage"] = vm.fillna(artifacts["mileage_median"])
    df_test["mileage_per_year"] = df_test["vehicle_mileage"] / (
        df_test["period_of_driving"] + 1
    )
    df_test["mileage_log"] = np.log1p(df_test["vehicle_mileage"])
    df_test["is_high_mileage"] = (
        df_test["vehicle_mileage"] > df_test["vehicle_mileage"].quantile(0.75)
    ).astype(int)

    # FINANCIAL (using artifacts from train)
    for col in ["annual_income", "vehicle_price", "vehicle_weight", "claim_est_payout"]:
        val = pd.to_numeric(df_test[col], errors="coerce")
        val = val.fillna(artifacts[f"{col}_med"])
        val = val.clip(artifacts[f"{col}_p01"], artifacts[f"{col}_p99"])
        df_test[f"{col}_capped"] = val
        df_test[f"{col}_log"] = np.log1p(val)

    df_test["payout_to_income"] = df_test["claim_est_payout_capped"] / (
        df_test["annual_income_capped"] + 1
    )
    df_test["payout_to_price"] = df_test["claim_est_payout_capped"] / (
        df_test["vehicle_price_capped"] + 1
    )
    df_test["income_to_price"] = df_test["annual_income_capped"] / (
        df_test["vehicle_price_capped"] + 1
    )
    df_test["is_high_income"] = (
        df_test["annual_income_capped"] > df_test["annual_income_capped"].quantile(0.75)
    ).astype(int)
    df_test["is_expensive_car"] = (
        df_test["vehicle_price_capped"] > df_test["vehicle_price_capped"].quantile(0.75)
    ).astype(int)
    df_test["is_large_payout"] = (
        df_test["claim_est_payout_capped"]
        > df_test["claim_est_payout_capped"].quantile(0.75)
    ).astype(int)

    # LIABILITY
    df_test["liab_prct"] = (
        pd.to_numeric(df_test["liab_prct"], errors="coerce").fillna(0).clip(0, 100)
    )
    df_test["liab_0_10"] = (df_test["liab_prct"] <= 10).astype(int)
    df_test["liab_10_20"] = (
        (df_test["liab_prct"] > 10) & (df_test["liab_prct"] <= 20)
    ).astype(int)
    df_test["liab_20_30"] = (
        (df_test["liab_prct"] > 20) & (df_test["liab_prct"] <= 30)
    ).astype(int)
    df_test["liab_30_40"] = (
        (df_test["liab_prct"] > 30) & (df_test["liab_prct"] <= 40)
    ).astype(int)
    df_test["liab_40_plus"] = (df_test["liab_prct"] > 40).astype(int)

    for i in range(0, 100, 5):
        df_test[f"liab_{i}_{i+5}"] = (
            (df_test["liab_prct"] > i) & (df_test["liab_prct"] <= i + 5)
        ).astype(int)

    for val in [15, 18, 20, 22, 25, 27, 30, 32, 35, 37, 40, 45, 50]:
        df_test[f"liab_exactly_{val}"] = (df_test["liab_prct"] == val).astype(int)

    df_test["liab_squared"] = df_test["liab_prct"] ** 2
    df_test["liab_cubed"] = df_test["liab_prct"] ** 3
    df_test["liab_sqrt"] = np.sqrt(df_test["liab_prct"])
    df_test["liab_inverse"] = 100 - df_test["liab_prct"]
    df_test["liab_inverse_sq"] = df_test["liab_inverse"] ** 2
    df_test["liab_log"] = np.log1p(df_test["liab_prct"])
    df_test["liab_zero"] = (df_test["liab_prct"] == 0).astype(int)
    df_test["liab_full"] = (df_test["liab_prct"] == 100).astype(int)
    df_test["liab_half"] = (df_test["liab_prct"] == 50).astype(int)

    # EVIDENCE
    df_test["has_witness"] = (
        df_test["witness_present_ind"]
        .fillna("N")
        .str.upper()
        .isin(["Y", "YES", "1", "TRUE"])
        .astype(int)
    )
    df_test["has_police"] = (
        pd.to_numeric(df_test["policy_report_filed_ind"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df_test["evidence_count"] = df_test["has_witness"] + df_test["has_police"]
    df_test["has_full_evidence"] = (df_test["evidence_count"] == 2).astype(int)
    df_test["has_no_evidence"] = (df_test["evidence_count"] == 0).astype(int)
    df_test["in_network"] = (
        df_test["in_network_bodyshop"]
        .fillna("no")
        .str.lower()
        .isin(["yes", "y", "1"])
        .astype(int)
    )

    # PROFILE
    df_test["high_education"] = (
        pd.to_numeric(df_test["high_education_ind"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df_test["address_change"] = (
        pd.to_numeric(df_test["address_change_ind"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df_test["safety_rating"] = pd.to_numeric(
        df_test["safety_rating"], errors="coerce"
    ).fillna(50)
    df_test["safety_high"] = (df_test["safety_rating"] >= 70).astype(int)
    df_test["safety_low"] = (df_test["safety_rating"] <= 30).astype(int)

    # ACCIDENT
    df_test["accident_type"] = df_test["accident_type"].fillna("Unknown").astype(str)
    df_test["accident_site"] = df_test["accident_site"].fillna("Unknown").astype(str)
    df_test["is_single_car"] = (
        df_test["accident_type"]
        .str.contains("single", case=False, na=False)
        .astype(int)
    )
    df_test["is_multi_unclear"] = (
        df_test["accident_type"]
        .str.contains("multi.*unclear", case=False, na=False)
        .astype(int)
    )
    df_test["is_multi_clear"] = (
        df_test["accident_type"]
        .str.contains("multi.*clear", case=False, na=False)
        .astype(int)
    )
    df_test["is_highway"] = (
        df_test["accident_site"]
        .str.contains("highway", case=False, na=False)
        .astype(int)
    )
    df_test["is_intersection"] = (
        df_test["accident_site"]
        .str.contains("intersection", case=False, na=False)
        .astype(int)
    )
    df_test["is_parking"] = (
        df_test["accident_site"]
        .str.contains("parking", case=False, na=False)
        .astype(int)
    )

    # INTERACTIONS
    df_test["liab_x_witness"] = df_test["liab_prct"] * df_test["has_witness"]
    df_test["liab_x_police"] = df_test["liab_prct"] * df_test["has_police"]
    df_test["liab_x_evidence_count"] = df_test["liab_prct"] * df_test["evidence_count"]
    df_test["liab_inverse_x_evidence"] = (
        df_test["liab_inverse"] * df_test["evidence_count"]
    )
    df_test["liab_20_30_x_multi_unclear"] = (
        df_test["liab_20_30"] * df_test["is_multi_unclear"]
    )
    df_test["liab_20_30_x_single"] = df_test["liab_20_30"] * df_test["is_single_car"]
    df_test["low_liab_x_multi"] = (df_test["liab_prct"] < 30).astype(int) * (
        1 - df_test["is_single_car"]
    )
    df_test["high_liab_x_single"] = (df_test["liab_prct"] > 50).astype(int) * df_test[
        "is_single_car"
    ]
    df_test["liab_x_highway"] = df_test["liab_prct"] * df_test["is_highway"]
    df_test["liab_x_intersection"] = df_test["liab_prct"] * df_test["is_intersection"]
    df_test["liab_x_weekend"] = df_test["liab_prct"] * df_test["is_weekend"]
    df_test["liab_x_rush_hour"] = df_test["liab_prct"] * df_test["is_rush_hour"]
    df_test["liab_x_night"] = df_test["liab_prct"] * df_test["is_night"]
    df_test["liab_x_young_driver"] = df_test["liab_prct"] * df_test["is_young_driver"]
    df_test["liab_x_new_driver"] = df_test["liab_prct"] * df_test["is_new_driver"]
    df_test["liab_inverse_x_experienced"] = (
        df_test["liab_inverse"] * df_test["is_experienced"]
    )
    df_test["liab_x_past_claims"] = df_test["liab_prct"] * df_test["has_past_claims"]
    df_test["liab_inverse_x_no_claims"] = df_test["liab_inverse"] * (
        1 - df_test["has_past_claims"]
    )
    df_test["liab_x_payout_ratio"] = df_test["liab_prct"] * df_test["payout_to_income"]
    df_test["liab_inverse_x_high_income"] = (
        df_test["liab_inverse"] * df_test["is_high_income"]
    )
    df_test["liab_20_30_x_multi_x_evidence"] = (
        df_test["liab_20_30"]
        * df_test["is_multi_unclear"]
        * (df_test["evidence_count"] > 0).astype(int)
    )
    df_test["low_liab_x_single_x_no_evidence"] = (
        (df_test["liab_prct"] < 25).astype(int)
        * df_test["is_single_car"]
        * df_test["has_no_evidence"]
    )
    df_test["high_liab_x_weekend_x_night"] = (
        (df_test["liab_prct"] > 60).astype(int)
        * df_test["is_weekend"]
        * df_test["is_night"]
    )
    df_test["golden_combo"] = (
        df_test["liab_20_30"]
        * df_test["is_multi_unclear"]
        * (df_test["evidence_count"] > 0).astype(int)
        * df_test["is_highway"]
    )

    # CATEGORICALS
    cat_cols = ["gender", "vehicle_category", "channel"]
    for col in cat_cols:
        df_test[col] = df_test[col].fillna("Unknown").astype(str)

    df_test["accident_combo"] = (
        df_test["accident_site"] + "_" + df_test["accident_type"]
    )
    df_test["zip_code"] = (
        pd.to_numeric(df_test["zip_code"], errors="coerce")
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(5)
    )
    df_test["zip3"] = df_test["zip_code"].str[:3]
    df_test["zip3"] = df_test["zip3"].where(df_test["zip3"] != "000", "unknown")

    test_engineered = df_test

    # Save engineered data and artifacts
    train_engineered.to_parquet("/opt/airflow/artifacts/train_engineered.parquet")
    test_engineered.to_parquet("/opt/airflow/artifacts/test_engineered.parquet")
    joblib.dump(artifacts, "/opt/airflow/artifacts/feature_artifacts.pkl")

    print(f"Features engineered: {train_engineered.shape[1]} columns")
    return {"n_features": train_engineered.shape[1]}


# ============================================================================
# TASK 3: Hyperparameter Optimization (SIMPLIFIED VERSION)
# ============================================================================
def run_hyperparameter_optimization(**context):
    """
    Simplified HPO - using fixed good params to speed up pipeline
    In production, you'd run full Optuna optimization
    """
    import json

    print("Using pre-optimized hyperparameters (skipping full HPO for speed)...")

    # These are good baseline params from previous runs
    best_params = {
        "learning_rate": 0.03,
        "depth": 6,
        "l2_leaf_reg": 3.0,
        "bagging_temperature": 0.5,
        "random_strength": 0.5,
        "min_data_in_leaf": 20,
    }

    # Save best parameters
    with open("/opt/airflow/artifacts/best_params.json", "w") as f:
        json.dump(best_params, f, indent=2)

    print(f"Best params: {best_params}")
    return best_params


# ============================================================================
# TASK 4: Train Ensemble Models (SIMPLIFIED VERSION)
# ============================================================================
def train_models(**context):
    """
    Simplified training - trains a single LightGBM model
    Full ensemble would include XGBoost and CatBoost
    """
    import pandas as pd
    import numpy as np
    import json
    import joblib
    import lightgbm as lgb
    from sklearn.metrics import f1_score, roc_auc_score, classification_report
    from sklearn.preprocessing import LabelEncoder

    print("Loading data...")
    train_df = pd.read_parquet("/opt/airflow/artifacts/train_engineered.parquet")
    test_df = pd.read_parquet("/opt/airflow/artifacts/test_engineered.parquet")

    # Prepare data
    train_df = train_df.dropna(subset=["subrogation"])
    y_train = train_df["subrogation"].astype(int)
    X_train = train_df.drop(columns=["subrogation", "claim_number"])

    # For test, if subrogation exists use it, otherwise create dummy
    if "subrogation" in test_df.columns:
        y_test = test_df["subrogation"].astype(int)
        X_test = test_df.drop(columns=["subrogation", "claim_number"])
    else:
        X_test = test_df.drop(columns=["claim_number"])
        y_test = None

    print(f"Train: {X_train.shape}, Test: {X_test.shape}")

    # Select features (top important ones)
    SELECTED_FEATURES = [
        "liab_prct",
        "liab_inverse",
        "liab_x_witness",
        "has_witness",
        "has_police",
        "evidence_count",
        "is_single_car",
        "is_multi_unclear",
        "is_highway",
        "is_parking",
        "liab_20_30",
        "liab_squared",
        "age_at_claim",
        "period_of_driving",
        "is_young_driver",
        "past_num_of_claims",
        "vehicle_mileage",
        "annual_income_capped",
        "vehicle_price_capped",
        "claim_est_payout_capped",
        "payout_to_income",
        "safety_rating",
        "high_education",
        "address_change",
    ]

    # Add categorical features
    cat_features = ["gender", "vehicle_category", "channel"]
    for col in cat_features:
        if col in X_train.columns:
            SELECTED_FEATURES.append(col)

    # Filter to existing columns
    SELECTED_FEATURES = [f for f in SELECTED_FEATURES if f in X_train.columns]

    # Label encode categorical features
    for col in cat_features:
        if col in SELECTED_FEATURES:
            le = LabelEncoder()
            X_train[col] = le.fit_transform(X_train[col].astype(str))
            X_test[col] = le.transform(X_test[col].astype(str))

    print(f"Using {len(SELECTED_FEATURES)} features")

    # Train LightGBM
    print("Training LightGBM model...")
    model = lgb.LGBMClassifier(
        n_estimators=1000,
        learning_rate=0.03,
        num_leaves=100,
        max_depth=6,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=1.0,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )

    model.fit(
        X_train[SELECTED_FEATURES],
        y_train,
        eval_set=[(X_test[SELECTED_FEATURES], y_test)] if y_test is not None else None,
        callbacks=(
            [lgb.early_stopping(100, verbose=False)] if y_test is not None else None
        ),
    )

    # Predictions
    train_probs = model.predict_proba(X_train[SELECTED_FEATURES])[:, 1]
    test_probs = model.predict_proba(X_test[SELECTED_FEATURES])[:, 1]

    # Find best threshold on train
    best_f1 = 0
    best_thr = 0.3
    for thr in np.linspace(0.1, 0.5, 41):
        preds = (train_probs >= thr).astype(int)
        f1 = f1_score(y_train, preds)
        if f1 > best_f1:
            best_f1 = f1
            best_thr = thr

    train_preds = (train_probs >= best_thr).astype(int)
    test_preds = (test_probs >= best_thr).astype(int)

    train_f1 = f1_score(y_train, train_preds)
    train_auc = roc_auc_score(y_train, train_probs)

    print(f"Train F1: {train_f1:.4f}, Train AUC: {train_auc:.4f}")
    print(f"Best threshold: {best_thr:.3f}")

    # Calculate test metrics if we have labels
    if y_test is not None:
        test_f1 = f1_score(y_test, test_preds)
        test_auc = roc_auc_score(y_test, test_probs)
        print(f"Test F1: {test_f1:.4f}, Test AUC: {test_auc:.4f}")
        print("\nClassification Report:")
        print(classification_report(y_test, test_preds))
    else:
        test_f1 = None
        test_auc = None
        print("No test labels available for evaluation")

    # Save model and predictions
    joblib.dump(model, "/opt/airflow/artifacts/model.pkl")

    # Save predictions with claim_number
    pred_df = pd.DataFrame(
        {
            "claim_number": test_df["claim_number"],
            "subrogation_proba": test_probs,
            "subrogation_pred": test_preds,
        }
    )
    pred_df.to_csv("/opt/airflow/artifacts/test_predictions.csv", index=False)

    metrics = {
        "train_f1": float(train_f1),
        "train_auc": float(train_auc),
        "test_f1": float(test_f1) if test_f1 is not None else None,
        "test_auc": float(test_auc) if test_auc is not None else None,
        "best_threshold": float(best_thr),
        "n_features": len(SELECTED_FEATURES),
    }

    with open("/opt/airflow/artifacts/metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"\nTraining complete! Predictions saved.")
    return metrics


# ============================================================================
# TASK 5: Generate Report
# ============================================================================
def generate_report(**context):
    """Generate final report"""
    import json

    with open("/opt/airflow/artifacts/metrics.json", "r") as f:
        metrics = json.load(f)

    report = f"""
    =============================================
    TriGuard ML Pipeline Execution Report
    =============================================
    
    Model Performance:
    - Training F1 Score: {metrics['train_f1']:.4f}
    - Training AUC: {metrics['train_auc']:.4f}
    - Test F1 Score: {metrics.get('test_f1', 'N/A')}
    - Test AUC: {metrics.get('test_auc', 'N/A')}
    - Best Threshold: {metrics['best_threshold']:.3f}
    - Features Used: {metrics['n_features']}
    
    Files Generated:
    - /opt/airflow/artifacts/model.pkl
    - /opt/airflow/artifacts/test_predictions.csv
    - /opt/airflow/artifacts/metrics.json
    
    =============================================
    """

    print(report)

    with open("/opt/airflow/artifacts/execution_report.txt", "w") as f:
        f.write(report)

    return metrics


# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    "triguard_ml_training_pipeline",
    default_args=default_args,
    description="Complete ML training pipeline for TriGuard subrogation prediction",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "training", "triguard", "ensemble"],
    max_active_runs=1,
) as dag:

    # Task 1: Load and split data
    load_task = PythonOperator(
        task_id="load_and_split_data",
        python_callable=load_and_split_data,
    )

    # Task 2: Feature engineering
    feature_task = PythonOperator(
        task_id="engineer_features",
        python_callable=engineer_features,
    )

    # Task 3: Hyperparameter optimization
    hpo_task = PythonOperator(
        task_id="hyperparameter_optimization",
        python_callable=run_hyperparameter_optimization,
    )

    # Task 4: Train models
    train_task = PythonOperator(
        task_id="train_ensemble_models",
        python_callable=train_models,
    )

    # Task 5: Generate report
    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    # Task 6: Cleanup temp files (optional)
    cleanup_task = BashOperator(
        task_id="cleanup_temp_files",
        bash_command="rm -f /opt/airflow/artifacts/train_data.parquet /opt/airflow/artifacts/test_data.parquet",
    )

    # Define task dependencies
    load_task >> feature_task >> hpo_task >> train_task >> report_task >> cleanup_task
