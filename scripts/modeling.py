"""
Subrogation Prediction Model (Ensemble)

This script loads data from a PostgreSQL database, performs extensive feature
engineering, runs a CatBoost-specific hyperparameter optimization, and trains
an F1-weighted ensemble of LightGBM, XGBoost, and CatBoost models to predict
subrogation.
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.preprocessing import LabelEncoder
import lightgbm as lgb
import xgboost as xgb
from catboost import CatBoostClassifier
from imblearn.over_sampling import SMOTE
import optuna
import os
from sqlalchemy import create_engine, text

# Load Data


def load_data():
    """
    Loads data from PostgreSQL database tables in 'stg' schema and merges them.
    """
    print("Connecting to database...")
    # 1. Read DB connection info from environment variables
    PGHOST = os.getenv("PGHOST")
    PGPORT = os.getenv("PGPORT", "5432")
    PGDB = os.getenv("PGDATABASE")
    PGUSER = os.getenv("PGUSER")
    PGPASS = os.getenv("PGPASSWORD")
    PGSSL = os.getenv("PGSSLMODE", "require")

    # 2. Create SQLAlchemy engine for Postgres
    engine_url = f"postgresql+psycopg2://{PGUSER}:{PGPASS}@{PGHOST}:{PGPORT}/{PGDB}"
    if PGSSL:
        engine_url += f"?sslmode={PGSSL}"
    engine = create_engine(engine_url)

    # 3. Test connection and fetch table names from schema `stg`
    with engine.begin() as con:
        print("Connection successful.")
        print("search_path =", con.execute(text("SHOW search_path")).scalar())
        print(
            "who/where   =",
            con.execute(text("SELECT current_database(), current_user")).fetchone(),
        )

        # Get all table names under schema `stg`
        table_names = (
            con.execute(
                text(
                    """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'stg'
            ORDER BY table_name
            """
                )
            )
            .scalars()
            .all()
        )

    print("\nTables in schema stg:", table_names)

    # 4. Load each table into a pandas DataFrame
    dfs = {}  # dict: {table_name: DataFrame}

    for t in table_names:
        print(f"Loading table stg.{t} ...")
        # Use double quotes around table name in case of capitals/special chars
        query = f'SELECT * FROM stg."{t}"'
        dfs[t] = pd.read_sql(query, engine)

    print("\nDone. Loaded tables:", list(dfs.keys()))

    # 5. Access individual tables
    accident_df = dfs["accident"]
    claim_df = dfs["claim"]
    driver_df = dfs["driver"]
    policyholder_df = dfs["policyholder"]
    vehicle_df = dfs["vehicle"]

    # Merge Data
    print("\nMerging dataframes...")
    for col in ["accident_key", "policyholder_key", "vehicle_key", "driver_key"]:
        claim_df[col] = claim_df[col].fillna(0).astype(int)
        claim_df[col] = claim_df[col].astype("int64")

    df = (
        claim_df.merge(accident_df, on="accident_key", how="left")
        .merge(policyholder_df, on="policyholder_key", how="left")
        .merge(vehicle_df, on="vehicle_key", how="left")
        .merge(driver_df, on="driver_key", how="left")
    )

    print("Data merging complete.")
    df.info()
    return df


# Feature Engineering Function
def create_enhanced_features_v2(df, artifacts=None):
    is_training = artifacts is None
    if is_training:
        artifacts = {}

    df = df.copy()

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
    if is_training:
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
        if is_training:
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

    if is_training:
        return df, artifacts
    else:
        return df


# Target Encoding Function
def target_encode(X_train, y_train, X_val, X_test, cols, smoothing=30):
    global_mean = y_train.mean()
    te_names = []

    for col in cols:
        agg = (
            pd.DataFrame({col: X_train[col], "y": y_train})
            .groupby(col)["y"]
            .agg(["sum", "count"])
        )
        agg["mean"] = (agg["sum"] + smoothing * global_mean) / (
            agg["count"] + smoothing
        )
        m = agg["mean"].to_dict()
        te_col = f"{col}_te"
        X_train[te_col] = X_train[col].map(m).fillna(global_mean)
        X_val[te_col] = X_val[col].map(m).fillna(global_mean)
        X_test[te_col] = X_test[col].map(m).fillna(global_mean)
        te_names.append(te_col)

    return te_names


# Selected Features
# Use SHAP-selected features from a previous run
SELECTED_FEATURES = [
    "is_single_car",
    "liab_x_witness",
    "liab_inverse",
    "liab_x_highway",
    "has_witness",
    "high_education",
    "address_change",
    "is_parking",
    "liab_40_plus",
    "is_multi_clear",
    "liab_prct",
    "liab_20_30",
    "liab_squared",
    "liab_cubed",
    "liab_sqrt",
    "liab_inverse_sq",
    "liab_log",
    "liab_zero",
    "liab_full",
    "liab_half",
    "liab_0_10",
    "liab_10_20",
    "liab_30_40",
    "liab_x_police",
    "liab_x_evidence_count",
    "liab_inverse_x_evidence",
    "liab_20_30_x_multi_unclear",
    "liab_20_30_x_single",
    "low_liab_x_multi",
    "high_liab_x_single",
    "liab_x_intersection",
    "liab_x_weekend",
    "liab_x_rush_hour",
    "liab_x_night",
    "liab_x_young_driver",
    "liab_x_new_driver",
    "liab_inverse_x_experienced",
    "liab_x_past_claims",
    "liab_inverse_x_no_claims",
    "liab_x_payout_ratio",
    "liab_inverse_x_high_income",
    "liab_20_30_x_multi_x_evidence",
    "low_liab_x_single_x_no_evidence",
    "high_liab_x_weekend_x_night",
    "golden_combo",
    "has_police",
    "evidence_count",
    "has_full_evidence",
    "has_no_evidence",
    "in_network",
    "safety_rating",
    "safety_high",
    "safety_low",
    "is_multi_unclear",
    "is_highway",
    "is_intersection",
    "age_at_claim",
    "period_of_driving",
    "is_young_driver",
    "is_senior_driver",
    "is_mid_age_driver",
    "is_new_driver",
    "is_experienced",
    "past_num_of_claims",
    "claims_per_year",
    "has_past_claims",
    "has_multiple_claims",
    "vehicle_mileage",
    "mileage_per_year",
    "mileage_log",
    "is_high_mileage",
    "annual_income_capped",
    "annual_income_log",
    "vehicle_price_capped",
    "vehicle_price_log",
    "vehicle_weight_capped",
    "vehicle_weight_log",
    "claim_est_payout_capped",
    "claim_est_payout_log",
    "payout_to_income",
    "payout_to_price",
    "income_to_price",
    "is_high_income",
    "is_expensive_car",
    "is_large_payout",
    "is_weekend",
    "is_weekday",
    "is_morning",
    "is_afternoon",
    "is_evening",
    "is_night",
    "is_rush_hour",
    "is_winter",
    "is_summer",
]


# CatBoost-Specific HPO: Optuna HPO Function
def optimize_catboost_hyperparameters(
    X, y, selected_features, n_trials=100, n_splits=5
):
    print(f"Running {n_trials} trials for CatBoost optimization...")

    cat_features = ["gender", "vehicle_category", "channel"]
    te_features = ["accident_type", "accident_site", "zip3", "accident_combo"]

    cat_features = [f for f in cat_features if f in X.columns]
    te_features = [f for f in te_features if f in X.columns]

    def objective(trial):
        params = {
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.05, log=True),
            "depth": trial.suggest_int("depth", 3, 8),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 10.0),
            "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 1.0),
            "random_strength": trial.suggest_float("random_strength", 0.0, 1.0),
            "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 10, 50),
        }

        skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
        f1_scores = []

        for train_idx, val_idx in skf.split(X, y):
            X_tr = X.iloc[train_idx].copy()
            X_va = X.iloc[val_idx].copy()
            y_tr = y.iloc[train_idx]
            y_va = y.iloc[val_idx]

            X_te_dummy = X_va.copy()  # Create dummy test for target encoding

            te_names = target_encode(
                X_tr, y_tr, X_va, X_te_dummy, te_features, smoothing=30
            )
            features = list(set(selected_features + cat_features + te_names))
            features = [f for f in features if f in X_tr.columns]

            # Label Encode any remaining object types
            for col in features:
                if X_tr[col].dtype == "object":
                    le = LabelEncoder()
                    all_vals = pd.concat([X_tr[col], X_va[col]]).unique()
                    le.fit(all_vals)
                    X_tr[col] = le.transform(X_tr[col])
                    X_va[col] = le.transform(X_va[col])

            smote = SMOTE(sampling_strategy=0.5, random_state=42)
            X_tr_res, y_tr_res = smote.fit_resample(X_tr[features], y_tr)

            model = CatBoostClassifier(
                iterations=1000, random_state=42, verbose=0, **params
            )
            model.fit(
                X_tr_res,
                y_tr_res,
                eval_set=(X_va[features], y_va),
                early_stopping_rounds=100,
                verbose=False,
            )

            probs = model.predict_proba(X_va[features])[:, 1]

            # Find best F1 threshold
            best_f1 = 0
            for thr in np.linspace(0.2, 0.4, 21):
                preds = (probs >= thr).astype(int)
                f1 = f1_score(y_va, preds)
                if f1 > best_f1:
                    best_f1 = f1

            f1_scores.append(best_f1)

        return np.mean(f1_scores)

    study = optuna.create_study(
        direction="maximize", study_name="catboost_optimization"
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

    print(f"\nBest F1: {study.best_value:.5f}")
    print("Best CatBoost params:")
    for k, v in study.best_params.items():
        print(f"  {k}: {v}")

    return study.best_params


# F1-Weighted Ensemble
def train_weighted_ensemble(
    X, y, X_test, selected_features, lgbm_params, catboost_params, n_splits=5
):
    """
    Train LGBM + XGB + CatBoost and combine with weighted average
    Weights based on individual OOF F1 scores
    """
    print("Starting weighted ensemble training...")

    cat_features = ["gender", "vehicle_category", "channel"]
    te_features = ["accident_type", "accident_site", "zip3", "accident_combo"]

    cat_features = [f for f in cat_features if f in X.columns]
    te_features = [f for f in te_features if f in X.columns]

    skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)

    oof_lgbm = np.zeros(len(X))
    oof_xgb = np.zeros(len(X))
    oof_cat = np.zeros(len(X))

    test_lgbm = np.zeros(len(X_test))
    test_xgb = np.zeros(len(X_test))
    test_cat = np.zeros(len(X_test))

    for fold, (train_idx, val_idx) in enumerate(skf.split(X, y), 1):
        print(f"--- Fold {fold}/{n_splits} ---")

        X_tr = X.iloc[train_idx].copy()
        X_va = X.iloc[val_idx].copy()
        X_te = X_test.copy()
        y_tr = y.iloc[train_idx]
        y_va = y.iloc[val_idx]

        te_names = target_encode(X_tr, y_tr, X_va, X_te, te_features, smoothing=30)
        features = list(set(selected_features + cat_features + te_names))
        features = [f for f in features if f in X_tr.columns]

        # Label Encode any remaining object types
        for col in features:
            if X_tr[col].dtype == "object":
                le = LabelEncoder()
                all_vals = pd.concat([X_tr[col], X_va[col], X_te[col]]).unique()
                le.fit(all_vals)
                X_tr[col] = le.transform(X_tr[col])
                X_va[col] = le.transform(X_va[col])
                X_te[col] = le.transform(X_te[col])

        smote = SMOTE(sampling_strategy=0.5, random_state=42 + fold)
        X_tr_res, y_tr_res = smote.fit_resample(X_tr[features], y_tr)

        # Model 1: LightGBM
        model_lgbm = lgb.LGBMClassifier(
            n_estimators=2000,
            random_state=42 + fold,
            n_jobs=-1,
            verbose=-1,
            **lgbm_params,
        )
        model_lgbm.fit(
            X_tr_res,
            y_tr_res,
            eval_set=[(X_va[features], y_va)],
            callbacks=[lgb.early_stopping(150, verbose=False)],
        )
        oof_lgbm[val_idx] = model_lgbm.predict_proba(X_va[features])[:, 1]
        test_lgbm += model_lgbm.predict_proba(X_te[features])[:, 1] / n_splits

        # Model 2: XGBoost
        model_xgb = xgb.XGBClassifier(
            n_estimators=2000,
            learning_rate=lgbm_params["learning_rate"],
            max_depth=lgbm_params["max_depth"],
            subsample=lgbm_params["subsample"],
            colsample_bytree=lgbm_params["colsample_bytree"],
            reg_alpha=lgbm_params["reg_alpha"],
            reg_lambda=lgbm_params["reg_lambda"],
            random_state=42 + fold,
            n_jobs=-1,
            eval_metric="logloss",
        )
        model_xgb.fit(
            X_tr_res, y_tr_res, eval_set=[(X_va[features], y_va)], verbose=False
        )
        oof_xgb[val_idx] = model_xgb.predict_proba(X_va[features])[:, 1]
        test_xgb += model_xgb.predict_proba(X_te[features])[:, 1] / n_splits

        # Model 3: CatBoost (with optimized params)
        model_cat = CatBoostClassifier(
            iterations=2000, random_state=42 + fold, verbose=0, **catboost_params
        )
        model_cat.fit(
            X_tr_res,
            y_tr_res,
            eval_set=(X_va[features], y_va),
            early_stopping_rounds=150,
            verbose=False,
        )
        oof_cat[val_idx] = model_cat.predict_proba(X_va[features])[:, 1]
        test_cat += model_cat.predict_proba(X_te[features])[:, 1] / n_splits

    # Calculate individual OOF F1 scores
    f1_lgbm = max(
        [f1_score(y, (oof_lgbm >= t).astype(int)) for t in np.linspace(0.2, 0.4, 41)]
    )
    f1_xgb = max(
        [f1_score(y, (oof_xgb >= t).astype(int)) for t in np.linspace(0.2, 0.4, 41)]
    )
    f1_cat = max(
        [f1_score(y, (oof_cat >= t).astype(int)) for t in np.linspace(0.2, 0.4, 41)]
    )

    print("\nIndividual model F1 scores:")
    print(f"  LightGBM : {f1_lgbm:.5f}")
    print(f"  XGBoost  : {f1_xgb:.5f}")
    print(f"  CatBoost : {f1_cat:.5f}")

    # Calculate weights based on F1 scores
    total_f1 = f1_lgbm + f1_xgb + f1_cat
    w_lgbm = f1_lgbm / total_f1
    w_xgb = f1_xgb / total_f1
    w_cat = f1_cat / total_f1

    print("\nWeights (based on F1):")
    print(f"  LightGBM : {w_lgbm:.3f}")
    print(f"  XGBoost  : {w_xgb:.3f}")
    print(f"  CatBoost : {w_cat:.3f}")

    # Weighted average
    oof_weighted = w_lgbm * oof_lgbm + w_xgb * oof_xgb + w_cat * oof_cat
    test_weighted = w_lgbm * test_lgbm + w_xgb * test_xgb + w_cat * test_cat

    # Find optimal threshold
    best_f1 = 0
    best_thr = 0.3
    for thr in np.linspace(0.2, 0.4, 41):
        preds = (oof_weighted >= thr).astype(int)
        f1 = f1_score(y, preds)
        if f1 > best_f1:
            best_f1 = f1
            best_thr = thr

    print(f"\nWeighted Ensemble OOF F1: {best_f1:.5f} (threshold: {best_thr:.3f})")
    print(f"Weighted Ensemble OOF AUC: {roc_auc_score(y, oof_weighted):.4f}")

    # Also try simple average for comparison
    oof_simple = (oof_lgbm + oof_xgb + oof_cat) / 3
    f1_simple = max(
        [f1_score(y, (oof_simple >= t).astype(int)) for t in np.linspace(0.2, 0.4, 41)]
    )
    print(f"Simple Average F1: {f1_simple:.5f} (for comparison)")

    return oof_weighted, test_weighted, best_thr, (w_lgbm, w_xgb, w_cat)


# Main Execution Pipeline
def main():
    """
    Main execution function for the modeling pipeline.
    """

    # Load and merge all data from DB
    df = load_data()

    # Split train and test
    df["claim_date"] = pd.to_datetime(df["claim_date"])

    test_df = df[
        (df["claim_date"].dt.year == 2016) & (df["claim_date"].dt.month == 9)
    ].copy()

    train_df = df[
        ~((df["claim_date"].dt.year == 2016) & (df["claim_date"].dt.month == 9))
    ].copy()

    # Clean target variable
    train_df = train_df.dropna(subset=["subrogation"])
    y = train_df["subrogation"].astype(int)

    # Separate features and IDs
    X_raw = train_df.drop(columns=["subrogation", "claim_number"])
    X_test_raw = test_df.drop(columns=["subrogation", "claim_number"])
    test_ids = test_df["claim_number"]

    print(
        f"\nData Split: Train={len(y)}, Test={len(test_ids)}, Positive Rate={y.mean():.4f}"
    )

    # Run Feature Engineering
    print("\nCreating enhanced features...")
    X, artifacts = create_enhanced_features_v2(X_raw)
    X_test = create_enhanced_features_v2(X_test_raw, artifacts=artifacts)
    print(f"Features created: {X.shape[1]}")
    print(f"Using SHAP-selected features: {len(SELECTED_FEATURES)}")

    # LGBM params from previous optimization
    lgbm_params = {
        "learning_rate": 0.0227,
        "num_leaves": 148,
        "max_depth": 3,
        "min_child_samples": 32,
        "subsample": 0.7545,
        "colsample_bytree": 0.5992,
        "reg_alpha": 4.786,
        "reg_lambda": 3.818,
    }

    # Run HPO
    print("\n" + "=" * 80)
    print("RUNNING: Optimize CatBoost Hyperparameters")
    print("=" * 80)

    # Note: n_trials=20 for a quick run, increase for production
    catboost_params = optimize_catboost_hyperparameters(
        X, y, SELECTED_FEATURES, n_trials=20, n_splits=5
    )

    print("\nCatBoost optimization complete.")

    # Run Ensemble Training
    print("\n" + "=" * 80)
    print("RUNNING: Train F1-Weighted Ensemble")
    print("=" * 80)

    oof_weighted, test_weighted, threshold, weights = train_weighted_ensemble(
        X, y, X_test, SELECTED_FEATURES, lgbm_params, catboost_params, n_splits=5
    )

    print("\nEnsemble training complete.")

    # Generate Submission and Report
    print("\n" + "=" * 80)
    print("FINAL RESULTS & SUBMISSION")
    print("=" * 80)

    # Create submission file
    preds_weighted = (test_weighted >= threshold).astype(int)
    submission_proba = pd.DataFrame(
        {"claim_number": test_ids, "subrogation_proba": test_weighted}
    )

    # Adjusted save path for a typical project structure
    save_path = "../data/submission.csv"
    submission_proba.to_csv(save_path, index=False)

    # Final reporting
    oof_f1 = f1_score(y, (oof_weighted >= threshold).astype(int))

    print(f"\nWeighted Ensemble F1: {oof_f1:.5f}")
    print(f"  Test positive rate: {preds_weighted.mean():.4f}")
    print(
        f"  Model weights: LGBM={weights[0]:.3f}, XGB={weights[1]:.3f}, CatBoost={weights[2]:.3f}"
    )
    print(f"  Saved: {save_path}")

    print("\n" + "=" * 80)
    print("Pipeline Finished.")
    print("=" * 80)


if __name__ == "__main__":
    main()
