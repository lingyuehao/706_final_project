import os

import polars as pl
import statsmodels.api as sm


# Load files
def load_csv(path: str) -> pl.DataFrame:
    return pl.read_csv(path, ignore_errors=True)


# Helper
def to_bool(col: pl.Expr) -> pl.Expr:
    s = col.cast(pl.Utf8, strict=False).str.strip_chars().str.to_lowercase()
    return (
        pl.when(s.is_in(["y", "yes", "true", "1"]))
        .then(pl.lit(1))
        .when(s.is_in(["n", "no", "false", "0"]))
        .then(pl.lit(0))
        .otherwise(None)
    )


# Clean claim
def clean_claim(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        [
            pl.col("subrogation").cast(pl.Float64, strict=False),
            pl.col("claim_est_payout").cast(pl.Float64, strict=False),
            pl.col("liab_prct").cast(pl.Float64, strict=False),
            pl.col("vehicle_key").cast(pl.Int64, strict=False),
            pl.col("channel").cast(pl.Utf8, strict=False),
            pl.col("claim_day_of_week").cast(pl.Utf8, strict=False),
            pl.col("witness_present_ind").cast(pl.Utf8, strict=False),
            pl.col("policy_report_filed_ind").cast(pl.Utf8, strict=False),
        ]
    )

    df = df.with_columns(
        [
            to_bool(pl.col("witness_present_ind")).alias("witness_present"),
            to_bool(pl.col("policy_report_filed_ind")).alias("policy_report_filed"),
        ]
    )
    return df


# Clean vehicle
def clean_vehicle(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col("vehicle_price").cast(pl.Float64, strict=False),
            pl.col("vehicle_weight").cast(pl.Float64, strict=False),
            pl.col("vehicle_mileage").cast(pl.Float64, strict=False),
            pl.col("vehicle_made_year").cast(pl.Int64, strict=False),
            pl.col("vehicle_key").cast(pl.Int64, strict=False),
            pl.col("vehicle_category").cast(pl.Utf8, strict=False),
            pl.col("vehicle_color").cast(pl.Utf8, strict=False),
        ]
    )


# Merge claim and vehicle
def merge_claim_vehicle(claim: pl.DataFrame, vehicle: pl.DataFrame) -> pl.DataFrame:
    return claim.join(vehicle, on="vehicle_key", how="inner")


# Summary claim
def numeric_summary_claim(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        [
            pl.mean("subrogation").alias("avg_subrogation"),
            pl.mean("claim_est_payout").alias("avg_payout"),
            pl.mean("liab_prct").alias("avg_liability_pct"),
        ]
    )


# Summary vehicle
def numeric_summary_vehicle(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        [
            pl.mean("vehicle_price").alias("avg_vehicle_price"),
            pl.mean("vehicle_mileage").alias("avg_mileage"),
            pl.mean("avg_weight"),
        ]
    )


# Categorical summary
def categorical_summary(df: pl.DataFrame, col: str):
    unique_vals = df.select(pl.col(col).unique()).to_series().to_list()
    counts = (
        df.group_by(col).agg(pl.len().alias("count")).sort("count", descending=True)
    )
    return unique_vals, counts


# Correlation analysis
def correlation_analysis(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.corr("subrogation", "vehicle_price").alias("corr_subro_price"),
        pl.corr("subrogation", "vehicle_mileage").alias("corr_subro_mileage"),
        pl.corr("claim_est_payout", "vehicle_price").alias("corr_payout_price"),
        pl.corr("subrogation", "liab_prct").alias("corr_subro_liability"),
    )


# Regression analysis
def run_regression(df: pl.DataFrame):
    df_pd = df.drop_nulls().to_pandas()

    y = df_pd["subrogation"]
    X = df_pd[
        [
            "vehicle_price",
            "claim_est_payout",
            "liab_prct",
            "witness_present",
            "policy_report_filed",
        ]
    ]
    X = sm.add_constant(X)

    model = sm.OLS(y, X).fit()
    return model


def main(data_dir: str):

    # Load
    claim = load_csv(os.path.join(data_dir, "Claim.csv"))
    vehicle = load_csv(os.path.join(data_dir, "Vehicle.csv"))

    print("Claim shape:", claim.shape)
    print("Vehicle shape:", vehicle.shape)

    # Clean
    claim_clean = clean_claim(claim)
    vehicle_clean = clean_vehicle(vehicle)

    # Merge
    merged = merge_claim_vehicle(claim_clean, vehicle_clean)
    print("Merged shape:", merged.shape)

    # Summaries
    print("\nClaim Summary:\n", numeric_summary_claim(claim_clean))
    print("\nVehicle Summary:\n", numeric_summary_vehicle(vehicle_clean))

    # Categorical summaries
    cat_cols_claim = [
        "channel",
        "claim_day_of_week",
        "witness_present_ind",
        "policy_report_filed_ind",
    ]
    cat_cols_vehicle = ["vehicle_category", "vehicle_color"]

    print("\n--- Categorical Variable Check ---")
    for col in cat_cols_claim:
        uniq, counts = categorical_summary(claim_clean, col)
        print(f"\n{col} (Claim):")
        print("Unique:", uniq)
        print(counts)

    for col in cat_cols_vehicle:
        uniq, counts = categorical_summary(vehicle_clean, col)
        print(f"\n{col} (Vehicle):")
        print("Unique:", uniq)
        print(counts)

    # Correlation analysis
    print("\nCorrelations:\n", correlation_analysis(merged))

    # Regression
    reg_df = merged.select(
        [
            "subrogation",
            "vehicle_price",
            "claim_est_payout",
            "liab_prct",
            "witness_present",
            "policy_report_filed",
        ]
    )
    model = run_regression(reg_df)
    print("\nRegression Results:\n")
    print(model.summary())


# if __name__ == "__main__":
# DATA_DIR = "data/tri_guard_5_py_clean"
# main(DATA_DIR)
