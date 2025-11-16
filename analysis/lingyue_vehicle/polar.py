import os

import polars as pl
import statsmodels.api as sm

# Paths
DATA_DIR = "data/tri_guard_5_py_clean"
claim_path = os.path.join(DATA_DIR, "Claim.csv")
vehicle_path = os.path.join(DATA_DIR, "Vehicle.csv")

# Load
claim = pl.read_csv(claim_path, ignore_errors=True)
vehicle = pl.read_csv(vehicle_path, ignore_errors=True)
print("Claim shape:", claim.shape)
print("Vehicle shape:", vehicle.shape)


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
claim_clean = claim.with_columns(
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
).with_columns(
    [
        to_bool(pl.col("witness_present_ind")).alias("witness_present"),
        to_bool(pl.col("policy_report_filed_ind")).alias("policy_report_filed"),
    ]
)

# Clean vehicle
vehicle_clean = vehicle.with_columns(
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

# Merge
merged = claim_clean.join(vehicle_clean, on="vehicle_key", how="inner")
print("Merged shape:", merged.shape)

# Summary
summary_claim = claim_clean.select(
    [
        pl.mean("subrogation").alias("avg_subrogation"),
        pl.mean("claim_est_payout").alias("avg_payout"),
        pl.mean("liab_prct").alias("avg_liability_pct"),
    ]
)
summary_vehicle = vehicle_clean.select(
    [
        pl.mean("vehicle_price").alias("avg_vehicle_price"),
        pl.mean("vehicle_mileage").alias("avg_mileage"),
        pl.mean("vehicle_weight").alias("avg_weight"),
    ]
)
print("\nClaim Summary:\n", summary_claim)
print("\nVehicle Summary:\n", summary_vehicle)

# Categorical summary
cat_cols_claim = [
    "channel",
    "claim_day_of_week",
    "witness_present_ind",
    "policy_report_filed_ind",
]
cat_cols_vehicle = ["vehicle_category", "vehicle_color"]

print("\n--- Categorical Variable Check ---")
for col in cat_cols_claim:
    print(f"\n{col} (Claim):")
    print("Unique:", claim_clean.select(pl.col(col).unique()).to_series().to_list())
    print(
        claim_clean.group_by(col)
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )

for col in cat_cols_vehicle:
    print(f"\n{col} (Vehicle):")
    print("Unique:", vehicle_clean.select(pl.col(col).unique()).to_series().to_list())
    print(
        vehicle_clean.group_by(col)
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )

# Correlations
corr = merged.select(
    pl.corr("subrogation", "vehicle_price").alias("corr_subro_price"),
    pl.corr("subrogation", "vehicle_mileage").alias("corr_subro_mileage"),
    pl.corr("claim_est_payout", "vehicle_price").alias("corr_payout_price"),
    pl.corr("subrogation", "liab_prct").alias("corr_subro_liability"),
)
print("\nCorrelations:\n", corr)

# Regression
df = merged.select(
    [
        "subrogation",
        "vehicle_price",
        "claim_est_payout",
        "liab_prct",
        "witness_present",
        "policy_report_filed",
    ]
).drop_nulls()

df_pd = df.to_pandas()
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
print("\nRegression Results:\n")
print(model.summary())
