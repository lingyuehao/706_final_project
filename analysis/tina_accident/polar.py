import os

import numpy as np
import polars as pl
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# create output directory
os.makedirs("analysis/tina_accident/analysis_results", exist_ok=True)

# load data
accident = pl.read_csv("data/tri_guard_5_py_clean/Accident.csv")
claim = pl.read_csv("data/tri_guard_5_py_clean/Claim.csv")

# cast accident_key to same type
accident = accident.with_columns(pl.col("accident_key").cast(pl.Int64))
claim = claim.with_columns(pl.col("accident_key").cast(pl.Int64))

# total accidents and basic stats
result1 = accident.select(
    [
        pl.len().alias("total_accidents"),
        pl.col("accident_site").n_unique().alias("unique_sites"),
        pl.col("accident_type").n_unique().alias("unique_types"),
    ]
)
result1.write_csv("analysis/tina_accident/analysis_results/result1_basic_stats.csv")

# sample of accident data
result2 = accident.head(10)
result2.write_csv("analysis/tina_accident/analysis_results/result2_sample_data.csv")

# Distribution by accident type
result3 = (
    accident.group_by("accident_type")
    .agg([pl.len().alias("accident_count")])
    .with_columns(
        [
            (pl.col("accident_count") * 100.0 / pl.col("accident_count").sum())
            .round(2)
            .alias("percentage")
        ]
    )
    .sort("accident_count", descending=True)
)
result3.write_csv(
    "analysis/tina_accident/analysis_results/result3_distribution_by_type.csv"
)

# distribution by accident site
result4 = (
    accident.group_by("accident_site")
    .agg([pl.len().alias("accident_count")])
    .with_columns(
        [
            (pl.col("accident_count") * 100.0 / pl.col("accident_count").sum())
            .round(2)
            .alias("percentage")
        ]
    )
    .sort("accident_count", descending=True)
)
result4.write_csv(
    "analysis/tina_accident/analysis_results/result4_distribution_by_site.csv"
)

# multi-vehicle accidents
result5 = (
    accident.filter(pl.col("accident_type").str.contains("multi_vehicle"))
    .group_by("accident_type")
    .agg([pl.len().alias("multi_vehicle_count")])
    .with_columns(
        [
            (pl.col("multi_vehicle_count") * 100.0 / len(accident))
            .round(2)
            .alias("percentage_of_all")
        ]
    )
    .sort("multi_vehicle_count", descending=True)
)
result5.write_csv(
    "analysis/tina_accident/analysis_results/result5_multi_vehicle_accidents.csv"
)

# cross-analysis: accident site and type
result6 = (
    accident.group_by(["accident_site", "accident_type"])
    .agg([pl.len().alias("accident_count")])
    .sort("accident_count", descending=True)
    .head(20)
)
result6.write_csv("analysis/tina_accident/analysis_results/result6_cross_analysis.csv")

# join accident and claim data
result7 = (
    accident.join(claim, on="accident_key", how="inner")
    .select(
        [
            "accident_key",
            "accident_site",
            "accident_type",
            "witness_present_ind",
            "policy_report_filed_ind",
            "in_network_bodyshop",
        ]
    )
    .head(20)
)
result7.write_csv("analysis/tina_accident/analysis_results/result7_joined_data.csv")

# high subrogation potential
result8 = (
    accident.join(claim, on="accident_key", how="inner")
    .filter(
        (pl.col("accident_type").str.contains("multi_vehicle"))
        & (pl.col("witness_present_ind") == "Yes")
        & (pl.col("policy_report_filed_ind") == 1)
    )
    .group_by(["accident_type", "accident_site"])
    .agg([pl.len().alias("high_potential_claims")])
    .with_columns(
        [
            (pl.col("high_potential_claims") * 100.0 / len(claim))
            .round(2)
            .alias("percentage_of_claims")
        ]
    )
    .sort("high_potential_claims", descending=True)
)
result8.write_csv(
    "analysis/tina_accident/analysis_results/result8_high_subrogation_potential.csv"
)

# subrogation indicators by accident type
result9 = (
    accident.join(claim, on="accident_key", how="inner")
    .group_by("accident_type")
    .agg(
        [
            pl.len().alias("total_claims"),
            pl.col("witness_present_ind").eq("Yes").sum().alias("claims_with_witness"),
            pl.col("policy_report_filed_ind")
            .eq(1)
            .sum()
            .alias("claims_with_police_report"),
            (
                (pl.col("witness_present_ind") == "Yes")
                & (pl.col("policy_report_filed_ind") == 1)
            )
            .sum()
            .alias("claims_with_both"),
        ]
    )
    .with_columns(
        [
            (pl.col("claims_with_both") * 100.0 / pl.col("total_claims"))
            .round(2)
            .alias("subrogation_potential_pct")
        ]
    )
    .sort("subrogation_potential_pct", descending=True)
)
result9.write_csv(
    "analysis/tina_accident/analysis_results/result9_subrogation_by_type.csv"
)

# subrogation indicators by accident site
result10 = (
    accident.join(claim, on="accident_key", how="inner")
    .group_by("accident_site")
    .agg(
        [
            pl.len().alias("total_claims"),
            pl.col("witness_present_ind").eq("Yes").sum().alias("claims_with_witness"),
            pl.col("policy_report_filed_ind")
            .eq(1)
            .sum()
            .alias("claims_with_police_report"),
            (
                (pl.col("witness_present_ind") == "Yes")
                & (pl.col("policy_report_filed_ind") == 1)
            )
            .sum()
            .alias("claims_with_both"),
        ]
    )
    .with_columns(
        [
            (pl.col("claims_with_both") * 100.0 / pl.col("total_claims"))
            .round(2)
            .alias("subrogation_potential_pct")
        ]
    )
    .sort("subrogation_potential_pct", descending=True)
)
result10.write_csv(
    "analysis/tina_accident/analysis_results/result10_subrogation_by_site.csv"
)

# comprehensive view with subrogation priority
result11 = (
    accident.join(claim, on="accident_key", how="inner")
    .group_by(["accident_key", "accident_site", "accident_type"])
    .agg(
        [
            pl.len().alias("claim_count"),
            pl.col("witness_present_ind").eq("Yes").sum().alias("witness_count"),
            pl.col("policy_report_filed_ind").eq(1).sum().alias("police_report_count"),
        ]
    )
    .with_columns(
        [
            pl.when(
                (pl.col("accident_type").str.contains("multi_vehicle"))
                & (pl.col("witness_count") > 0)
                & (pl.col("police_report_count") > 0)
            )
            .then(pl.lit("High"))
            .when(pl.col("accident_type").str.contains("multi_vehicle"))
            .then(pl.lit("Medium"))
            .otherwise(pl.lit("Low"))
            .alias("subrogation_priority")
        ]
    )
    .sort(["subrogation_priority", "claim_count"], descending=[False, True])
)
result11.write_csv(
    "analysis/tina_accident/analysis_results/result11_comprehensive_subrogation.csv"
)

# regression analysis
regression_data = accident.join(claim, on="accident_key", how="inner")

# create binary features
regression_df = regression_data.with_columns(
    [
        pl.when(pl.col("accident_type").str.contains("multi_vehicle"))
        .then(1)
        .otherwise(0)
        .alias("is_multi_vehicle"),
        pl.when(pl.col("witness_present_ind") == "Yes")
        .then(1)
        .otherwise(0)
        .alias("has_witness"),
        pl.when(pl.col("policy_report_filed_ind") == 1)
        .then(1)
        .otherwise(0)
        .alias("has_police_report"),
        pl.when(pl.col("in_network_bodyshop") == "Yes")
        .then(1)
        .otherwise(0)
        .alias("in_network"),
        pl.col("subrogation").alias("has_subrogation"),
    ]
)

# convert to pandas for easier encoding
regression_pd = regression_df.to_pandas()

# encode categorical variables
le_site = LabelEncoder()
le_type = LabelEncoder()
le_channel = LabelEncoder()

regression_pd["accident_site_encoded"] = le_site.fit_transform(
    regression_pd["accident_site"]
)
regression_pd["accident_type_encoded"] = le_type.fit_transform(
    regression_pd["accident_type"]
)
regression_pd["channel_encoded"] = le_channel.fit_transform(regression_pd["channel"])

# prepare features
feature_columns = [
    "is_multi_vehicle",
    "has_witness",
    "has_police_report",
    "in_network",
    "has_subrogation",
    "liab_prct",
    "accident_site_encoded",
    "accident_type_encoded",
    "channel_encoded",
]

# drop rows with missing values in features or target
regression_pd_clean = regression_pd[feature_columns + ["claim_est_payout"]].dropna()

X = regression_pd_clean[feature_columns].values
y = regression_pd_clean["claim_est_payout"].values

# split data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# train model
model = LinearRegression()
model.fit(X_train, y_train)

# predictions
y_pred = model.predict(X_test)

# metrics
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

# save regression results with all coefficients
regression_results = pl.DataFrame(
    {
        "metric": ["target_variable", "mse", "r2_score", "num_samples"]
        + [f"coef_{feat}" for feat in feature_columns]
        + ["intercept"],
        "value": [
            str("claim_est_payout"),
            str(mse),
            str(r2),
            str(len(regression_pd_clean)),
        ]
        + [str(coef) for coef in model.coef_]
        + [str(model.intercept_)],
    }
)
regression_results.write_csv(
    "analysis/tina_accident/analysis_results/result12_regression_metrics.csv"
)

# save feature importance
feature_importance = pl.DataFrame(
    {
        "feature": feature_columns,
        "coefficient": model.coef_,
        "abs_coefficient": np.abs(model.coef_),
    }
).sort("abs_coefficient", descending=True)
feature_importance.write_csv(
    "analysis/tina_accident/analysis_results/result13_feature_importance.csv"
)
