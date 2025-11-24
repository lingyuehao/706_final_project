import os
import numpy as np
import polars as pl
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder


def create_output_directory(path: str) -> None:
    """Ensure that an output directory exists."""
    os.makedirs(path, exist_ok=True)


def load_data(accident_path: str, claim_path: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load accident and claim CSV files and cast accident_key to Int64."""
    accident = pl.read_csv(accident_path).with_columns(
        pl.col("accident_key").cast(pl.Int64)
    )
    claim = pl.read_csv(claim_path).with_columns(pl.col("accident_key").cast(pl.Int64))
    return accident, claim


def group_distribution(
    df: pl.DataFrame, group_col: str, count_alias: str
) -> pl.DataFrame:
    """Compute distribution of records by a specified group column."""
    result = (
        df.group_by(group_col)
        .agg([pl.len().alias(count_alias)])
        .with_columns(
            (pl.col(count_alias) * 100.0 / pl.col(count_alias).sum())
            .round(2)
            .alias("percentage")
        )
        .sort(count_alias, descending=True)
    )
    return result


def join_accident_claim(accident: pl.DataFrame, claim: pl.DataFrame) -> pl.DataFrame:
    """Join accident and claim dataframes on 'accident_key'."""
    return accident.join(claim, on="accident_key", how="inner")


def compute_high_subrogation(
    accident: pl.DataFrame, claim: pl.DataFrame
) -> pl.DataFrame:
    """Identify accident groups with high subrogation potential."""
    return (
        accident.join(claim, on="accident_key", how="inner")
        .filter(
            (pl.col("accident_type").str.contains("multi_vehicle"))
            & (pl.col("witness_present_ind") == "Yes")
            & (pl.col("policy_report_filed_ind") == 1)
        )
        .group_by(["accident_type", "accident_site"])
        .agg([pl.len().alias("high_potential_claims")])
        .with_columns(
            (pl.col("high_potential_claims") * 100.0 / len(claim))
            .round(2)
            .alias("percentage_of_claims")
        )
        .sort("high_potential_claims", descending=True)
    )


def compute_subrogation_indicators(df: pl.DataFrame, group_col: str) -> pl.DataFrame:
    """Calculate subrogation potential by accident type or site."""
    result = (
        df.group_by(group_col)
        .agg(
            [
                pl.len().alias("total_claims"),
                pl.col("witness_present_ind")
                .eq("Yes")
                .sum()
                .alias("claims_with_witness"),
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
            (pl.col("claims_with_both") * 100.0 / pl.col("total_claims"))
            .round(2)
            .alias("subrogation_potential_pct")
        )
        .sort("subrogation_potential_pct", descending=True)
    )
    return result


def comprehensive_subrogation(
    accident: pl.DataFrame, claim: pl.DataFrame
) -> pl.DataFrame:
    """Create a comprehensive view of subrogation priority."""
    df = (
        accident.join(claim, on="accident_key", how="inner")
        .group_by(["accident_key", "accident_site", "accident_type"])
        .agg(
            [
                pl.len().alias("claim_count"),
                pl.col("witness_present_ind").eq("Yes").sum().alias("witness_count"),
                pl.col("policy_report_filed_ind")
                .eq(1)
                .sum()
                .alias("police_report_count"),
            ]
        )
        .with_columns(
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
        )
        .sort(["subrogation_priority", "claim_count"], descending=[False, True])
    )
    return df


def regression_analysis(
    accident: pl.DataFrame, claim: pl.DataFrame, output_dir: str
) -> None:
    """Perform regression analysis and save results."""
    regression_data = join_accident_claim(accident, claim)
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
    ).to_pandas()

    # Encode categorical features
    le_site, le_type, le_channel = LabelEncoder(), LabelEncoder(), LabelEncoder()
    regression_df["accident_site_encoded"] = le_site.fit_transform(
        regression_df["accident_site"]
    )
    regression_df["accident_type_encoded"] = le_type.fit_transform(
        regression_df["accident_type"]
    )
    regression_df["channel_encoded"] = le_channel.fit_transform(
        regression_df["channel"]
    )

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

    regression_pd_clean = regression_df[feature_columns + ["claim_est_payout"]].dropna()
    X = regression_pd_clean[feature_columns].values
    y = regression_pd_clean["claim_est_payout"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    model = LinearRegression().fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse, r2 = mean_squared_error(y_test, y_pred), r2_score(y_test, y_pred)

    regression_results = pl.DataFrame(
        {
            "metric": ["target_variable", "mse", "r2_score", "num_samples"]
            + [f"coef_{feat}" for feat in feature_columns]
            + ["intercept"],
            "value": [
                "claim_est_payout",
                str(mse),
                str(r2),
                str(len(regression_pd_clean)),
            ]
            + [str(coef) for coef in model.coef_]
            + [str(model.intercept_)],
        }
    )
    regression_results.write_csv(f"{output_dir}/result12_regression_metrics.csv")

    feature_importance = pl.DataFrame(
        {
            "feature": feature_columns,
            "coefficient": model.coef_,
            "abs_coefficient": np.abs(model.coef_),
        }
    ).sort("abs_coefficient", descending=True)

    feature_importance.write_csv(f"{output_dir}/result13_feature_importance.csv")


def main():
    output_dir = "analysis/tina_accident/analysis_results"
    create_output_directory(output_dir)

    accident, claim = load_data(
        "data/tri_guard_5_py_clean/Accident.csv", "data/tri_guard_5_py_clean/Claim.csv"
    )

    # Basic stats and samples
    accident.select(
        [
            pl.len().alias("total_accidents"),
            pl.col("accident_site").n_unique().alias("unique_sites"),
            pl.col("accident_type").n_unique().alias("unique_types"),
        ]
    ).write_csv(f"{output_dir}/result1_basic_stats.csv")

    accident.head(10).write_csv(f"{output_dir}/result2_sample_data.csv")

    # Distribution results
    group_distribution(accident, "accident_type", "accident_count").write_csv(
        f"{output_dir}/result3_distribution_by_type.csv"
    )
    group_distribution(accident, "accident_site", "accident_count").write_csv(
        f"{output_dir}/result4_distribution_by_site.csv"
    )

    # Multi-vehicle accidents
    multi_df = (
        accident.filter(pl.col("accident_type").str.contains("multi_vehicle"))
        .group_by("accident_type")
        .agg([pl.len().alias("multi_vehicle_count")])
        .with_columns(
            (pl.col("multi_vehicle_count") * 100.0 / len(accident))
            .round(2)
            .alias("percentage_of_all")
        )
        .sort("multi_vehicle_count", descending=True)
    )
    multi_df.write_csv(f"{output_dir}/result5_multi_vehicle_accidents.csv")

    # Cross analysis
    accident.group_by(["accident_site", "accident_type"]).agg(
        [pl.len().alias("accident_count")]
    ).sort("accident_count", descending=True).head(20).write_csv(
        f"{output_dir}/result6_cross_analysis.csv"
    )

    # Join preview
    join_accident_claim(accident, claim).select(
        [
            "accident_key",
            "accident_site",
            "accident_type",
            "witness_present_ind",
            "policy_report_filed_ind",
            "in_network_bodyshop",
        ]
    ).head(20).write_csv(f"{output_dir}/result7_joined_data.csv")

    # Subrogation-related analyses
    compute_high_subrogation(accident, claim).write_csv(
        f"{output_dir}/result8_high_subrogation_potential.csv"
    )

    joined_data = join_accident_claim(accident, claim)
    compute_subrogation_indicators(joined_data, "accident_type").write_csv(
        f"{output_dir}/result9_subrogation_by_type.csv"
    )
    compute_subrogation_indicators(joined_data, "accident_site").write_csv(
        f"{output_dir}/result10_subrogation_by_site.csv"
    )
    comprehensive_subrogation(accident, claim).write_csv(
        f"{output_dir}/result11_comprehensive_subrogation.csv"
    )

    # Regression analysis
    regression_analysis(accident, claim, output_dir)


if __name__ == "__main__":
    main()
