from pathlib import Path
import polars as pl
import statsmodels.formula.api as smf

# Configuration and Setup


def get_base_path() -> Path:
    """
    Return the project root directory based on the current file location.
    """
    return Path(__file__).resolve().parents[2]  # go up twice


def get_data_path(filename: str) -> Path:
    """
    Build a path to a file in the tri_guard_5_py_clean data directory.
    """
    return get_base_path() / "data" / "tri_guard_5_py_clean" / filename


def get_output_path(filename: str) -> Path:
    """
    Build a path in the current script's directory (for output files).
    """
    return Path(__file__).resolve().parent / filename


# Data Loading and Preparation


def load_csv_as_int(path: Path, int_col: str) -> pl.DataFrame:
    """
    Load a CSV into a Polars DataFrame and cast a specified column to Int64.
    """
    df = pl.read_csv(path)
    return df.with_columns(pl.col(int_col).cast(pl.Int64))


def merge_datasets(driver_df: pl.DataFrame, claim_df: pl.DataFrame) -> pl.DataFrame:
    """
    Merge driver and claim datasets on driver_key.
    """
    return driver_df.join(claim_df, on="driver_key", how="inner")


# Analysis Utilities


def compute_correlations(df: pl.DataFrame, target: str) -> pl.DataFrame:
    """
    Compute Pearson correlations between all numeric columns and a target column.

    Returns a Polars DataFrame with two columns:
        'column' - the numeric variable
        'corr_with_<target>' - correlation coefficient
    """
    numeric_types = (pl.Float64, pl.Int64)
    numeric_cols = [
        col
        for col, dtype in df.schema.items()
        if dtype in numeric_types and col != target
    ]

    corrs = [
        (col, df.select(pl.corr(pl.col(col), pl.col(target))).item())
        for col in numeric_cols
    ]

    corr_df = pl.DataFrame(corrs, schema=["column", f"corr_with_{target}"])
    return corr_df.sort(f"corr_with_{target}", descending=True)


# Modeling


def fit_logistic_regression(df: pl.DataFrame) -> None:
    """
    Fit and print a logistic regression model for subrogation prediction.
    """
    formula = (
        "subrogation ~ liab_prct + accident_key + "
        "policy_report_filed_ind + safety_rating + "
        "liab_prct:safety_rating"
    )
    model = smf.logit(formula=formula, data=df).fit()
    print(model.summary())


# Main Execution


def main():
    # Load data
    driver_df = load_csv_as_int(get_data_path("Driver.csv"), "driver_key")
    claim_df = load_csv_as_int(get_data_path("Claim.csv"), "driver_key")

    # Merge and export results
    merged_df = merge_datasets(driver_df, claim_df)
    merged_df.write_csv(get_output_path("driver_claim.csv"))

    # Compute and export correlations
    corr_df = compute_correlations(merged_df, target="subrogation")
    corr_df.write_csv(get_output_path("driver_subrogation_correlations.csv"))

    # Fit and summarize logistic regression
    fit_logistic_regression(merged_df)


if __name__ == "__main__":
    main()
