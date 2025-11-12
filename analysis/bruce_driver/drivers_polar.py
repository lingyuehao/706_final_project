from pathlib import Path
import polars as pl

# __file__ = analysis/bruce_driver/drivers_polar.py
# So we need to go up twice: bruce_driver -> analysis -> project root
base_path = Path(__file__).resolve().parent.parent.parent

# load driver data
driver_path = base_path / "data" / "tri_guard_5_py_clean" / "Driver.csv"
dr = pl.read_csv(driver_path)

# load claim data
claim_path = base_path / "data" / "tri_guard_5_py_clean" / "Claim.csv"
cl = pl.read_csv(claim_path)

# convert driver_key to Int64 for both dataframes
dr = dr.with_columns(pl.col("driver_key").cast(pl.Int64))
cl = cl.with_columns(pl.col("driver_key").cast(pl.Int64))

# Merge driver and claim data on 'driver key'
drivers = dr.join(cl, on="driver_key", how="inner")

# Export the merged table
drivers.write_csv(Path(__file__).resolve().parent / "driver_claim.csv")

# Compute Pearson correlation for each numerical column with 'subrogation'
num_cols = [c for c, dt in drivers.schema.items() if dt in (pl.Float64, pl.Int64)]
corrs = []

for col in num_cols:
    if col != "subrogation":
        val = drivers.select(pl.corr(pl.col(col), pl.col("subrogation"))).item()
        corrs.append((col, val))

corr_df = pl.DataFrame(corrs, schema=["column", "corr_with_subrogation"])
corr_df = corr_df.sort("corr_with_subrogation", descending=True)

corr_df.write_csv(Path(__file__).resolve().parent / "driver_subrogation_correlations.csv")

# Fit data for logistic regression
import statsmodels.formula.api as smf

# Formula with interactions included
formula = "subrogation ~ liab_prct + accident_key + policy_report_filed_ind + safety_rating + liab_prct:safety_rating"

logit_model = smf.logit(formula=formula, data=drivers).fit()
print(logit_model.summary())