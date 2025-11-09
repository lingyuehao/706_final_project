
import polars as pl
from pathlib import Path

# -------------------------------------------------------------------
# Helper: cast numeric columns to Float64 and print NaN warnings
# -------------------------------------------------------------------
def cast_col_to_float_with_warnings(df: pl.DataFrame, col: str) -> pl.DataFrame:
    if col not in df.columns:
        return df

    tmp = df.select(
        pl.col(col).alias("orig"),
        pl.col(col).cast(pl.Float64, strict=False).alias("casted"),
    )

    # Identify invalid (non-numeric) values that become null
    invalid = (
        tmp.filter(
            pl.col("casted").is_null()
            & pl.col("orig").is_not_null()
            & (pl.col("orig").cast(pl.Utf8).str.strip_chars() != "")
        )
        .group_by("orig")
        .len()
        .sort("len", descending=True)
    )

    if invalid.height > 0:
        for row in invalid.iter_rows(named=True):
            print(
                f"⚠️ WARNING: column '{col}' value '{row['orig']}' "
                f"→ NaN (n={row['len']}). Be cautious before joins."
            )

    return df.with_columns(pl.col(col).cast(pl.Float64, strict=False))


def cast_many_to_float(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    for c in cols:
        df = cast_col_to_float_with_warnings(df, c)
    return df


# -------------------------------------------------------------------
# Locate and load data
# -------------------------------------------------------------------
def find_project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "data").exists():
            return parent
    return here.parent


ROOT = find_project_root()
DATA_DIR = ROOT / "data" / "tri_guard_5_py_clean"

CLAIM_CSV = DATA_DIR / "Claim.csv"
POLICYHOLDER_CSV = DATA_DIR / "Policyholder.csv"

if not CLAIM_CSV.exists() or not POLICYHOLDER_CSV.exists():
    raise FileNotFoundError(
        f"Missing Claim.csv or Policyholder.csv in {DATA_DIR}.\n"
        "From project root, run:\n"
        "  python scripts/split_triguard_5tables.py"
    )

claim = pl.read_csv(CLAIM_CSV)
policyholder = pl.read_csv(POLICYHOLDER_CSV)

# -------------------------------------------------------------------
# Convert numeric columns to Float64 consistently
# -------------------------------------------------------------------
claim_float_cols = ["policyholder_key", "claim_est_payout", "liab_prct"]
policyholder_float_cols = [
    "policyholder_key",
    "annual_income",
    "past_num_of_claims",
    "high_education_ind",
    "email_or_tel_available",
    "address_change_ind",
]

claim = cast_many_to_float(claim, claim_float_cols)
policyholder = cast_many_to_float(policyholder, policyholder_float_cols)

# -------------------------------------------------------------------
# Register tables for SQL
# -------------------------------------------------------------------
ctx = pl.SQLContext()
ctx.register("claim", claim)
ctx.register("policyholder", policyholder)


query1 = """
SELECT
  p.high_education_ind AS has_higher_ed,
  AVG(c.claim_est_payout) AS avg_payout,
  COUNT(*) AS total_claims
FROM claim c
LEFT JOIN policyholder p
  ON c.policyholder_key = p.policyholder_key
GROUP BY 1
ORDER BY avg_payout DESC
"""

query2 = """
SELECT
  p.annual_income,
  COUNT(*) AS num_claims,
  AVG(c.claim_est_payout) AS avg_payout
FROM claim c
JOIN policyholder p
  ON c.policyholder_key = p.policyholder_key
GROUP BY 1
ORDER BY num_claims DESC
"""

query3 = """
SELECT
  p.high_education_ind AS has_higher_ed,
  AVG(p.past_num_of_claims) AS avg_past_claims
FROM policyholder p
GROUP BY 1
ORDER BY avg_past_claims
"""

query4 = """
SELECT
  CASE WHEN p.high_education_ind = 1 THEN 'HigherEdu' ELSE 'NonEdu' END AS education,
  p.living_status,
  ROUND(AVG(c.claim_est_payout), 2) AS avg_payout,
  COUNT(*) AS num_claims
FROM claim c
JOIN policyholder p
  ON c.policyholder_key = p.policyholder_key
GROUP BY 1, 2
ORDER BY avg_payout DESC
"""

query5 = """
WITH behavior AS (
  SELECT
    p.past_num_of_claims AS prev_claims,
    COUNT(c.claim_number) AS new_claims,
    AVG(c.claim_est_payout) AS avg_new_payout
  FROM claim c
  JOIN policyholder p
    ON c.policyholder_key = p.policyholder_key
  GROUP BY 1
)
SELECT
  prev_claims,
  new_claims,
  ROUND(avg_new_payout, 2) AS avg_payout
FROM behavior
ORDER BY prev_claims
"""

query6 = """
SELECT
  c.channel,
  COUNT(*) AS num_claims,
  ROUND(AVG(c.claim_est_payout), 2) AS avg_payout,
  ROUND(SUM(c.claim_est_payout), 2) AS total_payout
FROM claim c
GROUP BY 1
ORDER BY avg_payout DESC
"""

# -------------------------------------------------------------------
# Run all analyses
# -------------------------------------------------------------------
queries = {
    "avg_payout_by_education": query1,
    "claims_by_income": query2,
    "past_claims_by_education": query3,
    "education_vs_living_status": query4,
    "past_claim_behavior": query5,
    "channel_effect": query6,
}
