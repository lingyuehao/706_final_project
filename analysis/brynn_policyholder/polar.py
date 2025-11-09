
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

OUT = ROOT / "analysis"
OUT.mkdir(exist_ok=True)

def run_and_save(name: str, sql: str) -> pl.DataFrame:
    print(f"\n▶ Running: {name}")
    df = ctx.execute(sql).collect()
    print(df)
    path = OUT / f"{name}.csv"
    df.write_csv(path)
    print(f"Saved → {path}")
    return df

# 1) Average payout by education (0/1)
query1 = """
SELECT
  p.high_education_ind AS has_higher_ed,
  AVG(c.claim_est_payout) AS avg_payout,
  COUNT(*) AS total_claims
FROM claim c
LEFT JOIN policyholder p
  ON c.policyholder_key = p.policyholder_key
GROUP BY p.high_education_ind
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
GROUP BY p.high_education_ind
ORDER BY avg_past_claims
"""

query4 = """
WITH edu_living AS (
  SELECT
    CASE WHEN p.high_education_ind = 1 THEN 'HigherEdu' ELSE 'NonEdu' END AS education,
    p.living_status,
    c.claim_est_payout
  FROM claim c
  JOIN policyholder p
    ON c.policyholder_key = p.policyholder_key
)
SELECT
  education,
  living_status,
  ROUND(AVG(claim_est_payout), 2) AS avg_payout,
  COUNT(*) AS num_claims
FROM edu_living
GROUP BY education, living_status
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
  GROUP BY p.past_num_of_claims
)
SELECT
  prev_claims,
  new_claims,
  ROUND(avg_new_payout, 2) AS avg_payout
FROM behavior
GROUP BY prev_claims, new_claims, avg_new_payout
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



def _fmt(x):
    try:
        if x is None:
            return "N/A"
        if isinstance(x, float):
            return f"{x:,.2f}"
        return str(x)
    except Exception:
        return str(x)

def run(name, sql):
    print(f"\n▶ Running: {name}")
    df = ctx.execute(sql).collect()
    print(df)
    out_path = OUT / f"{name}.csv"
    df.write_csv(out_path)
    print(f"Saved → {out_path}")
    return df

# ---- Query 1 ----
r1 = run("avg_payout_by_education", queries["avg_payout_by_education"])
if r1.height > 0:
    groups = {row["has_higher_ed"]: row for row in r1.iter_rows(named=True)}
    hi = groups.get(1.0) or groups.get(1) or groups.get("1")
    lo = groups.get(0.0) or groups.get(0) or groups.get("0")
    if hi and lo:
        diff = hi["avg_payout"] - lo["avg_payout"]
        pct = diff / lo["avg_payout"] * 100 if lo["avg_payout"] != 0 else 0
        print(
            f"Narrative: Higher-education policyholders file {int(hi['total_claims'])} claims "
            f"with avg payout ${_fmt(hi['avg_payout'])}, vs non-educated "
            f"{int(lo['total_claims'])} claims at ${_fmt(lo['avg_payout'])}. "
            f"Difference: {_fmt(pct)}% higher payouts."
        )

# ---- Query 2 ----
r2 = run("claims_by_income", queries["claims_by_income"])
if r2.height > 0:
    top_vol = r2.sort("num_claims", descending=True).row(0, named=True)
    top_avg = r2.sort("avg_payout", descending=True).row(0, named=True)
    print(
        f"Narrative: Most frequent income level = {_fmt(top_vol['annual_income'])} "
        f"with {int(top_vol['num_claims'])} claims (avg payout ${_fmt(top_vol['avg_payout'])}). "
        f"Highest avg payout = {_fmt(top_avg['annual_income'])} "
        f"with ${_fmt(top_avg['avg_payout'])}."
    )

# ---- Query 3 ----
r3 = run("past_claims_by_education", queries["past_claims_by_education"])
if r3.height > 0:
    rows = {row["has_higher_ed"]: row for row in r3.iter_rows(named=True)}
    hi = rows.get(1.0) or rows.get(1) or rows.get("1")
    lo = rows.get(0.0) or rows.get(0) or rows.get("0")
    if hi and lo:
        print(
            f"Narrative: On average, higher-education policyholders had "
            f"{_fmt(hi['avg_past_claims'])} past claims vs non-educated "
            f"{_fmt(lo['avg_past_claims'])}."
        )

# ---- Query 4 ----
r4 = run("education_vs_living_status", queries["education_vs_living_status"])
if r4.height > 0:
    top_avg = r4.sort("avg_payout", descending=True).row(0, named=True)
    top_vol = r4.sort("num_claims", descending=True).row(0, named=True)
    print(
        f"Narrative: Highest avg payout group = {top_avg['education']}/{top_avg['living_status']} "
        f"(${_fmt(top_avg['avg_payout'])}, {int(top_avg['num_claims'])} claims). "
        f"Most frequent group = {top_vol['education']}/{top_vol['living_status']} "
        f"with {int(top_vol['num_claims'])} claims."
    )

# ---- Query 5 ----
r5 = run("past_claim_behavior", queries["past_claim_behavior"])
if r5.height > 0:
    low = r5.sort("prev_claims").row(0, named=True)
    high = r5.sort("prev_claims", descending=True).row(0, named=True)
    print(
        f"Narrative: Policyholders with {int(low['prev_claims'])} prior claims "
        f"filed {int(low['new_claims'])} new ones (avg payout ${_fmt(low['avg_payout'])}). "
        f"Those with {int(high['prev_claims'])} prior claims filed "
        f"{int(high['new_claims'])} new ones (avg payout ${_fmt(high['avg_payout'])})."
    )

# ---- Query 6 ----
r6 = run("channel_effect", queries["channel_effect"])
if r6.height > 0:
    top_avg = r6.sort("avg_payout", descending=True).row(0, named=True)
    top_total = r6.sort("total_payout", descending=True).row(0, named=True)
    top_num = r6.sort("num_claims", descending=True).row(0, named=True)
    print(
        f"Narrative: Highest avg payout channel = {top_avg['channel']} "
        f"(${_fmt(top_avg['avg_payout'])}). Highest total payout = {top_total['channel']} "
        f"(${_fmt(top_total['total_payout'])}). Most active channel = {top_num['channel']} "
        f"({int(top_num['num_claims'])} claims)."
    )

print("\n✅ All six analyses completed")



