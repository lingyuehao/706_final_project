import pandas as pd
from pathlib import Path

IN_PATH = "Training_TriGuard.csv"
OUT_DIR = Path("tri_guard_5_py_clean")
OUT_DIR.mkdir(parents=True, exist_ok=True)


df_raw = pd.read_csv(IN_PATH, dtype=str, keep_default_na=False)


def strip_df(df):
    return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)


df = strip_df(df_raw.copy())


def pick(*names):
    return [c for c in names if c in df.columns]


claim_keep = pick(
    "claim_number",
    "subrogation",
    "claim_est_payout",
    "liab_prct",
    "claim_date",
    "claim_day_of_week",
    "channel",
    "zip_code",
    "zip",
    "witness_present_ind",
    "policy_report_filed_ind",
    "in_network_bodyshop",
)
driver_cols = pick("year_of_born", "gender", "age_of_DL", "safety_rating")
policy_cols = pick(
    "annual_income",
    "high_education_ind",
    "email_or_tel_available",
    "address_change_ind",
    "living_status",
    "past_num_of_claims",
)
veh_cols = pick(
    "vehicle_made_year",
    "vehicle_category",
    "vehicle_price",
    "vehicle_color",
    "vehicle_weight",
    "vehicle_mileage",
    "age_of_vehicle",
)
acc_cols = pick("accident_site", "accident_type")

if "claim_number" in df.columns and "claim_number" not in claim_keep:
    claim_keep = ["claim_number"] + claim_keep

if "zip_code" in claim_keep and "zip" in claim_keep:
    claim_keep.remove("zip")


def build_dim(cols, key):
    if not cols:
        return pd.DataFrame(columns=[key])
    dim = df[cols].copy()
    mask_all_empty = dim.apply(lambda r: all((v == "" for v in r)), axis=1)
    dim = dim[~mask_all_empty].drop_duplicates().reset_index(drop=True)
    dim[key] = range(1, len(dim) + 1)
    return dim


Driver = build_dim(driver_cols, "driver_key")
Policyholder = build_dim(policy_cols, "policyholder_key")
Vehicle = build_dim(veh_cols, "vehicle_key")
Accident = build_dim(acc_cols, "accident_key")


join_cols = list(dict.fromkeys(driver_cols + policy_cols + veh_cols + acc_cols))
claim_cols_full = list(dict.fromkeys(claim_keep + join_cols))
Claim = (
    df[claim_cols_full].drop_duplicates(subset=["claim_number"])
    if "claim_number" in claim_cols_full
    else df[claim_cols_full]
).copy()


def left_merge_key(base, dim, on_cols):
    return (
        base.merge(dim, on=on_cols, how="left") if on_cols and not dim.empty else base
    )


Claim = left_merge_key(Claim, Accident, acc_cols)
Claim = left_merge_key(Claim, Policyholder, policy_cols)
Claim = left_merge_key(Claim, Vehicle, veh_cols)
Claim = left_merge_key(Claim, Driver, driver_cols)


final_cols = [c for c in claim_keep if c in Claim.columns] + [
    c
    for c in ["accident_key", "policyholder_key", "vehicle_key", "driver_key"]
    if c in Claim.columns
]
Claim = Claim[final_cols].copy()


def save_csv(df_, name):
    df_.to_csv(OUT_DIR / f"{name}.csv", index=False)


save_csv(Accident, "Accident")
save_csv(Policyholder, "Policyholder")
save_csv(Vehicle, "Vehicle")
save_csv(Driver, "Driver")
save_csv(Claim, "Claim")
