# AutoRisk Subro

# Goals
- Identify opportunity of subrogation in first-party physical damage claims.
- Understand key indicators of subrogation opportunity.
- Provide a recommendation on how this information could be leveraged at TriGaurd Insurance Company

# Data Normalization and Validation
Note: by mingjie, please do not modify

## 1. Component Overview

This sector outlines the **data normalization and validation phase** of the project. The objective of this workstream was to refactor the original `Training_TriGuard.csv dataset` from a single, wide-format table into a relational database schema.

This foundational step is critical for the main project, as it improves data integrity, reduces data redundancy, and optimizes the dataset for subsequent, complex analysis.

This process was conducted in two stages:

1. **Transformation:** Splitting the original wide table into five normalized, topic-based tables.

2. **Validation:** Ensuring the five new tables can be merged to perfectly reproduce the original dataset, confirming the transformation was lossless.

## 2. Transformation Process

#### Original Source

- **File:** `Training_TriGuard.csv`

- **Description:** A single, denormalized table containing all information related to claims, accidents, policyholders, vehicles, and drivers.

#### Transformation Script

- **Script:** `split_triguard_5tables.py`

- **Action:** This script processes the original CSV and splits its columns into five logically distinct tables, linked by foreign keys.

#### Normalized Output Tables

The transformation resulted in the following five CSV files:

1. `Claim.csv`: The main fact table containing claim-specific details and the foreign keys (`accident_key`, `policyholder_key`, `vehicle_key`, `driver_key`).

2. `Accident.csv`: Dimension table with accident details.

3. `Policyholder.csv`: Dimension table with policyholder information.

4. `Vehicle.csv`: Dimension table with vehicle-specific data.

5. `Driver.csv`: Dimension table with driver information.

## 3. Validation and Integrity Check

To confirm that the splitting process was accurate, a "sanity check" was performed using the provided Jupyter Notebook.

- **Validation Notebook:** `data_inspection.ipynb`

#### Validation Process

The notebook executes the following steps:

1. **Load Data:** Loads the five new CSVs (`Claim.csv`, etc.) into pandas DataFrames.

2. **Load Original:** Loads the original `Training_TriGuard.csv` for comparison.

3. **Prepare Keys:** Converts the foreign key columns (accident_key, policyholder_key, vehicle_key, driver_key) in the main `df_claim` table to the `int64` data type to handle potential nulls (NaNs) and ensure type compatibility for merging.

4. **Reconstruct Table:** Performs a series of `left-merge` operations, starting with the `df_claim` DataFrame and joining the other four tables using their respective keys.

5. **Verify:** Compares the reconstructed, merged DataFrame against the original `Training_TriGuard` DataFrame to confirm they are identical.

#### Result

The `pd.testing.assert_frame_equal` test passed, confirming that the reconstructed table is **identical** to the original dataset. This validation confirms that the normalization process was successful, accurate, and lossless.

## 4. Next Steps

With data integrity validated, the five normalized tables are now ready for import into the **Postgres database**. This relational schema will serve as the clean foundation for the **project's next analytical phases**.