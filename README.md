# AutoRisk Subro

![CI Pipeline](https://github.com/lingyuehao/706_final_project/actions/workflows/ci.yml/badge.svg?branch=main)
![Code Quality](https://github.com/lingyuehao/706_final_project/actions/workflows/code-quality.yml/badge.svg?branch=main)

## Goals

- Identify opportunity of subrogation in first-party physical damage claims.

- Understand key indicators of subrogation opportunity.

- Provide a recommendation on how this information could be leveraged at TriGaurd Insurance Company

## Project Overview

This README provides a comprehensive guide for the project, covering environment setup, the data's origin story, and connection workflows.

- **Part 1: Environment Setup** Details a reproducible workflow for all team members using VS Code Dev Containers, Python, and a connection to AWS RDS Postgres.

- **Part 2: Data Normalization Process** Outlines the core data task that was performed: refactoring the `Training_TriGuard.csv` dataset into a normalized, five-table relational schema. This section explains the origin of the data now in the database.

- **Part 3: Getting Started** Provides the practical connection details for both Jupyter and DBeaver, and outlines the next steps for data analysis.

## Part 1: Environment Setup

This guide walks through the process from a fresh clone to a working setup.

**TL;DR:** Copy the files below, fill the local `.env`, and Reopen in Container. The database is already initialized and populated. Proceed to **Part 3** to connect.

### Repository Layout
```
.
├─ .devcontainer/
│  ├─ docker-compose.yml
│  ├─ devcontainer.json
│  └─ .env            # local only, NOT committed (use .env.example template)
├─ requirements.txt
├─ notebooks/
│  ├─ data_inspection.ipynb
│  └─ load_staging_data.ipynb   
└─ .env.example
```

### Dev Container Configuration

#### .devcontainer/docker-compose.yml

Single service dev container (connects to RDS directly):

```
services:
  dev:
    image: mcr.microsoft.com/devcontainers/python:3.11
    volumes:
      - ..:/workspaces:cached
    working_dir: /workspaces
    command: sleep infinity
    env_file: .env
    environment:
      # RDS typically requires SSL
      PGSSLMODE: "require"
```

#### .devcontainer/devcontainer.json

Installs psql client and Python deps on first boot.

```
{
  "name": "IDS706 Postgres Dev (RDS)",
  "dockerComposeFile": "docker-compose.yml",
  "service": "dev",
  "workspaceFolder": "/workspaces",
  "postCreateCommand": "sudo apt-get update && sudo apt-get install -y postgresql-client && pip install -r requirements.txt",
  "customizations": {
    "vscode": {
      "extensions": [
        "ms-python.python",
        "ms-toolsai.jupyter"
      ]
    }
  }
}
```

### Environment Variables

#### .devcontainer/.env (Local only, do not commit)

Fill with the RDS connection details. Example:

```
PGHOST=[your-rds-endpoint].amazonaws.com
PGUSER=[your_user]
PGPASSWORD=[your_password]
PGDATABASE=jet2_holiday
PGPORT=5432
PGSSLMODE=require
```

#### .env.example (Committed)

Template for teammates:

```
# Copy to .devcontainer/.env and fill values
# AWS Lightsail PostgreSQL
PGHOST=ls-56e0e6c1fb3506f3b9ee56f44ff2b9c804031cfd.c49qe0yao7zm.us-east-1.rds.amazonaws.com
PGUSER=dbmasteruser
PGPASSWORD=jet2_holiday
PGDATABASE=jet2_holiday
PGPORT=5432
PGSSLMODE=require
```

- **Security tip:** Never commit `.devcontainer/.env`. Add it to `.gitignore`.

### Python Dependencies

Minimal additions for DB + Parquet (append to the existing `requirements.txt`):

```
SQLAlchemy>=2.0
psycopg2-binary>=2.9
python-dotenv>=1.0
pyarrow>=16.0
```

### Start the Dev Container

1. Open the repo in VS Code.

2. Click the green corner button → **Reopen in Container**.

3. Wait for `postCreateCommand` to finish.

4. Once finished, proceed to **Part 3**.


## Part 2: Data Ingestion, Storage and Processing

###  Cloud Storage and Deployment (AWS Lightsail)

To ensure persistent data access and enable team collaboration, all cleaned tables (`Claim`, `Vehicle`, `Driver`, `Policyholder`, and `Accident`) were uploaded to an AWS Lightsail instance. AWS Lightsail provides a simplified cloud infrastructure with built-in SSH access and managed PostgreSQL database capabilities, making it suitable for lightweight data warehousing and remote querying. The Lightsail instance hosts a PostgreSQL container that mirrors our local schema (`stg`), allowing real-time querying and integration with analysis scripts in the `analysis/` folder.  

This setup ensures:

- Secure and persistent cloud storage  
- Seamless remote access for team members

<p align="center">
  <img src="data/AWS.png" alt="TriGuard ERD" width="700">
</p>


### Data Storage and Schema Design

To improve data consistency and enable efficient subrogation analysis, I normalized the original wide Training_TriGuard.csv dataset into five relational tables: Claim, Vehicle, Driver, Policyholder, and Accident. This decomposition follows a star schema, where Claim acts as the central fact table, linking to four dimension tables through foreign keys. The process was completed using Python (pandas) by extracting unique keys and attributes for each entity and ensuring referential integrity between tables.

| Table            | Description                                                                                                     | Primary Key        | Key Columns / Notes                                                                                   |
| ---------------- | --------------------------------------------------------------------------------------------------------------- | ------------------ | ----------------------------------------------------------------------------------------------------- |
| **Claim**        | Central fact table containing subrogation indicators, payout estimates, liability percentage, and related keys. | `claim_number`     | Links to all four dimension tables (`accident_key`, `policyholder_key`, `vehicle_key`, `driver_key`). |
| **Vehicle**      | Stores information about the insured vehicle such as make year, price, weight, and mileage.                     | `vehicle_key`      | Linked to Claim via `vehicle_key`.                                                                    |
| **Driver**       | Contains driver attributes like age, gender, license years, and driving state.                                  | `driver_key`       | Linked to Claim via `driver_key`.                                                                     |
| **Policyholder** | Includes policyholder-level data such as policy state, tenure months, and prior claims count.                   | `policyholder_key` | Linked to Claim via `policyholder_key`.                                                               |
| **Accident**     | Records accident-related information including location, date, weather, and severity.                           | `accident_key`     | Linked to Claim via `accident_key`.                                                                   |

Each table has been exported as a CSV file and loaded into the PostgreSQL stg schema.

### Entity Relationship Diagram (ERD)

The logical schema of the TriGuard Subrogation dataset is illustrated below:

<p align="center">
  <img src="data/TriGuard_ERD_pretty.png" alt="TriGuard ERD" width="700">
</p>

### Current Database State

The entire "bootstrap" and "normalization" process described below has been completed by Mingjie.

- The `stg` (Staging) and `mart` (Data Mart) schemas **have been created**.

- The five normalized tables (`claim`, `accident`, etc.) **have been loaded** into the `stg` schema.

Team members **do not** need to run any bootstrap or data loading scripts. The data is ready for use, as confirmed by this image:

The following sections simply document how this state was achieved.

### Workstream Objective

This sector outlines the **data normalization and validation phase** of the project. The objective of this workstream was to refactor the original `Training_TriGuard.csv dataset` from a single, wide-format table into a relational database schema.

This foundational step is critical, as it improves data integrity, reduces data redundancy, and optimizes the dataset for subsequent analysis.

### Transformation Process

- **Original Source:** `Training_TriGuard.csv` (A single, denormalized table).

- **Transformation Script:** `split_triguard_5tables.py`

- **Action:** This script processes the original CSV and splits its columns into five logically distinct tables, linked by foreign keys.

### Normalized Output Tables

The transformation resulted in the following five CSV files (which are now tables in `stg`):

1. `claim`: The main fact table with foreign keys.

2. `accident`: Dimension table with accident details.

3. `policyholder`: Dimension table with policyholder information.

4. `vehicle`: Dimension table with vehicle-specific data.

5. `driver`: Dimension table with driver information.

### Validation and Integrity Check

To confirm that the splitting process was accurate, a "sanity check" was performed using the `data_inspection.ipynb` notebook.

**Validation Process:**

1. **Load Data:** Loads the five new CSVs (`Claim.csv`, etc.) into pandas DataFrames.

2. **Load Original:** Loads the original `Training_TriGuard.csv` for comparison.

3. **Prepare Keys:** Converts the foreign key columns (`accident_key`, etc.) in the `df_claim` table to `int64` to ensure type compatibility for merging.

4. **Reconstruct Table:** Performs a series of `left-merge` operations, starting with the `df_claim` and joining the other four tables.

5. **Verify:** Compares the reconstructed, merged DataFrame against the original using `pd.testing.assert_frame_equal`.

**Result:** The test passed, confirming the normalization was successful, accurate, and lossless before the data was loaded.

## Part 3: Getting Started (Connecting and Workflow)

After completing Part 1 (launching the Dev Container), the database is ready to be queried.

### Connect from Jupyter / Python

This snippet can be used in a notebook (e.g., `notebooks/01_connect_and_test.ipynb`):

```
import os
import pandas as pd
from sqlalchemy import create_engine, text

PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT", "5432")
PGDB   = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASS = os.getenv("PGPASSWORD")
PGSSL  = os.getenv("PGSSLMODE", "require")

engine = create_engine(
    f"postgresql+psycopg2://{PGUSER}:{PGPASS}@{PGHOST}:{PGPORT}/{PGDB}"
    + (f"?sslmode={PGSSL}" if PGSSL else "")
)

with engine.begin() as con:
    print("Connection successful.")
    print("search_path =", con.execute(text("show search_path")).scalar())
    print("who/where   =", con.execute(text("select current_database(), current_user")).fetchone())

# Test by reading from the 'stg.claim' table, which is already loaded
print("\nVerifying data in stg.claim...")
df_check = pd.read_sql("SELECT * FROM stg.claim LIMIT 5", engine)
display(df_check)
```

### Connect from DBeaver (Local Desktop)

1. New Connection → PostgreSQL

2. Main tab:

    - Host: ls-56e0e6c1fb3506f3b9ee56f44ff2b9c804031cfd.c49qe0yao7zm.us-east-1.rds.amazonaws.com

    - Port: 5432

    - Database: jet2_holiday

    - Username: dbmasteruser

    - Password: jet2_holiday

3. SSL tab:

    - Mode: require

4. Test Connection → Finish.

### Common Issues

- Auth/timeout: Ensure the RDS security group allows the current client IP address on port 5432.

- SSL errors: Keep sslmode=require in all connections.

- Notebook cannot connect: Confirm the container has the env vars (echo $PGHOST) and that psql works from the VS Code terminal.

## Part4: Analysis Results

### Analysis Results of SQL analysis performed on the `accident` and `claim` datasets

This section documents the key findings from the SQL analysis performed on the `accident` and `claim` datasets to identify subrogation opportunities.
SQL queries are in `analysis/tina_accident/accident_claim.sql`.

**Query 1: Basic Statistics**

![accident query 1 result](analysis/tina_accident/results_screenshot/accident_q1.png)

- Total accidents: 12
- Unique accident sites: 4
- Unique accident types: 3

**Query 2: Detailed Accident Records**
The dataset contains 12 accidents with the following distribution:
- Accident sites: Parking Area, Unknown, Highway/Intersection, Local
- Accident types: multi_vehicle_clear, multi_vehicle_unclear, single_car
- Accident keys range from 1 to 12
  
<img src="analysis/tina_accident/results_screenshot/accident_q2.1.png"  width="300" height="150">
<img src="analysis/tina_accident/results_screenshot/accident_q2.2.png"  width="300" height="150">

**Query 3: Accident Type Distribution**

![accident query 3 result](analysis/tina_accident/results_screenshot/accident_q3.png)

The dataset is evenly distributed across three accident types, with multi-vehicle accidents (both clear and unclear) representing 66.67% of all accidents.

**Query 4: Accident Site Distribution**

![accident query 4 result](analysis/tina_accident/results_screenshot/accident_q4.png)

Accidents are evenly distributed across all four site types (25% each).

**Query 5: Multi-Vehicle Accident Analysis**

![accident query 5 result](analysis/tina_accident/results_screenshot/accident_q5.png)

8 out of 12 accidents (66.67%) are multi-vehicle incidents, indicating significant subrogation potential across the dataset.

**Query 6: Accident-Site-Type Combinations**

![accident query 6 result](analysis/tina_accident/results_screenshot/accident_q6.png)

The analysis shows all 12 unique combinations of accident sites and types, with each combination appearing exactly once. This suggests a well-distributed sample dataset covering various scenarios.

**Query 7: Comprehensive Accident-Claim Join**
Successfully joined accident characteristics with claim indicators including:
- Witness presence (Y/N)
- Policy report filed (0/1)
- In-network bodyshop usage (yes/no)
  
<img src="analysis/tina_accident/results_screenshot/accident_q7.png"  width="500">


**Query 8: High Subrogation Potential Claims**
This query identifies claims with the highest subrogation potential (multi-vehicle + witnesses + police report).

![accident query 8 result](analysis/tina_accident/results_screenshot/accident_q8.png)

Query 8 returned no results, indicating that none of the multi-vehicle accidents in this dataset have both a witness present and a police report filed. This suggests limited high-priority subrogation opportunities in the current dataset.

**Query 9: Accident Type Subrogation Indicators**

![accident query 9 result](analysis/tina_accident/results_screenshot/accident_q9.png)

No claims in the dataset have witnesses present, which severely limits subrogation potential. While police reports are filed for 60-65% of multi-vehicle claims, the absence of witnesses reduces the strength of potential subrogation cases.

**Query 10: Accident Site Subrogation Indicators**

![accident query 10 result](analysis/tina_accident/results_screenshot/accident_q10.png)

Across all accident sites, approximately 60% of claims have police reports filed, but again, the absence of witnesses across all locations limits subrogation effectiveness.

**Query 11: Comprehensive Subrogation Priority View**

![accident query 11 result](analysis/tina_accident/results_screenshot/accident_q11.png)

The analysis categorized accidents into subrogation priority levels:
- Low Priority: Single-car accidents (accidents 4, 5, 6, 9)
- Medium Priority: Multi-vehicle accidents without both witnesses and police reports (accidents 1, 2, 3, 7, 8, 10, 11, 12)
- High Priority: None identified (would require multi-vehicle + witness + police report)

#### Key Insights and Recommendations
1. Despite 66.67% of accidents being multi-vehicle incidents, the absence of witness documentation means no claims achieve "high priority" subrogation status.

2. 60-65% of claims have police reports filed, which is a positive indicator but insufficient alone for strong subrogation cases.

3. Multi-Vehicle Accident Distribution: 
   - multi_vehicle_clear: 6,190 claims (34.4%)
   - multi_vehicle_unclear: 6,555 claims (36.4%)
   - Combined multi-vehicle total: 12,745 claims (70.8% of all claims)

**Recommended actions for the company:**
1. Implement processes to ensure witness information is captured at the scene. This is the single most critical gap in the current data.

2. With over 70% of claims involving multiple vehicles, these represent the largest pool for potential subrogation, if witness and documentation gaps can be addressed.

3. The 6,190 "multi_vehicle_clear" claims with police reports (3,711) should be the first target for subrogation investigation, as fault determination is clearer.

4. Investigation Priority: 
   - First: Multi-vehicle accidents at Highway/Intersection with police reports
   - Second: Multi-vehicle accidents at Unknown/Parking Area with police reports
   - Third: All other multi-vehicle accidents

5. Data Quality: The "Unknown" accident site category (4,310 claims, 23.9%) should be minimized through better initial claims documentation.
---

### Polars-Based Regression Analysis on Claim Payouts

This section documents regression modeling performed using Polars to predict `claim_est_payout` and identify key cost drivers.

**Analysis Script:** `analysis/tina_accident/polar.py`

**Model Overview:**
- Target Variable: claim_est_payout
- Model Type: Linear Regression (OLS)
- Features: 9 variables including binary indicators, liability percentage, and encoded categorical variables
- Data Processing: Missing values removed, categorical variables label-encoded

**Model Performance:**
- R² Score: -0.0013 (negative, indicating model performs worse than baseline)
- MSE: 13248383
- RMSE: ~$3640 (square root of MSE)
- Sample Size: 17999 claims

**Feature Importance (Ranked by Absolute Coefficient):**

| Rank | Feature | Coefficient | Impact |
|------|---------|-------------|--------|
| 1 | Police Report Filed | +$110.53 | Claims with police reports have ~$111 higher payouts |
| 2 | Subrogation Flag | -$55.50 | Subrogation cases have ~$55 lower payouts (cost recovery) |
| 3 | Multi-vehicle Accident | +$43.93 | Multi-vehicle incidents increase payouts by ~$44 |
| 4 | Accident Type | +$31.43 | Different accident types have varying cost impacts |
| 5 | Channel | -$27.62 | Claim submission channel affects payout amounts |
| 6 | Accident Site | +$7.96 | Location has minor impact on costs |
| 7 | Liability Percentage | +$5.01 | Higher liability slightly increases payout |
| 8 | Witness Present | ~$0 | No measurable impact on payout amounts |
| 9 | In-network Bodyshop | ~$0 | No measurable impact on payout amounts |

**Base Intercept:** $3297 (baseline payout when all features are zero/reference category)

**Key Findings:**

1. No Predictive Power: The negative R² (-0.0013) indicates that this feature set cannot predict claim payout amounts—the model performs worse than simply predicting the mean payout for all claims. Claim amounts are driven primarily by factors not captured in this dataset, such as:
   - Actual damage severity
   - Medical costs and injuries
   - Vehicle repair complexity
   - Vehicle market value
   - Parts and labor costs

2. Documentation Impact: Police reports are the strongest predictor among available features (+$110), suggesting documented claims may involve more severe incidents or justify higher payouts.

3. Subrogation Effect: The negative coefficient for subrogation (-$55) indicates the company successfully recovers costs on flagged claims, reducing net payout amounts.

4. Witness Paradox: Despite witnesses being critical for subrogation success (per SQL analysis), they do not affect payout amounts—confirming their value lies in recovery potential, not cost prediction.

**Comparison with Prior Analyses:**

This regression complements the SQL-based subrogation analysis:
- SQL Analysis: Identified documentation (witness + police report) as key for subrogation opportunity
- Regression Analysis: Shows documentation affects payout amounts but explains essentially zero variance
- Combined Insight: High-payout claims with strong documentation (police report + witness) represent the best subrogation targets

**Limitations:**

- Current features explain 0% of payout variance (negative R²)
- Missing critical predictors: damage severity, injury indicators, vehicle value, repair costs
- Model is not suitable for production use without additional features

**Next Steps:**

1. Collect damage severity indicators (e.g., airbag deployment, towing required, total loss indicator)
2. Include vehicle value and actual repair cost estimates
3. Add injury/medical claim flags
4. Test non-linear models (Random Forest, XGBoost) for better predictions
5. Integrate high-payout claims with strong documentation into subrogation priority queue (Query 9 criteria)

**Files Generated:**
- `result12_regression_metrics.csv` - Model performance metrics and coefficients
- `result13_feature_importance.csv` - Feature ranking by absolute coefficient value
---

### Analysis Results (Claim & Vehicle Tables）

#### Query 1: By Vehicle Category & Color

Purpose: Compare vehicle category and color against subrogation likelihood, payout behavior, and claim-to-value ratio.

Findings:

- Highest subrogation rate: Compact / silver (0.2737) — followed by black (0.2525) and red (0.2409).

- Compact cars overall subrogate more frequently than Large or Medium vehicles.

- Subrogated claims tend to have slightly higher average payouts (e.g., Compact/silver ≈ $3,801 vs. $3,634 non-subro).

- Claim-to-value ratio remains around 0.16–0.17, suggesting consistent proportional payout levels.

<p align="center"> <img src="analysis/lingyue_vehicle/query1-1.png" alt="Query 1 Vehicle Category vs Color" width="700"> <br> <img src="analysis/lingyue_vehicle/query1-2.png" alt="Query 1 Extended Results" width="700"> </p>


#### Query 2: By Vehicle Price Band

Purpose: Analyze whether higher-value vehicles are more likely to be subrogated and how payout ratios change by price.

Findings:

- Subrogation rate rises slightly with vehicle value.

- Claim-to-value ratio decreases as price increases — expensive cars yield smaller proportional payouts.

- Indicates lower loss severity per dollar value for luxury segments.

<p align="center"> <img src="analysis/lingyue_vehicle/query2.png" alt="Query 2 Vehicle Price Band" width="700"> </p>


#### Query 3: By Mileage Band

Purpose: Examine how vehicle mileage correlates with subrogation frequency, liability percentage, and payout level.

Findings:

- Subrogation rate slightly declines as mileage increases.

- Newer, low-mileage cars (<10k) have higher payouts and slightly lower liability percentages, implying clearer fault assignment.

<p align="center"> <img src="analysis/lingyue_vehicle/query3.png" alt="Query 3 Mileage Band" width="700"> </p>


#### Query 4: Witness & Police Report Indicators

Purpose: Evaluate the effect of documentation (witness presence and police reports) on subrogation outcomes per vehicle category.

Findings:

- “Witness = Y & Police Report = 1” produces the highest subrogation rates:
  
  -- Compact: 0.3360 vs 0.1546 when N/0

  -- Large: 0.3227 vs 0.1500

  -- Medium: 0.3226 vs 0.1667

- Documentation significantly boosts subrogation likelihood across all segments.

<p align="center"> <img src="analysis/lingyue_vehicle/query4.png" alt="Query 4 Witness and Police Report Indicators" width="700"> </p>


#### Query 5: Category × Channel Interaction

Purpose: Compare subrogation frequency across claim submission channels for each vehicle type.

Findings:

- Phone and Broker channels outperform Online in subrogation rate (~0.24 vs ~0.22).

- Average payouts are similar, suggesting channel impacts probability, not payout magnitude.

<p align="center"> <img src="analysis/lingyue_vehicle/query5.png" alt="Query 5 Category vs Channel" width="700"> </p>

#### Summary

- Compact vehicles are most subrogation-prone, especially in silver/black/red colors.

- Witness and police documentation more than doubles subrogation odds.

- Low-mileage and mid-value vehicles show favorable recovery potential.

- Phone/Broker submissions outperform Online for effective subrogation follow-up.


#### Statistics Analysis

<div align="center"> <img src="analysis/lingyue_vehicle/summary.png" width="520"/> </div>

Highlights：

- Avg subrogation rate: 0.2286

- Avg claim payout: $3602

- Avg liability percentage: 38.28%

- Avg vehicle price: $30.6k

- Avg mileage: ~80k miles

These baselines provide a reference point for later comparisons.


We then examined main categorical fields (channel, day of week, witness indicator, police report indicator, vehicle category) to identify dominant levels and potential anomalies.

<div align="center"> <img src="analysis/lingyue_vehicle/c1.png" width="600"/> </div>
<div align="center"> <img src="analysis/lingyue_vehicle/c2.png" width="600"/> </div>

#### Key takeaways：

- Most claims come from Broker and Phone channels.

- Claim volumes are relatively even across weekdays.

- Witness and police report indicators are mostly complete (only one null).

- Vehicle categories are evenly distributed across Compact / Medium / Large.


We computed correlations between key numeric variables to evaluate basic relationships.

<div align="center"> <img src="analysis/lingyue_vehicle/correlation.png" width="520"/> </div>


- Subrogation has almost zero correlation with vehicle price or mileage.

- Liability percentage shows a moderate negative correlation (–0.30) with subrogation.

- Claim payout and vehicle price are essentially uncorrelated.

- This confirms that vehicle value itself is not driving subrogation outcomes.


We also fit an OLS regression with the following predictors:
vehicle_price, claim_est_payout, liab_prct, witness_present, policy_report_filed.

<div align="center"> <img src="analysis/lingyue_vehicle/c2.png" width="650"/> </div>

- Liability percentage is the strongest factor (negative direction).

- Witness present significantly increases the likelihood of subrogation.

- Police report filed has a smaller but positive effect.

- Very low R² (~0.12) indicates subrogation decisions depend on additional factors not captured in the dataset.
  

### Logistic Regression on Subrogation

![Log regression summary table](analysis/bruce_driver/Log_reg_result.png)
- Logistic regression confirms liab_prct as the key driver, while controlling for others and quantifying independent effects.
- The model’s pseudo R² ≈ 0.10 suggests there are other unmodeled factors (perhaps policy type, damages, or party fault data).
- Correlations of each variable with subrogation is stored in [this correlations table](analysis/bruce_driver/driver_subrogation_correlations.csv).

### Queries Analysis on `Driver` and `Claim` tables
#### Query 1: Gender breakdown
![Average age of obtaining DL by gender](analysis/bruce_driver/avg_age_dl.png)

This SQL query groups all drivers by gender and calculates three key statistics for each group: the total number of drivers, the average age at which they obtained their driver’s license, and the average safety rating. The results show that there are slightly more male drivers (7,800) than female drivers (7,390). Both genders obtained their driver’s licenses at roughly the same average age of 22.1 years. However, male drivers have a marginally higher average safety rating (74.26) compared to female drivers (73.91), suggesting that overall driving safety performance is very similar between genders, with only a small difference in favor of male drivers.

#### Query 2: Driving experience categorization
![Categorize age of obtaining DL by gender](analysis/bruce_driver/categorize_age_dl.png)

This query breaks down the driver data further by both **gender** and **driver’s license age group** (`dl_age_group`), summarizing how safety ratings vary depending on when drivers obtained their licenses. It reports the **number of drivers** and their **average safety rating** for each combination of gender and age group (Early, Mid, and Young). The results show that most drivers obtained their licenses in the “Young” group, with over 5,000 drivers for each gender. Within each age group, male and female drivers have fairly similar safety ratings, though small differences appear: females have slightly higher ratings in the “Early” and “Mid” groups, whereas males have a marginally higher rating in the “Young” group. Across all categories, the differences in average safety rating are minimal (within about 0.5 points), suggesting that neither gender nor license-age group strongly influences safety performance, though those who got their licenses earlier tend to have slightly higher average safety ratings overall.

#### Query 3: Safety ratings distribution and gender breakdown
![Quartile statistics about safey ratings by gender](analysis/bruce_driver/safety_rating.png)

This query groups drivers by **gender** and by **performance quartile** - specifically, whether their safety rating falls in the bottom quartile, middle 50%, or top quartile of all drivers. For each subgroup, it shows how many drivers belong to it, the **median safety rating** of that group (`median_group`), the **overall median** for that gender (`median_total_gender`), and how much the group’s median differs from the total median (`diff_from_total_med`). The results indicate a predictable gradient: drivers in the top quartile have substantially higher median ratings (around 92) compared to those in the bottom quartile (around 57). The middle 50% cluster near the overall gender medians (74–75). Both genders follow the same pattern, but male medians are slightly higher overall, as reflected in the one‑point difference in total median values. The `diff_from_total_med` column reinforces this trend—bottom‑quartile drivers score roughly 17–18 points below their gender’s median, while top‑quartile drivers exceed it by a similar margin—showing a balanced distribution of performance within each gender group.


### Analysis Results of SQL analysis performed on the `accident` and `policyholder` datasets


**Query 1: Avg Payout by Education Level**

Joined Claim with Policyholder to compare average payout and claim volume between policyholders with vs. without higher education.

<img width="342" height="159" alt="Screenshot 2025-11-14 at 10 40 47 AM" src="https://github.com/user-attachments/assets/e24d23ef-dcc3-4d24-91dc-153e7cd8b449" />

- **Insights:**
  - Higher-education group: 12,482 claims, average payout $3,609
  - Non-education group: 5,518 claims, average payout $3,586
  - Very similar payout levels across groups
  - Education does not meaningfully influence payout severity



**Query 2: Claims Distribution by Income**

Grouped claims by annual income and calculated claim frequency and average payout.

<img width="311" height="271" alt="Screenshot 2025-11-14 at 10 42 09 AM" src="https://github.com/user-attachments/assets/af616024-f757-4a94-ace0-4cd060e1b34a" />

- **Insights:**
  - A few income brackets dominate claim volume (e.g. 30k band with 3,818 claims).
  - High-income bands file far fewer claims.
  - Average payout varies widely due to low observation counts in many brackets.
  - Income is not correlated with stronger subrogation documentation.



**Query 3: Past Claim History by Education**

Calculated average number of past claims for each education segment.

<img width="291" height="146" alt="Screenshot 2025-11-14 at 10 45 00 AM" src="https://github.com/user-attachments/assets/6f1f9f67-b327-434d-bb9d-27ad73b7d17f" />

- Insights:
  - Both groups averaged around 4 past claims.
  - No meaningful behavioral difference.
  - Not useful for detecting recoverable vs. non-recoverable claims.



**Query 4: Education × Living Status Segmentation**

Computed average payout and claim counts across living status (rent vs own) stratified by education.

<img width="421" height="169" alt="Screenshot 2025-11-14 at 10 46 07 AM" src="https://github.com/user-attachments/assets/bfb98b82-6539-4841-94dc-837d6438cc24" />

- Insights:

  - Renters (both education levels) filed more claims.
  - Claim severity varies modestly between cells (~$3,530–$3,660).
  - Useful for demographic profiling but not for subrogation opportunity.
 
  
**Query 5: Past Claim Behavior (Prev Claims vs New Claims)**

Grouped policyholders by their historical claim count and computed new claim frequency and average payout.

<img width="430" height="272" alt="Screenshot 2025-11-14 at 10 47 50 AM" src="https://github.com/user-attachments/assets/8a909649-d429-4c84-89c8-8e8c80a82506" />

- Insights:
  - Large spike at 0 past claims → 9,875 new claims, meaning most policyholders are new claimants.
  - High-frequency claimants (20+ past claims) exist but are extremely rare.
  - Past claim count does not predict documentation quality (police report, witness, etc.)


**Query 6: Channel Effect**

Analyzed claims by reporting channel (Broker vs Online vs Phone).

<img width="383" height="171" alt="Screenshot 2025-11-14 at 10 50 06 AM" src="https://github.com/user-attachments/assets/c72f902a-b342-416e-aaa1-1e7e57b70c42" />

- Insights:
  - Broker channel produced the most claims (9,573) and highest total payout.
  - Average payouts across channels are nearly identical ($3,500–$3,625).
  - Channel does not meaningfully predict subrogation potential.


**Query 7: Accident Key Analysis**

Grouped claims by accident_key and computed frequency and average payout.

<img width="348" height="270" alt="Screenshot 2025-11-14 at 10 51 10 AM" src="https://github.com/user-attachments/assets/cfebf1b6-e785-4ab9-873a-4c7eb339d767" />

- Insights:
  - Accident Key 11, 10, 8, 3, and 7 dominate claim volume (all >1,400 claims).
  - These could represent common accident categories (e.g., rear-end, sideswipe), but without accident metadata they remain numeric codes.
  - Higher-frequency accident keys warrant focused subrogation attention if documentation is strong.

**Query 8: Witness + Police Report Analysis**

Evaluated documentation quality by accident_key:
  - Witness present? (Y/N)
  - Police report filed? (0/1)

<img width="661" height="273" alt="Screenshot 2025-11-14 at 10 52 21 AM" src="https://github.com/user-attachments/assets/2a533f3e-7611-4926-97a9-bf5846e9e8c8" />

- Insights:
  - The most common scenario:
   - Accident Key 11, witness N, police report 1, with 604 claims.
  - Witness presence is very low overall.
  - Police report rate is moderate, but lack of witnesses weakens recovery cases.
  - Strong documentation combinations (Witness Y + Police Report 1) are rare.

**Query 9: High-Priority Subrogation Candidates**

Filtered for the highest-quality subrogation opportunities using:
  - police_report_filed_ind = 1
  - witness_present_ind = 'Y'
  - liab_prct between 20–80 (avoids obvious 0/100% liability cases)

<img width="686" height="288" alt="Screenshot 2025-11-14 at 10 54 21 AM" src="https://github.com/user-attachments/assets/f488fd13-0111-4422-a825-9f39d0c38121" />

<img width="677" height="190" alt="Screenshot 2025-11-14 at 10 54 39 AM" src="https://github.com/user-attachments/assets/57a74d45-5126-4418-9c52-ff894a134361" />

- Insights:
  - 4,413 claims meet high-priority conditions.
  - Top candidates show payout amounts around $21,504, indicating large recovery value.
  - Many high-priority cases have 0 past claims, suggesting non-fraudulent, strong documentation scenarios.
  - Accident_keys 2, 3, 4, 8, 9, and 10 appear repeatedly among high-priority cases.
 
  

#### Key Insights and Recommendations
**1. Demographics do NOT predict subrogation opportunity.**
Education, income, renter vs owner, and past claims show little relationship with documentation quality or payouts. These are not actionable for recovery.
**2. Documentation quality is the most important driver.**
Query 8 shows:
- Witness presence is rare
- Police reports are more common
- The strongest subrogation cases need both, but this combination is extremely uncommon
**3. Accident keys 11, 10, 8, 3, 7 are the highest-volume accident categories.**
These categories should be prioritized if documentation exists.
**4. Query 9 identified 4,413 high-quality subrogation candidates.**
These claims have:
- Witness present
- Police report filed
- Non-extreme liability percentages
- These should be immediately routed to a recovery team.
**5. Many strong cases cluster around higher payouts.**
This increases the financial value of directing subrogation resources strategically.

#### **Recommended actions for the company:**
###### **1. Strengthen Documentation Collection at the Scene**
**Recommendation:**
Improve capture of witness information, photos, and incident details through enhanced intake processes (mobile app prompts, adjuster checklists, broker training).

**Reason:**
Across the dataset, witness presence is extremely rare, even though it is one of the strongest predictors of successful subrogation.
Query 8 shows thousands of police-reported claims but almost no witnesses.
This missing information significantly weakens TriGuard’s ability to establish fault and pursue recovery.

Better documentation → stronger evidence → higher subrogation recovery rates.



###### **2. Create a High-Priority Subrogation Queue (Using Query 9 Criteria)**
**Recommendation:**
Automatically flag claims that meet the following:
- Police report filed
- Witness present
- Liability between 20–80%
- High payout (e.g., >$7,500)

**Reason:**
Query 9 identified 4,413 claims that check these boxes—these are your best possible subrogation opportunities.
These claims:
- Have strong evidence
- Have clear but not one-sided liability
- Have high enough payouts to justify recovery effort
A dedicated routing queue ensures these valuable cases are not overlooked.


###### **3. Build an Accident-Key-Based Subrogation Model**
**Recommendation:**
Use the high-frequency accident categories (accident_keys 11, 10, 8, 3, 7) to predict which accident types historically yield the highest subrogation recoveries.
**Reason:**
Query 7 reveals that a small set of accident_keys accounts for over half of all claims.
If these categories also correlate with frequent subrogation successes, they become the highest ROI areas for resource allocation.
Focusing adjusters and legal resources on the right accident categories increases recovery efficiency.



###### **4. Do NOT Use Demographic Segmentation in Subrogation**
**Recommendation:**
Avoid using income, education, living status, or past claim counts to decide subrogation priority.

**Reason:**
Queries 1–6 show that demographic variables:
- Do NOT affect payout amounts
- Do NOT predict documentation quality
- Do NOT correlate with subrogation likelihood
Using them could introduce bias without improving financial outcomes.
Subrogation success depends on claim-level evidence, not who the policyholder is.
This keeps TriGuard both effective and fair.



###### **5. Implement a “Claim Documentation Score” (0–3 Scale)**
**Recommendation:**
Assign each new claim a score based on:
- Police report filed → +1
- Witness present → +1
- Liability 20–80% → +1
Claims scoring 2 or 3 should be automatically routed to subrogation.

**Reason:**
This simple scoring system directly reflects the conditions that Query 8 and Query 9 show are most predictive of subrogation success.
It:
- Removes guesswork
- Standardizes the process
- Reduces missed recovery opportunities
- Supports automation and scalability



###### **6. Broker & Agent Training on Evidence Collection**
**Recommendation:**
Provide targeted workflows, scripts, or digital forms to brokers and phone agents to help them collect key information during the first notice of loss (FNOL).

**Reason:**
Query 6 shows the Broker channel handles the majority of claims, meaning improvements here have the largest impact.
If agents consistently ask for:
- Witness contact
- Photos
- Third-party details
- Police report confirmation
TriGuard will collect more subrogation-critical evidence early—before it becomes impossible to retrieve later.

Early capture → higher evidence quality → better recovery rate.


###### **7. Prioritize High-Payout, Multi-Evidence Claims for Legal Review**
**Recommendation:**
Allocate more legal or recovery resources towards high-payout claims that have both witness + police report documentation.

**Reason:**
Query 9 shows that verified high-evidence claims often have payouts exceeding $20,000, making them especially worth pursuing.

Legal expenses are fixed, but recovery potential scales with payout.
Focusing on high-value recoverable cases maximizes ROI for the subrogation unit.

## Part5: Machine Learning Pipeline

This part implements a complete machine learning pipeline to predict the likelihood of insurance subrogation. The model ingests data directly from a PostgreSQL database, performs advanced feature engineering, and trains an F1-score-weighted ensemble of GBDT models (LightGBM, XGBoost, and CatBoost).

Initial exploration and prototyping were conducted in Jupyter Notebooks (`.ipynb`). These notebooks have been exported to `.html` for easy reference and review. The final, production-ready pipeline is consolidated in `modeling.py` for automated execution.

### 🚀 Core Methodology

The model's strategy is built upon several key components to maximize the F1 score:

1.  **Direct Database Ingestion:** Connects to a PostgreSQL database using environment variables. It loads and joins five distinct tables: `claim`, `accident`, `policyholder`, `vehicle`, and `driver`.

2.  **Time-Based Validation:** A realistic validation strategy is used. The model trains on all available data **except** for September 2016. September 2016 is held out as the final, unseen test set, simulating a real-world "predict next month" scenario.

3.  **Advanced Feature Engineering:** The `create_enhanced_features_v2` function generates over 150 features, including:
    * **Time-Based:** Claim date/time decomposition (e.g., `is_rush_hour`, `is_weekend`).
    * **Driver & Vehicle:** Calculated features like `age_at_claim` and `period_of_driving`.
    * **Liability:** Extensive engineering on `liab_prct`, creating polynomial, inverse, and binned variations.
    * **Interactions:** High-order features combining liability, evidence, and accident type (e.g., `golden_combo`).
    * **Leakage-Proof:** Uses an `artifacts` dictionary to pass training-set statistics (medians, quantiles) to the test set, preventing data leakage.

4.  **Imbalanced Data Handling:** Employs **SMOTE** (Synthetic Minority Over-sampling TEchnique) within each cross-validation fold to create a more balanced training set without contaminating the validation fold.

5.  **Hyperparameter Optimization (HPO):** Uses **Optuna** to run a dedicated HPO study for the CatBoost model, finding the best parameters specifically for its architecture.

6.  **F1-Weighted Ensemble:** The final prediction is not a simple average. It is a weighted average of three models, where the weights are based on each model's individual Out-of-Fold (OOF) F1 score.

    $Weight_{model} = \frac{F1_{model}}{F1_{LGBM} + F1_{XGB} + F1_{CAT}}$

### 🛠️ How to Run

#### 1. Prerequisites

* Python 3.8+
* Access to the PostgreSQL database.
* Required Python packages (see `requirements.txt`).

#### 2. Dependencies

Install the required libraries. It is highly recommended to use a virtual environment.

```
pip install -r requirements.txt
```

#### 3. Environment Variables
This script **requires** environment variables to connect to the database. Set them in your shell session before running the script.

```
# Example for Linux/macOS
export PGHOST="your-database-host.com"
export PGPORT="5432"
export PGDATABASE="your_database_name"
export PGUSER="your_username"
export PGPASSWORD="your_password"

# Optional: set to "allow" or "prefer" if SSL is not required
export PGSSLMODE="require"
```

#### 4. Execute the Pipeline
Once your environment variables are set and dependencies are installed, simply run the Python script:

```
python modeling.py
```

### 📊 Pipeline Output
The script will execute the full pipeline:

1. Connect to the database and load/merge tables.

2. Split data into Train (all) and Test (Sept 2016).

3. Run the Optuna HPO study for CatBoost (20 trials by default).

4. Run the 5-fold cross-validation to train the F1-weighted ensemble.

5. Print a final report to the console, including:

    - Individual model OOF F1 scores.

    - Calculated model weights.

    - The final Weighted Ensemble OOF F1 score and AUC.

6. Save the test set predictions to `submission.csv`, containing `claim_number` and `subrogation_proba`.