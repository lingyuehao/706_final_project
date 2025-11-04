# AutoRisk Subro

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
PGHOST=
PGUSER=
PGPASSWORD=
PGDATABASE=
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


## Part 2: Data Normalization Process

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

    - Host: The RDS endpoint (e.g., [your-rds-endpoint].amazonaws.com)

    - Port: 5432

    - Database: jet2_holiday

    - Username: The PGUSER

    - Password: The PGPASSWORD

3. SSL tab:

    - Mode: require

4. Test Connection → Finish.

### Common Issues

- Auth/timeout: Ensure the RDS security group allows the current client IP address on port 5432.

- SSL errors: Keep sslmode=require in all connections.

- Notebook cannot connect: Confirm the container has the env vars (echo $PGHOST) and that psql works from the VS Code terminal.

### Next Steps

1. Connect to the database using the snippets in Notebook or Dbeaver.

2. tbc