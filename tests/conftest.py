"""
Pytest configuration and fixtures for the test suite
"""

import pytest
import pandas as pd
import polars as pl
import numpy as np
from pathlib import Path
import tempfile


@pytest.fixture
def sample_data_dir():
    """Fixture providing path to sample data directory"""
    return Path(__file__).parent.parent / "data" / "tri_guard_5_py_clean"


@pytest.fixture
def sample_claim_data():
    """Fixture providing sample claim data as pandas DataFrame"""
    np.random.seed(42)
    n = 100

    data = {
        "claim_number": [f"CLM{i:05d}" for i in range(n)],
        "claim_date": pd.date_range("2016-01-01", periods=n, freq="D"),
        "accident_key": np.random.randint(1, 13, n),
        "policyholder_key": np.random.randint(1, 100, n),
        "vehicle_key": np.random.randint(1, 100, n),
        "driver_key": np.random.randint(1, 100, n),
        "subrogation": np.random.choice([0, 1], n, p=[0.75, 0.25]),
        "claim_est_payout": np.random.uniform(1000, 25000, n),
        "liab_prct": np.random.uniform(0, 100, n),
        "witness_present_ind": np.random.choice(["Y", "N"], n),
        "policy_report_filed_ind": np.random.choice([0, 1], n),
        "in_network_bodyshop": np.random.choice(["yes", "no"], n),
        "channel": np.random.choice(["Broker", "Phone", "Online"], n),
    }

    return pd.DataFrame(data)


@pytest.fixture
def sample_polars_claim_data():
    """Fixture providing sample claim data as Polars DataFrame"""
    np.random.seed(42)
    n = 100

    return pl.DataFrame(
        {
            "claim_number": [f"CLM{i:05d}" for i in range(n)],
            "accident_key": np.random.randint(1, 13, n),
            "subrogation": np.random.choice([0, 1], n, p=[0.75, 0.25]),
            "claim_est_payout": np.random.uniform(1000, 25000, n),
            "liab_prct": np.random.uniform(0, 100, n),
            "witness_present_ind": np.random.choice(["Y", "N"], n),
            "policy_report_filed_ind": np.random.choice(["0", "1"], n),
            "in_network_bodyshop": np.random.choice(["yes", "no"], n),
            "channel": np.random.choice(["Broker", "Phone", "Online"], n),
        }
    )


@pytest.fixture
def sample_accident_data():
    """Fixture providing sample accident data"""
    return pd.DataFrame(
        {
            "accident_key": range(1, 13),
            "accident_type": np.random.choice(
                ["single_car", "multi_vehicle_clear", "multi_vehicle_unclear"], 12
            ),
            "accident_site": np.random.choice(
                ["Highway", "Intersection", "Parking Area", "Local"], 12
            ),
        }
    )


@pytest.fixture
def sample_driver_data():
    """Fixture providing sample driver data"""
    return pd.DataFrame(
        {
            "driver_key": range(1, 101),
            "year_of_born": np.random.randint(1950, 2000, 100),
            "age_of_DL": np.random.randint(16, 25, 100),
            "gender": np.random.choice(["M", "F"], 100),
            "safety_rating": np.random.uniform(50, 100, 100),
        }
    )


@pytest.fixture
def sample_vehicle_data():
    """Fixture providing sample vehicle data"""
    return pd.DataFrame(
        {
            "vehicle_key": range(1, 101),
            "vehicle_price": np.random.uniform(15000, 50000, 100),
            "vehicle_mileage": np.random.uniform(0, 150000, 100),
            "vehicle_weight": np.random.uniform(2500, 5000, 100),
            "vehicle_category": np.random.choice(["Compact", "Medium", "Large"], 100),
            "vehicle_color": np.random.choice(["silver", "black", "red", "white"], 100),
            "vehicle_made_year": np.random.randint(2000, 2017, 100),
        }
    )


@pytest.fixture
def sample_policyholder_data():
    """Fixture providing sample policyholder data"""
    return pd.DataFrame(
        {
            "policyholder_key": range(1, 101),
            "annual_income": np.random.uniform(20000, 150000, 100),
            "past_num_of_claims": np.random.randint(0, 10, 100),
            "high_education_ind": np.random.choice([0, 1], 100),
            "address_change_ind": np.random.choice([0, 1], 100),
            "living_status": np.random.choice(["Own", "Rent"], 100),
            "zip_code": [str(np.random.randint(10000, 99999)) for _ in range(100)],
        }
    )


@pytest.fixture
def temp_csv_dir():
    """Fixture providing temporary directory for CSV files"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_db_environment(monkeypatch):
    """Fixture to mock database environment variables"""
    monkeypatch.setenv("PGHOST", "localhost")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "test_db")
    monkeypatch.setenv("PGUSER", "test_user")
    monkeypatch.setenv("PGPASSWORD", "test_pass")
    monkeypatch.setenv("PGSSLMODE", "disable")


@pytest.fixture
def sample_image_paths(tmp_path):
    """Fixture providing paths to sample image files"""
    from PIL import Image

    # Create sample PNG
    png_path = tmp_path / "test_image.png"
    img = Image.new("RGB", (100, 100), color="red")
    img.save(png_path)

    # Create sample JPEG
    jpg_path = tmp_path / "test_image.jpg"
    img = Image.new("RGB", (100, 100), color="blue")
    img.save(jpg_path)

    return {"png": png_path, "jpg": jpg_path, "dir": tmp_path}
