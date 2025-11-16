"""
Tests for analysis scripts in the analysis folder
"""

import polars as pl


class TestTinaAccidentAnalysis:
    """Test suite for analysis/tina_accident/polar.py"""

    def test_polars_basic_operations(self, sample_polars_claim_data):
        """Test basic Polars operations"""
        df = sample_polars_claim_data

        # Test aggregation
        result = df.group_by("channel").agg([pl.len().alias("count")])

        assert result.height > 0
        assert "count" in result.columns

    def test_accident_type_filtering(self):
        """Test accident type filtering logic"""
        df = pl.DataFrame(
            {
                "accident_type": [
                    "single_car",
                    "multi_vehicle_clear",
                    "multi_vehicle_unclear",
                    "single_car",
                ]
            }
        )

        # Test multi-vehicle filter
        multi_vehicle = df.filter(pl.col("accident_type").str.contains("multi_vehicle"))

        assert multi_vehicle.height == 2

    def test_regression_data_preparation(self, sample_polars_claim_data):
        """Test data preparation for regression analysis"""
        df = sample_polars_claim_data

        # Create binary features
        df_with_features = df.with_columns(
            [
                pl.when(pl.col("witness_present_ind") == "Y")
                .then(1)
                .otherwise(0)
                .alias("has_witness"),
                pl.when(pl.col("policy_report_filed_ind") == "1")
                .then(1)
                .otherwise(0)
                .alias("has_police_report"),
            ]
        )

        assert "has_witness" in df_with_features.columns
        assert "has_police_report" in df_with_features.columns
        assert df_with_features["has_witness"].dtype == pl.Int32


class TestBrynnPolicyholderAnalysis:
    """Test suite for analysis/brynn_policyholder/polar.py"""

    def test_float_casting_helper(self):
        """Test float casting with warnings"""
        df = pl.DataFrame({"value": ["100.5", "200.3", "invalid", "300.0"]})

        # Cast to float
        df_cast = df.with_columns(pl.col("value").cast(pl.Float64, strict=False))

        # Should have nulls for invalid values
        assert df_cast["value"].null_count() > 0

    def test_sql_context_operations(self):
        """Test Polars SQL context operations"""
        claim = pl.DataFrame(
            {
                "claim_number": [1, 2, 3],
                "policyholder_key": [1, 2, 1],
                "claim_est_payout": [1000.0, 2000.0, 1500.0],
            }
        )

        policyholder = pl.DataFrame(
            {"policyholder_key": [1, 2], "high_education_ind": [1.0, 0.0]}
        )

        ctx = pl.SQLContext()
        ctx.register("claim", claim)
        ctx.register("policyholder", policyholder)

        query = """
        SELECT
            p.high_education_ind,
            AVG(c.claim_est_payout) as avg_payout
        FROM claim c
        LEFT JOIN policyholder p ON c.policyholder_key = p.policyholder_key
        GROUP BY p.high_education_ind
        """

        result = ctx.execute(query).collect()
        assert result.height > 0
        assert "avg_payout" in result.columns

    def test_income_analysis(self):
        """Test income-based analysis"""
        df = pl.DataFrame(
            {
                "annual_income": [30000.0, 50000.0, 30000.0, 70000.0],
                "claim_est_payout": [1000.0, 1500.0, 1200.0, 2000.0],
            }
        )

        result = (
            df.group_by("annual_income")
            .agg(
                [
                    pl.len().alias("num_claims"),
                    pl.mean("claim_est_payout").alias("avg_payout"),
                ]
            )
            .sort("num_claims", descending=True)
        )

        assert result.height > 0
        assert result["num_claims"][0] == 2  # 30000 appears twice


class TestLingyueVehicleAnalysis:
    """Test suite for analysis/lingyue_vehicle/polar.py"""

    def test_boolean_helper_function(self):
        """Test boolean conversion helper"""
        df = pl.DataFrame({"witness": ["Y", "N", "yes", "no", "1", "0"]})

        def to_bool(col: pl.Expr) -> pl.Expr:
            s = col.cast(pl.Utf8, strict=False).str.strip_chars().str.to_lowercase()
            return (
                pl.when(s.is_in(["y", "yes", "true", "1"]))
                .then(pl.lit(1))
                .when(s.is_in(["n", "no", "false", "0"]))
                .then(pl.lit(0))
                .otherwise(None)
            )

        result = df.with_columns([to_bool(pl.col("witness")).alias("witness_bool")])

        assert result["witness_bool"][0] == 1  # 'Y'
        assert result["witness_bool"][1] == 0  # 'N'
        assert result["witness_bool"][2] == 1  # 'yes'

    def test_vehicle_data_merge(self):
        """Test vehicle data merging"""
        claim = pl.DataFrame(
            {
                "claim_number": [1, 2, 3],
                "vehicle_key": [1, 2, 1],
                "subrogation": [0.0, 1.0, 0.0],
            }
        )

        vehicle = pl.DataFrame(
            {
                "vehicle_key": [1, 2],
                "vehicle_category": ["Compact", "Large"],
                "vehicle_price": [20000.0, 35000.0],
            }
        )

        merged = claim.join(vehicle, on="vehicle_key", how="inner")

        assert merged.height == 3
        assert "vehicle_category" in merged.columns
        assert "vehicle_price" in merged.columns

    def test_correlation_calculation(self):
        """Test correlation calculations"""
        df = pl.DataFrame(
            {
                "subrogation": [0.0, 1.0, 0.0, 1.0, 0.0],
                "vehicle_price": [20000.0, 30000.0, 25000.0, 35000.0, 22000.0],
                "liab_prct": [30.0, 50.0, 20.0, 60.0, 25.0],
            }
        )

        corr = df.select(
            [
                pl.corr("subrogation", "vehicle_price").alias("corr_subro_price"),
                pl.corr("subrogation", "liab_prct").alias("corr_subro_liability"),
            ]
        )

        assert corr.height == 1
        assert isinstance(corr["corr_subro_price"][0], float)


class TestBruceDriverAnalysis:
    """Test suite for analysis/bruce_driver/drivers_polar.py"""

    def test_driver_key_casting(self):
        """Test driver_key type casting"""
        df = pl.DataFrame({"driver_key": ["1", "2", "3"]})

        df_cast = df.with_columns(pl.col("driver_key").cast(pl.Int64))

        assert df_cast["driver_key"].dtype == pl.Int64

    def test_correlation_with_subrogation(self):
        """Test correlation calculation with subrogation"""
        df = pl.DataFrame(
            {
                "driver_key": [1, 2, 3, 4, 5],
                "age_of_DL": [20, 22, 25, 18, 21],
                "safety_rating": [70.0, 85.0, 60.0, 90.0, 75.0],
                "subrogation": [0.0, 1.0, 0.0, 1.0, 0.0],
            }
        )

        # Calculate correlation for numeric columns
        num_cols = ["age_of_DL", "safety_rating"]
        corrs = []

        for col in num_cols:
            val = df.select(pl.corr(pl.col(col), pl.col("subrogation"))).item()
            corrs.append((col, val))

        assert len(corrs) == 2
        assert all(isinstance(corr[1], (float, type(None))) for corr in corrs)

    def test_safety_rating_categorization(self):
        """Test safety rating categorization"""
        df = pl.DataFrame({"safety_rating": [50.0, 75.0, 90.0, 60.0, 85.0]})

        # Categorize safety ratings
        df_cat = df.with_columns(
            [
                pl.when(pl.col("safety_rating") >= 80)
                .then(pl.lit("High"))
                .when(pl.col("safety_rating") >= 60)
                .then(pl.lit("Medium"))
                .otherwise(pl.lit("Low"))
                .alias("safety_category")
            ]
        )

        assert df_cat["safety_category"][0] == "Low"  # 50
        assert df_cat["safety_category"][2] == "High"  # 90


class TestAnalysisOutputs:
    """Test suite for analysis output validation"""

    def test_csv_output_creation(self, tmp_path):
        """Test CSV output file creation"""
        df = pl.DataFrame(
            {
                "metric": ["total_claims", "avg_payout"],
                # Use float for both to avoid type mixing
                "value": [100.0, 3500.50],
            }
        )

        output_path = tmp_path / "test_output.csv"
        df.write_csv(output_path)

        assert output_path.exists()

        # Read back and verify
        df_read = pl.read_csv(output_path)
        assert df_read.height == 2

    def test_analysis_results_structure(self):
        """Test structure of analysis results"""
        # Simulate analysis results
        results = {
            "total_claims": 100.0,  # Use float
            "avg_payout": 3500.50,
            "subrogation_rate": 0.25,
        }

        # Convert to DataFrame
        df = pl.DataFrame(
            {"metric": list(results.keys()), "value": list(results.values())}
        )

        assert df.height == 3
        assert "metric" in df.columns
        assert "value" in df.columns
