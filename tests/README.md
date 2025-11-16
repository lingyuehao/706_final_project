# Test Suite Documentation

This directory contains comprehensive tests for the TriGuard Insurance Subrogation Prediction System.

## Test Structure

```
tests/
├── __init__.py                    # Test package initialization
├── conftest.py                    # Pytest fixtures and configuration
├── test_modeling.py               # Tests for modeling pipeline
├── test_data_utils.py            # Tests for data loading and utilities
├── test_analysis_scripts.py      # Tests for analysis scripts
├── test_system_pipeline.py       # Integration tests for full pipeline
├── test_images.py                # Tests for image file validation
└── README.md                     # This file
```

## Running Tests

### Run All Tests
```bash
pytest tests/ -v
```

### Run Fast Tests Only (skip slow and database tests)
```bash
pytest tests/ -v -m "not slow and not database"
```

Or use the Makefile:
```bash
make test-fast
```

### Run Specific Test Categories

**Unit Tests:**
```bash
pytest tests/ -v -m unit
```

**Integration Tests:**
```bash
pytest tests/ -v -m integration
```

**Modeling Tests:**
```bash
pytest tests/test_modeling.py -v
```

**Analysis Script Tests:**
```bash
pytest tests/test_analysis_scripts.py -v
```

**Image Validation Tests:**
```bash
pytest tests/test_images.py -v
```

## Test Coverage

Generate coverage report:
```bash
pytest tests/ --cov=scripts --cov=analysis --cov-report=html
```

Or use the Makefile:
```bash
make coverage
```

View coverage report by opening `htmlcov/index.html` in a browser.

## Test Markers

Tests can be marked with the following markers:

- `@pytest.mark.slow` - Slow tests that take significant time
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.unit` - Unit tests
- `@pytest.mark.analysis` - Tests for analysis scripts
- `@pytest.mark.modeling` - Tests for modeling pipeline
- `@pytest.mark.images` - Tests for image validation
- `@pytest.mark.database` - Tests requiring database connection

Example:
```python
@pytest.mark.slow
@pytest.mark.integration
def test_full_pipeline():
    # Test implementation
    pass
```

## Test Fixtures

Common fixtures are defined in `conftest.py`:

### Data Fixtures
- `sample_claim_data` - Sample claim data as pandas DataFrame
- `sample_polars_claim_data` - Sample claim data as Polars DataFrame
- `sample_accident_data` - Sample accident data
- `sample_driver_data` - Sample driver data
- `sample_vehicle_data` - Sample vehicle data
- `sample_policyholder_data` - Sample policyholder data

### Utility Fixtures
- `sample_data_dir` - Path to sample data directory
- `temp_csv_dir` - Temporary directory for CSV files
- `mock_db_environment` - Mock database environment variables
- `sample_image_paths` - Sample image file paths

## Writing New Tests

### Test Naming Convention
- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

### Example Test

```python
import pytest

class TestFeatureEngineering:
    """Test suite for feature engineering"""
    
    def test_create_features(self, sample_claim_data):
        """Test basic feature creation"""
        result = create_features(sample_claim_data)
        
        assert 'new_feature' in result.columns
        assert len(result) == len(sample_claim_data)
    
    @pytest.mark.slow
    def test_expensive_operation(self):
        """Test expensive operation"""
        result = expensive_operation()
        assert result is not None
```

## Continuous Integration

Tests run automatically on:
- Push to main/master/develop branches
- Pull requests
- Scheduled nightly runs

### CI Workflows

1. **Main CI Pipeline** (`.github/workflows/ci.yml`)
   - Runs on push and PR
   - Tests on Python 3.9, 3.10, 3.11
   - Includes linting and security checks

2. **Scheduled Tests** (`.github/workflows/test-on-schedule.yml`)
   - Runs daily at 2 AM UTC
   - Includes slow tests

3. **Code Quality** (`.github/workflows/code-quality.yml`)
   - Runs on pull requests
   - Checks formatting and complexity

## Common Issues

### Issue: Tests fail with database connection errors
**Solution:** Skip database tests using:
```bash
pytest tests/ -m "not database"
```

### Issue: Slow test execution
**Solution:** Run fast tests only:
```bash
make test-fast
```

### Issue: Import errors
**Solution:** Ensure all dependencies are installed:
```bash
pip install -r requirements.txt
```

### Issue: Image tests fail
**Solution:** Ensure Pillow is installed:
```bash
pip install Pillow>=10.0.0
```

## Best Practices

1. **Keep tests independent** - Each test should be able to run independently
2. **Use fixtures** - Reuse common test data through fixtures
3. **Mark slow tests** - Use `@pytest.mark.slow` for time-consuming tests
4. **Mock external dependencies** - Mock database connections, API calls, etc.
5. **Test edge cases** - Include tests for boundary conditions and error cases
6. **Document tests** - Add docstrings explaining what each test validates

## Test Data

Test data is generated using fixtures in `conftest.py`. Key features:
- Reproducible (using fixed random seeds)
- Realistic distributions
- Covers edge cases
- No external dependencies

## Maintenance

### Adding New Tests
1. Create test file following naming convention
2. Import necessary fixtures from `conftest.py`
3. Use appropriate test markers
4. Add documentation

### Updating Fixtures
1. Modify `conftest.py`
2. Ensure backward compatibility
3. Update dependent tests if needed
4. Document changes

### Test Coverage Goals
- Maintain >80% code coverage
- 100% coverage for critical paths
- All public APIs must be tested

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Pytest Fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [Pytest Markers](https://docs.pytest.org/en/stable/mark.html)
- [Coverage.py](https://coverage.readthedocs.io/)

