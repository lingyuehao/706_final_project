# CI/CD Documentation

This directory contains GitHub Actions workflows for Continuous Integration and Continuous Deployment.

## Workflows

### 1. Main CI Pipeline (`ci.yml`)

**Trigger:** Push to main/master/develop, Pull Requests

**Jobs:**
- **test**: Run test suite on Python 3.9, 3.10, 3.11
- **lint**: Code quality checks (Black, isort, Flake8, Pylint)
- **security**: Security scans (Bandit, Safety)
- **test-analysis-scripts**: Test analysis scripts
- **test-modeling**: Test modeling pipeline
- **test-images**: Validate image files
- **build-summary**: Summarize all job results

**Features:**
- Matrix testing across Python versions
- Comprehensive test coverage
- Code coverage reporting to Codecov
- Artifact uploads for coverage reports

### 2. Scheduled Tests (`test-on-schedule.yml`)

**Trigger:** Daily at 2 AM UTC, Manual dispatch

**Purpose:** Run comprehensive tests including slow tests

**Features:**
- Full test suite execution
- HTML test report generation
- Nightly test artifact uploads
- Failure notifications

### 3. Code Quality Checks (`code-quality.yml`)

**Trigger:** Pull Requests, Manual dispatch

**Jobs:**
- **format-check**: Verify code formatting (Black, isort)
- **complexity-check**: Analyze code complexity (Radon)
- **documentation-check**: Check docstring coverage (Interrogate, pydocstyle)

**Features:**
- Automated formatting suggestions
- Complexity metrics
- Documentation coverage reporting

## Configuration

### Environment Variables

The CI workflows may require the following environment variables (if testing with real database):

```yaml
env:
  PGHOST: ${{ secrets.PGHOST }}
  PGPORT: ${{ secrets.PGPORT }}
  PGDATABASE: ${{ secrets.PGDATABASE }}
  PGUSER: ${{ secrets.PGUSER }}
  PGPASSWORD: ${{ secrets.PGPASSWORD }}
```

**Note:** Database tests are skipped by default in CI.

### Secrets Setup

If you need to add secrets to your repository:

1. Go to Settings → Secrets and variables → Actions
2. Add new repository secrets:
   - `PGHOST`
   - `PGPORT`
   - `PGDATABASE`
   - `PGUSER`
   - `PGPASSWORD`

### Branch Protection

Recommended branch protection rules for `main`:

- [x] Require pull request reviews before merging
- [x] Require status checks to pass before merging
  - CI Pipeline / test
  - CI Pipeline / lint
  - Code Quality / format-check
- [x] Require branches to be up to date before merging
- [x] Include administrators

## Workflow Status Badges

Add these badges to your README.md:

```markdown
![CI Pipeline](https://github.com/YOUR_USERNAME/YOUR_REPO/workflows/CI%20Pipeline/badge.svg)
![Code Quality](https://github.com/YOUR_USERNAME/YOUR_REPO/workflows/Code%20Quality%20Checks/badge.svg)
[![codecov](https://codecov.io/gh/YOUR_USERNAME/YOUR_REPO/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_USERNAME/YOUR_REPO)
```

## Local Testing

Before pushing, run tests locally:

```bash
# Fast tests
make test-fast

# Full test suite
make test

# With coverage
make coverage

# Linting
make lint

# Auto-format
make format
```

## Customization

### Modify Python Versions

Edit the matrix in `ci.yml`:

```yaml
strategy:
  matrix:
    python-version: ['3.9', '3.10', '3.11', '3.12']
```

### Add New Test Jobs

1. Create new job in `ci.yml`:
```yaml
new-test-job:
  name: New Test Category
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Run new tests
      run: pytest tests/test_new.py -v
```

2. Add to `build-summary` needs:
```yaml
needs: [test, lint, security, new-test-job]
```

### Modify Test Markers

Edit test commands to include/exclude markers:

```yaml
- name: Run specific tests
  run: |
    pytest tests/ -v -m "unit and not slow"
```

## Troubleshooting

### Tests Fail in CI but Pass Locally

**Common causes:**
- Python version differences
- Missing environment variables
- Different dependency versions
- Operating system differences

**Solutions:**
1. Test locally with same Python version as CI
2. Use `tox` for multi-version testing
3. Check GitHub Actions logs for specific errors

### Slow CI Builds

**Optimization strategies:**
1. Cache pip dependencies:
```yaml
- uses: actions/setup-python@v4
  with:
    cache: 'pip'
```

2. Parallelize tests:
```bash
pytest tests/ -n auto
```

3. Skip slow tests in main CI:
```bash
pytest tests/ -m "not slow"
```

### Coverage Reports Not Uploading

**Check:**
1. Codecov token is set in secrets
2. `coverage.xml` is generated
3. Codecov action is properly configured

## Maintenance

### Regular Updates

- Update action versions quarterly
- Review and update Python versions
- Monitor security advisories
- Update dependency versions

### Best Practices

1. **Keep workflows DRY** - Use composite actions for repeated steps
2. **Fail fast** - Run quick checks first
3. **Clear naming** - Use descriptive job and step names
4. **Documentation** - Document custom workflows
5. **Security** - Never hardcode secrets

## Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Workflow Syntax](https://docs.github.com/en/actions/reference/workflow-syntax-for-github-actions)
- [Codecov Documentation](https://docs.codecov.com/)

