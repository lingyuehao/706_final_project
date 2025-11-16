# Makefile for TriGuard Insurance Subrogation Prediction System

.PHONY: help install test lint format clean coverage run-analysis run-model

help:
	@echo "Available targets:"
	@echo "  install          - Install all dependencies"
	@echo "  test             - Run all tests"
	@echo "  test-fast        - Run fast tests only (skip slow and database tests)"
	@echo "  test-unit        - Run unit tests only"
	@echo "  test-integration - Run integration tests"
	@echo "  test-analysis    - Run analysis script tests"
	@echo "  test-modeling    - Run modeling tests"
	@echo "  test-images      - Run image validation tests"
	@echo "  lint             - Run linting checks"
	@echo "  format           - Auto-format code"
	@echo "  coverage         - Generate coverage report"
	@echo "  clean            - Remove generated files"
	@echo "  run-analysis     - Run analysis scripts"
	@echo "  run-model        - Run modeling pipeline"

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v --tb=short

test-fast:
	pytest tests/ -v --tb=short -m "not slow and not database"

test-unit:
	pytest tests/ -v --tb=short -m unit

test-integration:
	pytest tests/ -v --tb=short -m integration

test-analysis:
	pytest tests/test_analysis_scripts.py -v

test-modeling:
	pytest tests/test_modeling.py -v -m "not slow"

test-images:
	pytest tests/test_images.py -v

lint:
	@echo "Running flake8..."
	flake8 scripts/ analysis/ tests/ --max-line-length=127 --extend-ignore=E203,W503 || true
	@echo "\nRunning pylint..."
	pylint scripts/ --disable=all --enable=E,F --exit-zero || true

format:
	@echo "Running black..."
	black scripts/ analysis/ tests/
	@echo "\nRunning isort..."
	isort scripts/ analysis/ tests/

coverage:
	pytest tests/ --cov=scripts --cov=analysis --cov-report=html --cov-report=term-missing --cov-report=xml
	@echo "\nCoverage report generated in htmlcov/index.html"

clean:
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf *.pyc
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

run-analysis:
	@echo "Running analysis scripts..."
	python analysis/tina_accident/polar.py
	python analysis/brynn_policyholder/polar.py
	python analysis/lingyue_vehicle/polar.py
	python analysis/bruce_driver/drivers_polar.py

run-model:
	@echo "Running modeling pipeline..."
	python scripts/modeling.py

# Docker-related targets
docker-build:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-test:
	docker-compose run --rm dev pytest tests/ -v

# CI-related targets
ci-test:
	pytest tests/ -v --tb=short -m "not database" --cov=scripts --cov=analysis --cov-report=xml

ci-lint:
	flake8 scripts/ analysis/ tests/ --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 scripts/ analysis/ tests/ --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

# Development helpers
install-dev:
	pip install -r requirements.txt
	pip install black isort flake8 pylint mypy pytest-watch

watch-test:
	pytest-watch tests/ -v

check:
	@echo "Running all checks..."
	@$(MAKE) lint
	@$(MAKE) test-fast
	@echo "\nAll checks completed!"
