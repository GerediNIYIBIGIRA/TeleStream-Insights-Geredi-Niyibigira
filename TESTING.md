# Testing Guide for TeleStream ETL Pipeline

## Overview
This document describes the testing strategy and how to run tests for the batch transformation pipeline.

## Test Structure

```
tests/
├── unit/
│   └── test_batch_transform.py    # Pure function unit tests
├── integration/
│   └── test_pipeline_e2e.py       # End-to-end pipeline tests
└── performance/
    └── test_load.py                # Performance and load tests
```

## Requirements

### Install Test Dependencies
```bash
pip install pytest pytest-cov pytest-asyncio
pip install pyspark==3.4.0
```

### Optional: Delta Lake Support
```bash
pip install delta-spark==2.4.0
```

## Running Tests

### Run All Unit Tests
```bash
pytest tests/unit/test_batch_transform.py -v
```

### Run Specific Test Classes
```bash
# Test only data quality filters
pytest tests/unit/test_batch_transform.py::TestDataQuality -v

# Test only business logic
pytest tests/unit/test_batch_transform.py::TestCallClassifications -v

# Test only aggregations
pytest tests/unit/test_batch_transform.py::TestAggregations -v
```

### Run Specific Test Functions
```bash
pytest tests/unit/test_batch_transform.py::TestRevenueCategory::test_revenue_category_edge_cases -v
```

### Run with Coverage
```bash
pytest tests/unit/test_batch_transform.py --cov=processing --cov-report=html
```

View coverage report:
```bash
open htmlcov/index.html
```

### Run with Different Verbosity Levels
```bash
# Minimal output
pytest tests/unit/test_batch_transform.py -q

# Verbose output
pytest tests/unit/test_batch_transform.py -v

# Extra verbose with print statements
pytest tests/unit/test_batch_transform.py -vv -s
```

## Test Categories

### 1. Normalization Tests (`TestNormalization`)
- **Purpose**: Test data normalization and hashing
- **Functions Tested**: `normalize_columns()`, `enrich_with_hash()`
- **Run**: `pytest tests/unit/test_batch_transform.py::TestNormalization -v`

### 2. Metadata Tests (`TestMetadata`)
- **Purpose**: Test metadata enrichment
- **Functions Tested**: `add_metadata_columns()`
- **Run**: `pytest tests/unit/test_batch_transform.py::TestMetadata -v`

### 3. Data Quality Tests (`TestDataQuality`)
- **Purpose**: Test data validation and filtering
- **Functions Tested**: `apply_data_quality_filters()`
- **Key Validations**:
  - Null value removal
  - Negative value filtering
  - Duplicate removal
- **Run**: `pytest tests/unit/test_batch_transform.py::TestDataQuality -v`

### 4. Temporal Feature Tests (`TestTemporalFeatures`)
- **Purpose**: Test timestamp-based feature extraction
- **Functions Tested**: `add_temporal_features()`
- **Run**: `pytest tests/unit/test_batch_transform.py::TestTemporalFeatures -v`

### 5. Call Classification Tests (`TestCallClassifications`)
- **Purpose**: Test business logic for call categorization
- **Functions Tested**: `add_call_classifications()`
- **Run**: `pytest tests/unit/test_batch_transform.py::TestCallClassifications -v`

### 6. Revenue Category Tests (`TestRevenueCategory`)
- **Purpose**: Test revenue tier classification
- **Functions Tested**: `add_revenue_category()`
- **Edge Cases**: Boundary values (5.0, 10.0)
- **Run**: `pytest tests/unit/test_batch_transform.py::TestRevenueCategory -v`

### 7. Signal Quality Tests (`TestSignalQuality`)
- **Purpose**: Test network signal classification
- **Functions Tested**: `add_signal_quality()`
- **Categories**: excellent, good, fair, poor
- **Run**: `pytest tests/unit/test_batch_transform.py::TestSignalQuality -v`

### 8. Data Quality Score Tests (`TestDataQualityScore`)
- **Purpose**: Test DQ score calculation
- **Functions Tested**: `calculate_data_quality_score()`
- **Run**: `pytest tests/unit/test_batch_transform.py::TestDataQualityScore -v`

### 9. Aggregation Tests (`TestAggregations`)
- **Purpose**: Test aggregation logic for analytics
- **Functions Tested**: 
  - `aggregate_daily_usage()`
  - `aggregate_network_performance()`
- **Run**: `pytest tests/unit/test_batch_transform.py::TestAggregations -v`

### 10. Integration Tests (`TestIntegration`)
- **Purpose**: Test complete transformation pipeline
- **Run**: `pytest tests/unit/test_batch_transform.py::TestIntegration -v`

### 11. Edge Cases Tests (`TestEdgeCases`)
- **Purpose**: Test boundary conditions
- **Scenarios**: Empty dataframes, zero values
- **Run**: `pytest tests/unit/test_batch_transform.py::TestEdgeCases -v`

## Continuous Integration

The tests are automatically run in CI/CD pipeline via GitHub Actions.

### GitHub Actions Workflow
```yaml
name: Test Pipeline
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov
      - name: Run tests
        run: pytest tests/unit/ --cov=processing --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

## Troubleshooting

### Issue: `TypeError: 'JavaPackage' object is not callable`
**Solution**: This is a Java/PySpark compatibility issue.

```bash
# Option 1: Check Java version (need Java 8 or 11)
java -version

# Option 2: Reinstall PySpark
pip uninstall pyspark
pip install pyspark==3.4.0

# Option 3: Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

### Issue: Delta Lake warnings
**Solution**: Delta Lake is optional for unit tests.

```bash
# Tests will work without Delta Lake
# To suppress warnings, remove Delta configs from test fixtures
```

### Issue: Tests run slowly
**Solution**: Use local[1] mode and disable UI

```python
spark = SparkSession.builder \
    .master("local[1]") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()
```

## Test Coverage Goals

- **Target Coverage**: 85%+
- **Critical Paths**: 100% coverage for:
  - Data quality filters
  - Business logic transformations
  - Aggregation calculations

## Best Practices

1. **Isolate Tests**: Each test should be independent
2. **Use Fixtures**: Reuse common test data via fixtures
3. **Test Edge Cases**: Always test boundary conditions
4. **Mock External Dependencies**: Use mocks for S3, databases
5. **Fast Tests**: Keep unit tests fast (<5 seconds per test)
6. **Clear Assertions**: Use descriptive assertion messages

## Example Test Run Output

```bash
$ pytest tests/unit/test_batch_transform.py -v

tests/unit/test_batch_transform.py::TestNormalization::test_normalize_columns PASSED
tests/unit/test_batch_transform.py::TestNormalization::test_enrich_with_hash PASSED
tests/unit/test_batch_transform.py::TestDataQuality::test_apply_data_quality_filters_removes_nulls PASSED
tests/unit/test_batch_transform.py::TestDataQuality::test_apply_data_quality_filters_removes_negatives PASSED
tests/unit/test_batch_transform.py::TestCallClassifications::test_add_call_classifications PASSED
tests/unit/test_batch_transform.py::TestRevenueCategory::test_add_revenue_category PASSED
tests/unit/test_batch_transform.py::TestSignalQuality::test_signal_quality_all_categories PASSED
tests/unit/test_batch_transform.py::TestAggregations::test_aggregate_daily_usage PASSED

========================= 20 passed in 15.2s =========================
```

## Next Steps

After unit tests pass:
1. Run integration tests with real data
2. Perform load testing with large datasets
3. Test in staging environment
4. Monitor test coverage metrics in CI/CD