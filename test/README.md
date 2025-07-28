# Test Directory

This directory contains test files and testing utilities for the data lake project.

## Purpose

Ensures code quality, reliability, and functionality through comprehensive testing of data processing pipelines and transformations.

## Structure

```
test/
├── unit/                    # Unit tests for individual functions
├── integration/             # Integration tests for pipelines
├── data/                    # Test data files
└── utils/                   # Testing utilities
```

## Testing Strategy

### Unit Tests
Test individual functions and components in isolation.

#### Transformation Functions
```python
def test_clean_null_dates():
    """Test date cleaning function"""
    # Test with null dates
    # Test with valid dates
    # Test with empty strings
```

#### Data Quality Functions
```python
def test_clean_null_numbers():
    """Test numeric value cleaning"""
    # Test null numbers
    # Test valid numbers
    # Test edge cases
```

### Integration Tests
Test complete data processing pipelines.

#### Pipeline Tests
```python
def test_bronze_to_silver_pipeline():
    """Test complete bronze to silver transformation"""
    # Test end-to-end pipeline
    # Verify output data quality
    # Check partitioning
```

#### Data Quality Tests
```python
def test_data_quality_rules():
    """Test data quality enforcement"""
    # Test null handling
    # Test date validation
    # Test type conversion
```

## Test Data

### Sample Data
- **Small datasets**: For fast unit tests
- **Synthetic data**: Generated for testing
- **Edge cases**: Data with quality issues

### Test Scenarios
1. **Normal Data**: Valid, clean data
2. **Null Values**: Missing data handling
3. **Invalid Dates**: Date parsing errors
4. **Type Mismatches**: Data type issues
5. **Large Datasets**: Performance testing

## Running Tests

### Unit Tests
```bash
# Run all unit tests
python -m pytest test/unit/

# Run specific test file
python -m pytest test/unit/test_transformations.py

# Run with coverage
python -m pytest --cov=jobs test/unit/
```

### Integration Tests
```bash
# Run integration tests
python -m pytest test/integration/

# Run with Spark
python -m pytest test/integration/test_pipeline.py
```

## Test Utilities

### Mock Data
```python
def create_test_employee_data():
    """Create test employee data"""
    return [
        {"id": "EMP001", "name": "John Doe", "salary": 50000},
        {"id": "EMP002", "name": "Jane Smith", "salary": 60000}
    ]
```

### Assertion Helpers
```python
def assert_data_quality(df):
    """Assert data quality rules"""
    assert df.filter(col("id").isNull()).count() == 0
    assert df.filter(col("salary") < 0).count() == 0
```

## Continuous Integration

### GitHub Actions
```yaml
# .github/workflows/test.yml
- name: Run Tests
  run: |
    python -m pytest test/
    python -m pytest --cov=jobs --cov-report=xml
```

### Quality Gates
- **Test Coverage**: Minimum 80%
- **Code Quality**: No critical issues
- **Performance**: Tests complete within time limits

## Best Practices

### Test Design
- **Isolation**: Tests should be independent
- **Repeatability**: Tests should be deterministic
- **Completeness**: Cover all code paths
- **Maintainability**: Easy to understand and modify

### Test Data Management
- **Small datasets**: For fast execution
- **Realistic data**: Mimic production scenarios
- **Version control**: Track test data changes
- **Cleanup**: Remove test artifacts

## Future Enhancements

### Planned Tests
- **Performance Tests**: Load testing for large datasets
- **Security Tests**: Data access and encryption
- **API Tests**: REST API endpoints
- **UI Tests**: Web interface testing 