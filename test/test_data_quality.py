import pytest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)
from pyspark.sql.functions import col

# Add project path to import real functions
sys.path.append('/opt/spark/apps')

# Import real data quality function from project
from utils.spark_utils import check_null_percentage

@pytest.fixture(scope="session", autouse=True)
def spark():
    spark = SparkSession.builder.appName("test_data_quality_real").getOrCreate()
    yield spark
    spark.stop()

def test_data_quality_function(spark):
    """Test using the real data quality function from the project"""
    
    # Create test data with ALL types of missing data
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", FloatType(), True)
    ])
    
    # Data with all possible missing data scenarios
    test_data = [
        ("EMP001", "John Doe", "john@company.com", "IT", 5000.0),           # Complete
        ("EMP002", None, "jane@company.com", "HR", 6000.0),                 # NULL
        ("EMP003", "", "bob@company.com", "IT", 7000.0),                    # Empty string
        ("EMP004", "   ", "alice@company.com", "Finance", 6500.0),          # Whitespace only
        ("EMP005", "nan", "charlie@company.com", "IT", 5500.0),             # "nan"
        ("EMP006", "NaN", "david@company.com", "HR", 4800.0),               # "NaN"
        ("EMP007", "null", "eve@company.com", "IT", 5200.0),                # "null"
        ("EMP008", "NULL", "frank@company.com", "Finance", 7500.0),         # "NULL"
        ("EMP009", "None", "grace@company.com", "IT", 5800.0),              # "None"
        ("EMP010", "N/A", "henry@company.com", "HR", 6200.0),               # "N/A"
        ("EMP011", "n/a", "iris@company.com", "IT", 5400.0),                # "n/a"
        ("EMP012", "[]", "jack@company.com", "Finance", 6800.0),            # Empty array
        ("EMP013", "{}", "kate@company.com", "IT", 5900.0),                 # Empty object
        ("EMP014", "undefined", "leo@company.com", "HR", 6100.0),           # "undefined"
        ("EMP015", "UNDEFINED", "maya@company.com", "IT", 5300.0)           # "UNDEFINED"
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    print("Testing real data quality function with all missing data types...")
    print(f"Total records: {df.count()}")
    print(f"Total columns: {len(df.columns)}")
    
    # Test the real function on each column
    for column in df.columns:
        print(f"\n--- Testing column: {column} ---")
        
        # Use the real function from the project
        result = check_null_percentage(
            df, 
            column, 
            red_threshold=30.0,  # Red if > 30% missing
            yellow_threshold=10.0  # Yellow if > 10% missing
        )
        
        print(f"  Total records: {result['total_count']}")
        print(f"  Missing records: {result['null_count']}")
        print(f"  Missing percentage: {result['null_percentage']:.1f}%")
        print(f"  Quality flag: {result['flag']}")
        
        # Basic assertions for data quality function
        assert result["total_count"] == 15, f"Total count should be 15 for {column}"
        assert result["null_count"] >= 0, f"Null count should be >= 0 for {column}"
        assert result["null_percentage"] >= 0.0, f"Null percentage should be >= 0 for {column}"
        assert result["null_percentage"] <= 100.0, f"Null percentage should be <= 100 for {column}"
        assert result["flag"] in ["Green", "Yellow", "Red"], f"Flag should be Green/Yellow/Red for {column}"
        
        # Verify the function works correctly
        if column == "name":
            # Name column has 14 missing values out of 15 (93.33%)
            assert result["null_count"] == 14, f"Name should have 14 missing values, but got {result['null_count']}"
            assert result["null_percentage"] == 93.33, f"Name should have 93.33% missing, but got {result['null_percentage']}%"
            assert result["flag"] == "Red", f"Name should be Red flag (>30% missing)"
        
        elif column in ["id", "email", "department", "salary"]:
            # Other columns have no missing values
            assert result["null_count"] == 0, f"{column} should have 0 missing values"
            assert result["null_percentage"] == 0.0, f"{column} should have 0% missing"
            assert result["flag"] == "Green", f"{column} should be Green flag (0% missing)"
    
    print("\nAll data quality checks passed!")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
