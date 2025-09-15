#!/usr/bin/env python3
"""
Tests to verify missing data treatment (null values) 
including fillna() and dropna() with different types of empty values
"""

import pytest
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Add project path to import functions
sys.path.append('/opt/spark/apps')

from utils.spark_utils import clean_dataframe, apply_referential_integrity

@pytest.fixture(scope="session", autouse=True)
def spark():
    """Create Spark session for tests"""
    spark = SparkSession.builder.appName("simple_test").getOrCreate()
    yield spark
    spark.stop()

def create_test_data(spark):
    """Create test data with ALL types of null/empty values"""

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True)
    ])
    
    data = [
        ("001", "João", 25, 5000),           # Complete data
        ("002", None, 30, 6000),             # NULL
        ("003", "Maria", None, 7000),        # NULL age
        ("004", "Pedro", 28, None),          # NULL salary
        ("005", "", 32, 5500),               # Empty string
        ("006", " ", 35, 8000),            # Only spaces
        ("007", "nan", 40, 9000),            # String "nan"
        ("008", "NaN", 45, 10000),           # String "NaN"
        ("009", "null", 50, 11000),          # String "null"
        ("010", "NULL", 55, 12000),          # String "NULL"
        ("011", "None", 60, 13000),          # String "None"
        ("012", "N/A", 65, 14000),           # String "N/A"
        ("013", "n/a", 70, 15000),           # String "n/a"
        ("014", "[]", 75, 16000),            # String "[]"
        ("015", "{}", 80, 17000),            # String "{}"
        ("016", "undefined", 85, 18000),     # String "undefined"
        ("017", "UNDEFINED", 90, 19000),     # String "UNDEFINED"
        (None, "Ana", 95, 20000),            # NULL ID
        ("", "Bruno", 100, 21000),           # Empty string ID
        (" ", "Carlos", 105, 22000)        # Spaces only ID
    ]
    
    return spark.createDataFrame(data, schema)

def test_fillna_simple(spark):
    """Simple test for fillna() function - replace empty values"""
    from utils.spark_utils import transform_empty_strings

    df = create_test_data(spark)
    df = transform_empty_strings(df)
    
    cleaning_rules = {
        "null_strings": ["name"],
        "null_integers": ["age", "salary"]
    }
    df_clean = clean_dataframe(df, cleaning_rules)
    
    assert df_clean.count() == 20, "Should maintain 20 records"
    
    from pyspark.sql.functions import trim, lower
    empty_names = df_clean.filter(
        df_clean.name.isNull() | 
        (df_clean.name == "") | 
        (trim(df_clean.name) == "") |
        (lower(df_clean.name) == "nan") |
        (df_clean.name == "null") |
        (df_clean.name == "NULL") |
        (df_clean.name == "None") |
        (df_clean.name == "N/A") |
        (df_clean.name == "n/a") |
        (df_clean.name == "[]") |
        (df_clean.name == "{}") |
        (df_clean.name == "undefined") |
        (df_clean.name == "UNDEFINED")
    ).count()
    
    empty_ages = df_clean.filter(df_clean.age.isNull()).count()
    empty_salaries = df_clean.filter(df_clean.salary.isNull()).count()
    
    assert empty_names == 0, "Should not have empty names"
    assert empty_ages == 0, "Should not have empty ages"
    assert empty_salaries == 0, "Should not have empty salaries"

def test_dropna_simple(spark):
    """Test for dropna() function - remove records with empty data"""
    df = create_test_data(spark)
    from utils.spark_utils import transform_empty_strings
    df = transform_empty_strings(df)
    
    required_columns = ["id"]
    df_clean = apply_referential_integrity(df, required_columns)
    
    final_records = df_clean.count()
    assert final_records == 17, f"Should have 17 final records, but has {final_records}"
    
    from pyspark.sql.functions import trim
    empty_ids = df_clean.filter(
        df_clean.id.isNull() | 
        (df_clean.id == "") | 
        (trim(df_clean.id) == "")
    ).count()
    
    assert empty_ids == 0, "Should not have empty IDs"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
