import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType
)
from pyspark.sql.functions import col

@pytest.fixture(scope="session", autouse=True)
def spark():
    spark = SparkSession.builder.appName("test_schema").getOrCreate()
    yield spark
    spark.stop()

def test_schema_with_different_types(spark):
    """Test dataframe creation with explicit schema and different data types"""
    
    # 1. Define schema with different types
    schema = StructType([
        StructField("id", StringType(), True),                    # String
        StructField("name", StringType(), True),                  # String
        StructField("age", IntegerType(), True),                  # Integer
        StructField("salary", FloatType(), True),                 # Float
        StructField("is_active", BooleanType(), True),            # Boolean
    ])
    
    # 2. Test data
    test_data = [
        ("EMP001", "test1", 25, 5000.50, True),
        ("EMP002", "test2", 30, 7500.75, False),
        ("EMP003", "test3", 35, 10000.00, True),
        ("EMP004", "test4", 28, 6500.25, True),
        ("EMP005", "test5", 32, 8500.80, False)
    ]
    
    # 3. Create dataframe with explicit schema
    df = spark.createDataFrame(test_data, schema)
    
    # 4. Basic tests
    assert df.count() == 5, "Should have 5 records"
    assert len(df.columns) == 5, "Should have 5 columns"
    
    # 5. Schema tests
    assert df.schema == schema, "Schema should match the defined schema"
    
    # 6. Data type tests
    schema_dict = {field.name: field.dataType for field in df.schema.fields}
    
    assert isinstance(schema_dict["id"], StringType), "ID should be StringType"
    assert isinstance(schema_dict["name"], StringType), "Name should be StringType"
    assert isinstance(schema_dict["age"], IntegerType), "Age should be IntegerType"
    assert isinstance(schema_dict["salary"], FloatType), "Salary should be FloatType"
    assert isinstance(schema_dict["is_active"], BooleanType), "Is_active should be BooleanType"

    
    # 8. Operations by type tests
    # String operations
    names = df.select("name").collect()
    assert len(names) == 5, "Should have 5 names"
    
    # Integer operations
    avg_age = df.agg({"age": "avg"}).collect()[0]["avg(age)"]
    assert avg_age == 30.0, f"Average age should be 30.0, but was {avg_age}"
    
    # Float operations
    total_salary = df.agg({"salary": "sum"}).collect()[0]["sum(salary)"]
    expected_total = 5000.50 + 7500.75 + 10000.00 + 6500.25 + 8500.80  # Calculate exact sum
    assert abs(total_salary - expected_total) < 0.01, f"Total salary should be approximately {expected_total}, but was {total_salary}"
    
    # Boolean operations
    active_count = df.filter(col("is_active") == True).count()
    assert active_count == 3, "Should have 3 active employees"
    
    inactive_count = df.filter(col("is_active") == False).count()
    assert inactive_count == 2, "Should have 2 inactive employees"
    
    print("Schema test with different types passed!")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
