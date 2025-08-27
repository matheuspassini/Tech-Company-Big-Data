import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="session", autouse=True)
def spark_fixture():
    spark = SparkSession.builder.appName("test_dataframe_creation").getOrCreate()
    yield spark
    spark.stop()

def test_dataframe_creation(spark_fixture):

    employee_data = [
    ("EMP001", 8500, 4.8, True),
    ("EMP002", 6500, 4.5, False),
    ("EMP003", 4500, 4.2, False),
    ("EMP004", 12000, 4.9, True),
    ("EMP005", 7500, 4.6, False)
    ]

    columns = ["employee_id", "salary", "performance_rating", "is_active"] 
    employee_df = spark_fixture.createDataFrame(employee_data, columns)
    assert employee_df.count() == 5
    assert employee_df.columns == ["employee_id", "salary", "performance_rating", "is_active"]
    assert employee_df.filter(col("employee_id") == "EMP001").collect()[0]["salary"] == 8500
    assert employee_df.filter(col("employee_id") == "EMP002").collect()[0]["performance_rating"] == 4.5
    assert employee_df.filter(col("employee_id") == "EMP003").collect()[0]["is_active"] == False
    assert employee_df.filter(col("employee_id") == "EMP004").collect()[0]["salary"] == 12000
    assert employee_df.filter(col("employee_id") == "EMP005").collect()[0]["performance_rating"] == 4.6

if __name__ == "__main__":
    pytest.main([__file__, "-v"])






