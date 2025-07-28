from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, array, lit, size, split

def clean_null_dates(df: DataFrame) -> DataFrame:
    for col_name in [
        "created_date", 
        "due_date"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df

def clean_null_numbers(df: DataFrame) -> DataFrame:
    for col_name in [
        "estimated_hours", 
        "actual_hours",
        "dependencies"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

def clean_null_strings(df: DataFrame) -> DataFrame:
    for col_name in [
        "name", 
        "description", 
        "status", 
        "priority"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df

def clean_arrays(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("tags", when(col("tags").isNull(), "No tags").otherwise(col("tags")))
    )