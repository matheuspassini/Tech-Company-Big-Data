from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def clean_null_dates(df: DataFrame) -> DataFrame:
    for col_name in [
        "founded_date",
        "last_audit_date"
    ]:
        df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df

def clean_null_floats(df: DataFrame) -> DataFrame:
    for col_name in [
        "budget",
        "quarterly_budget",
        "yearly_revenue"
    ]:
        df.withColumn(col_name, when(col(col_name).isNull(), 0.0).otherwise(col(col_name)))
    return df    
    
def clean_null_numbers(df: DataFrame) -> DataFrame:
    for col_name in [
        "headcount",
        "office_size"
    ]:
        df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

def clean_null_strings(df: DataFrame) -> DataFrame:
    for col_name in [
        "location",
        "region",
        "description"
    ]:
        df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df
    
def clean_arrays(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("tech_stack", when(col("tech_stack").isNull(), "Not available").otherwise(col("tech_stack")))
    )