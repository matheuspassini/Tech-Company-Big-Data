from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def clean_null_dates(df: DataFrame) -> DataFrame:
    for col_name in [
        "hire_date", 
        "last_review_date", 
        "next_review_date"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df
    
def clean_null_numbers(df: DataFrame) -> DataFrame:
    for col_name in [
        "projects_assigned", 
        "overtime_hours", 
        "vacation_days_used", 
        "vacation_days_remaining", 
        "years_experience"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df
    
def clean_null_strings(df: DataFrame) -> DataFrame:
    for col_name in [
        "position", 
        "work_location"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df
    
def clean_null_booleans(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("is_manager", when(col("is_manager").isNull(), False).otherwise(col("is_manager")))
    )

def clean_arrays(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("skills", when(col("skills").isNull(), "No skills").otherwise(col("skills")))
          .withColumn("certifications", when(col("certifications").isNull(), "No certifications").otherwise(col("certifications")))
    )