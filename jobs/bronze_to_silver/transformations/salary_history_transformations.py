from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def clean_null_dates(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("effective_date", when(col("effective_date").isNull(), "0000-01-01").otherwise(col("effective_date")))
    )

def clean_null_numbers(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("salary", when(col("salary").isNull(), 0).otherwise(col("salary")))
          .withColumn("bonus_amount", when(col("bonus_amount").isNull(), 0).otherwise(col("bonus_amount")))
          .withColumn("stock_options", when(col("stock_options").isNull(), 0).otherwise(col("stock_options")))
    )

def clean_null_strings(df: DataFrame) -> DataFrame:
    for col_name in [
        "change_reason", "position", "currency"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df 