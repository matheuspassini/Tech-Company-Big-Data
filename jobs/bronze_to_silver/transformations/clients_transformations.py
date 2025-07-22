from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def clean_null_dates(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("contract_start_date", when(col("contract_start_date").isNull(), "0000-01-01").otherwise(col("contract_start_date")))
          .withColumn("last_contact_date", when(col("last_contact_date").isNull(), "0000-01-01").otherwise(col("last_contact_date")))
          .withColumn("customer_since", when(col("customer_since").isNull(), "0000-01-01").otherwise(col("customer_since")))
    )

def clean_null_numbers(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("contract_value", when(col("contract_value").isNull(), 0).otherwise(col("contract_value")))
          .withColumn("annual_revenue", when(col("annual_revenue").isNull(), 0).otherwise(col("annual_revenue")))
          .withColumn("employee_count", when(col("employee_count").isNull(), 0).otherwise(col("employee_count")))
    )

def clean_null_floats(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("churn_risk_score", when(col("churn_risk_score").isNull(), 0.0).otherwise(col("churn_risk_score")))
          .withColumn("satisfaction_score", when(col("satisfaction_score").isNull(), 0.0).otherwise(col("satisfaction_score")))
          .withColumn("upsell_potential", when(col("upsell_potential").isNull(), 0.0).otherwise(col("upsell_potential")))
    )

def clean_null_strings(df: DataFrame) -> DataFrame:
    for col_name in [
        "company_name", "industry", "contact_person", "email", "phone", "address", "country",
        "status", "payment_history", "contract_type"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df 