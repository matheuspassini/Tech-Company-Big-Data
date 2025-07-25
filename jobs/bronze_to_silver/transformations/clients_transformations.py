from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def clean_null_dates(df: DataFrame) -> DataFrame:
    for col_name in [
        "contract_start_date", 
        "last_contact_date", 
        "customer_since"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df

def clean_null_numbers(df: DataFrame) -> DataFrame:
    for col_name in [
        "contract_value", 
        "annual_revenue", 
        "employee_count"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

def clean_null_floats(df: DataFrame) -> DataFrame:
    for col_name in [
        "churn_risk_score", 
        "satisfaction_score", 
        "upsell_potential"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0.0).otherwise(col(col_name)))
    return df

def clean_null_strings(df: DataFrame) -> DataFrame:
    for col_name in [
        "company_name", 
        "industry", 
        "contact_person", 
        "email", 
        "phone", 
        "address", 
        "country",
        "status", 
        "payment_history", 
        "contract_type"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df 