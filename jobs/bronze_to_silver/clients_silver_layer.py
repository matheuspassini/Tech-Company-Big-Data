#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)
from pyspark.sql.functions import col, when, current_timestamp, year, month, dayofmonth

# Include transformation functions directly in the file to work in cluster mode
def clean_null_dates(df):
    for col_name in [
        "contract_start_date", 
        "last_contact_date", 
        "customer_since"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df

def clean_null_numbers(df):
    for col_name in [
        "contract_value", 
        "annual_revenue", 
        "employee_count"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

def clean_null_floats(df):
    for col_name in [
        "churn_risk_score", 
        "satisfaction_score", 
        "upsell_potential"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0.0).otherwise(col(col_name)))
    return df

def clean_null_strings(df):
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

spark = SparkSession.builder.appName('Clients-Tech-Company-Application-Cluster').getOrCreate()

# Schema definition
clients_schema = StructType([
    StructField("id", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("contact_person", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("country", StringType(), True),
    StructField("contract_start_date", StringType(), True),
    StructField("contract_value", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("account_manager_id", StringType(), True),
    StructField("annual_revenue", IntegerType(), True),
    StructField("employee_count", IntegerType(), True),
    StructField("last_contact_date", StringType(), True),
    StructField("customer_since", StringType(), True),
    StructField("churn_risk_score", FloatType(), True),
    StructField("satisfaction_score", FloatType(), True),
    StructField("payment_history", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("upsell_potential", FloatType(), True)
])

# 1. Read CSV file with header
df_clients = spark.read.schema(clients_schema).csv('hdfs://master:8080/opt/spark/data/bronze_layer/clients.csv', header=True)

# 2. Transform empty strings in null values
df_clients = df_clients.select([
    when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_clients.columns
])

# 3. Add timestamp column
df_clients = df_clients.withColumn('received_at', current_timestamp())

# 4. Drop null values (Referential Integrity)
df_clients = df_clients.filter(col("id").isNotNull())
df_clients = df_clients.filter(col("account_manager_id").isNotNull())

# 5. Apply modularized transformations
df_clients = clean_null_dates(df_clients)
df_clients = clean_null_numbers(df_clients)
df_clients = clean_null_floats(df_clients)
df_clients = clean_null_strings(df_clients)

# 6. Add partitioning columns
df_clients = df_clients.withColumn("year_contract_start", year(col("contract_start_date")))
df_clients = df_clients.withColumn("month_contract_start", month(col("contract_start_date")))
df_clients = df_clients.withColumn("day_contract_start", dayofmonth(col("contract_start_date")))

# 7. Write to parquet with partitioning
df_clients.write.partitionBy("year_contract_start", "month_contract_start", "day_contract_start").parquet("hdfs://master:8080/opt/spark/data/silver_layer/clients.parquet", mode="overwrite")

spark.stop() 