# EXTRACT, TRANSFORM, LOAD
from ConnectionConfig import setupEnvironment
setupEnvironment()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import os
import sys

print("Starting date dimension ETL process...")

try:
    spark = SparkSession.builder \
        .appName("DateDimension") \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .getOrCreate()
    print("Spark session created successfully")
except Exception as e:
    print(f"Failed to create Spark session: {str(e)}")
    sys.exit(1)

# TRANSFORM - Generate date dimension data
print("Generating date dimension data...")
start_date = datetime(2015, 1, 1)
end_date = datetime(2024, 12, 31)
date_list = []

current_date = start_date
while current_date <= end_date:
    date_list.append(current_date)
    current_date += timedelta(days=1)

print(f"Generated {len(date_list)} date records")

# Create dataframe
date_df = spark.createDataFrame([(d,) for d in date_list], ["date"])

# Add all attributes (no surrogate key - PostgreSQL SERIAL will handle this)
date_df = date_df \
    .withColumn("year", year("date")) \
    .withColumn("quarter", quarter("date")) \
    .withColumn("month_nr", month("date")) \
    .withColumn("month_name", date_format("date", "MMMM")) \
    .withColumn("day_nr", dayofmonth("date")) \
    .withColumn("day_name", date_format("date", "EEEE")) \
    .withColumn("is_weekday", ~(dayofweek("date").isin([1, 7])))

print(f"DataFrame created with {date_df.count()} records")

# LOAD - Write directly to PostgreSQL DWH
try:
    print("Loading data to PostgreSQL DWH...")

    # PostgreSQL connection parameters
    dwh_params = {
        "url": "jdbc:postgresql://localhost:5435/ridesdwh",
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Use truncate mode to clear data but keep table structure
    date_df.write \
        .format("jdbc") \
        .option("url", dwh_params["url"]) \
        .option("dbtable", "DateDim") \
        .option("user", dwh_params["user"]) \
        .option("password", dwh_params["password"]) \
        .option("driver", dwh_params["driver"]) \
        .mode("append") \
        .save()

    print("Data loaded to PostgreSQL DWH successfully")

except Exception as e:
    print(f"Failed to write data: {str(e)}")
    sys.exit(1)

print("Date dimension ETL process completed")
spark.stop()
