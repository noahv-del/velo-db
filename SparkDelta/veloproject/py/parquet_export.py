# parquet_export.py
from ConnectionConfig import setupEnvironment

setupEnvironment()

from pyspark.sql import SparkSession
import os
import psycopg2
import sys

print("Starting data warehouse export to Parquet...")

try:
    spark = SparkSession.builder \
        .appName("DWHExport") \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .config("spark.sql.warehouse.dir", "../spark_warehouse") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "4g") \
        .getOrCreate()
    print("Spark session created successfully")
except Exception as e:
    print(f"Failed to create Spark session: {str(e)}")
    sys.exit(1)

# Connection parameters
dwh_params = {
    "url": "jdbc:postgresql://localhost:5435/ridesdwh",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Create output directory
output_dir = "../parquet_files/"
os.makedirs(output_dir, exist_ok=True)

# Export dimension tables first
print("Exporting dimension tables...")
dim_tables = ["dim_date", "dim_user", "dim_lock", "dim_weather", "dim_vehicle", "weather_processed"]
for table in dim_tables:
    print(f"Exporting {table}...")
    try:
        dim_df = spark.read \
            .format("jdbc") \
            .option("url", dwh_params["url"]) \
            .option("dbtable", table) \
            .option("user", dwh_params["user"]) \
            .option("password", dwh_params["password"]) \
            .option("driver", dwh_params["driver"]) \
            .load()
        dim_df.write.mode("overwrite").parquet(f"{output_dir}{table}")
        print(f"✓ Exported {dim_df.count()} records from {table}")
    except Exception as e:
        print(f"Error exporting {table}: {str(e)}")

# Create a view for fact_ride with year information
print("Setting up view for fact table export...")
try:
    connection = psycopg2.connect(
        host="localhost",
        port=5435,
        database="ridesdwh",
        user="postgres",
        password="postgres"
    )

    with connection.cursor() as cursor:
        cursor.execute("""
        CREATE OR REPLACE VIEW fact_ride_export AS
        SELECT fr.*, EXTRACT(YEAR FROM d.date)::INTEGER AS year
        FROM fact_ride fr
        JOIN dim_date d ON fr.date_sk = d.date_sk
        """)
        connection.commit()
    connection.close()
except Exception as e:
    print(f"Error creating view: {str(e)}")
    sys.exit(1)

# Get total count
print("Getting record count...")
count_df = spark.read \
    .format("jdbc") \
    .option("url", dwh_params["url"]) \
    .option("query", "SELECT COUNT(*) FROM fact_ride") \
    .option("user", dwh_params["user"]) \
    .option("password", dwh_params["password"]) \
    .option("driver", dwh_params["driver"]) \
    .load()

total_records = count_df.collect()[0][0]
batch_size = 500000
batches = (total_records // batch_size) + (1 if total_records % batch_size > 0 else 0)

print(f"Exporting {total_records} records in {batches} batches")

# Export fact table in batches
for batch in range(batches):
    offset = batch * batch_size
    print(f"Processing batch {batch + 1}/{batches} (offset {offset})")

    try:
        query = f"""
        SELECT * FROM fact_ride_export
        ORDER BY ride_sk
        LIMIT {batch_size} OFFSET {offset}
        """

        batch_df = spark.read \
            .format("jdbc") \
            .option("url", dwh_params["url"]) \
            .option("query", query) \
            .option("user", dwh_params["user"]) \
            .option("password", dwh_params["password"]) \
            .option("driver", dwh_params["driver"]) \
            .option("fetchsize", "10000") \
            .load()

        # First batch overwrites, others append
        write_mode = "overwrite" if batch == 0 else "append"

        # Write with year partitioning (fewer directories than year-month)
        batch_df.repartition(4).write \
            .mode(write_mode) \
            .partitionBy("year") \
            .parquet(f"{output_dir}fact_ride")

        print(f"✓ Exported batch {batch + 1} with {batch_df.count()} records")

    except Exception as e:
        print(f"Error processing batch {batch + 1}: {str(e)}")
        break


# Clean up
try:
    connection = psycopg2.connect(
        host="localhost",
        port=5435,
        database="ridesdwh",
        user="postgres",
        password="postgres"
    )
    with connection.cursor() as cursor:
        cursor.execute("DROP VIEW IF EXISTS fact_ride_export")
        connection.commit()
    connection.close()
    print("Temporary view removed")
except Exception as e:
    print(f"Error cleaning up: {str(e)}")

print("Data warehouse export completed")
spark.stop()