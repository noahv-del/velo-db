from ConnectionConfig import setupEnvironment

setupEnvironment()

# EXTRACT, TRANSFORM, LOAD
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import os
import sys

print("Starting lock dimension ETL process...")
# Initialize Spark with better error reporting
try:
    spark = SparkSession.builder \
        .appName("LockDimension") \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .getOrCreate()
    print("Spark session created successfully")
except Exception as e:
    print(f"Failed to create Spark session: {str(e)}")
    sys.exit(1)

# Database connection parameters
db_params = {
    "url": "jdbc:postgresql://localhost:5435/ridesdwh",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Source database connection parameters
source_params = {
    "url": "jdbc:postgresql://localhost:5433/veloDB",
    "user": "postgres",
    "password": "Student_1234",
    "driver": "org.postgresql.Driver"
}

# Create table with error handling
try:
    print("Creating lock dimension table...")
    connection = psycopg2.connect(
        host="localhost",
        port=5435,
        database="ridesdwh",
        user="postgres",
        password="postgres"
    )
    cursor = connection.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_lock (
        lock_SK SERIAL PRIMARY KEY,
        lock_id INTEGER,
        station_id INTEGER,
        station_nr VARCHAR(50),
        street VARCHAR(255),
        number VARCHAR(50),
        zipcode VARCHAR(20),
        district VARCHAR(100),
        gps_coord POINT
    )
    """)
    connection.commit()
    cursor.close()
    connection.close()
    print("Lock dimension table created successfully")
except Exception as e:
    print(f"Database error: {str(e)}")
    sys.exit(1)

# EXTRACT - Read locks and stations from source system
try:
    print("Extracting locks and stations from source system...")
    locks_df = spark.read \
        .format("jdbc") \
        .option("url", source_params["url"]) \
        .option("dbtable", "locks") \
        .option("user", source_params["user"]) \
        .option("password", source_params["password"]) \
        .option("driver", source_params["driver"]) \
        .load()

    stations_df = spark.read \
        .format("jdbc") \
        .option("url", source_params["url"]) \
        .option("dbtable", "stations") \
        .option("user", source_params["user"]) \
        .option("password", source_params["password"]) \
        .option("driver", source_params["driver"]) \
        .load()

    print(f"Extracted {locks_df.count()} locks and {stations_df.count()} stations")
except Exception as e:
    print(f"Error extracting source data: {str(e)}")
    sys.exit(1)

# TRANSFORM - Join locks with stations
print("Joining locks with stations...")
try:
    lock_dim_df = locks_df.join(stations_df, locks_df.stationid == stations_df.stationid) \
        .select(
        locks_df.lockid.alias("lock_id"),
        stations_df.stationid.alias("station_id"),
        stations_df.stationnr.alias("station_nr"),
        stations_df.street,
        stations_df.number,
        stations_df.zipcode,
        stations_df.district,
        stations_df.gpscoord.alias("gps_coord")
    )

    print(f"Created lock dimension with {lock_dim_df.count()} regular locks")

    # Create a "no lock" record for scooters
    no_lock_data = [(-1, None, "no lock", None, None, None, None, None)]
    no_lock_schema = StructType([
        StructField("lock_id", IntegerType(), True),
        StructField("station_id", IntegerType(), True),
        StructField("station_nr", StringType(), True),
        StructField("street", StringType(), True),
        StructField("number", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("district", StringType(), True),
        StructField("gps_coord", StringType(), True)
    ])

    no_lock_df = spark.createDataFrame(no_lock_data, no_lock_schema)
    print("Created 'no lock' record for scooters")

    # Union regular locks with no lock record
    full_lock_dim_df = lock_dim_df.union(no_lock_df)
    print(f"Combined dimension has {full_lock_dim_df.count()} records")

except Exception as e:
    print(f"Error transforming lock data: {str(e)}")
    sys.exit(1)

# LOAD - Write to PostgreSQL with error handling
try:
    print("Writing lock dimension data to database...")
    full_lock_dim_df.write \
        .format("jdbc") \
        .option("url", db_params["url"]) \
        .option("dbtable", "dim_lock") \
        .option("user", db_params["user"]) \
        .option("password", db_params["password"]) \
        .option("driver", db_params["driver"]) \
        .mode("overwrite") \
        .save()
    print("Data written successfully")
except Exception as e:
    print(f"Failed to write data: {str(e)}")
    sys.exit(1)

# Verify data was written
try:
    print("Verifying data in database...")
    connection = psycopg2.connect(
        host="localhost",
        port=5435,
        database="ridesdwh",
        user="postgres",
        password="postgres"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM dim_lock")
    count = cursor.fetchone()[0]
    print(f"Records in LockDim: {count}")
    cursor.close()
    connection.close()
except Exception as e:
    print(f"Verification error: {str(e)}")

print("Lock dimension ETL process completed")
spark.stop()