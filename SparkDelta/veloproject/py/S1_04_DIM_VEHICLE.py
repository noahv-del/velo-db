from ConnectionConfig import setupEnvironment
setupEnvironment()

# EXTRACT, TRANSFORM, LOAD
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import psycopg2
import sys

# Initialize Spark
spark = SparkSession.builder \
    .appName("VehicleDimension") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

# Source database connection parameters
source_params = {
    "url": "jdbc:postgresql://localhost:5433/veloDB",
    "user": "postgres",
    "password": "Student_1234",
    "driver": "org.postgresql.Driver"
}

# Data warehouse connection parameters
db_params = {
    "url": "jdbc:postgresql://localhost:5435/ridesdwh",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Connect to PostgreSQL to create table
try:
    connection = psycopg2.connect(
        host="localhost",
        port=5435,
        database="ridesdwh",
        user="postgres",
        password="postgres"
    )
    cursor = connection.cursor()

    # Create vehicle dimension table with just two columns
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_vehicle (
        vehicle_id INTEGER PRIMARY KEY,
        type VARCHAR(50)
    )
    """)

    connection.commit()
    cursor.close()
    connection.close()
except Exception as e:
    print(f"Error creating table: {str(e)}")
    sys.exit(1)

# EXTRACT - Get vehicle data with bike type from source DB
try:
    # Correct join path: vehicles → bikelots → bike_types
    vehicle_query = """
    SELECT
        v.vehicleid,
        bt.biketypedescription
    FROM vehicles v
    JOIN bikelots bl ON v.bikelotid = bl.bikelotid
    JOIN bike_types bt ON bl.biketypeid = bt.biketypeid
    """

    vehicle_df = spark.read \
        .format("jdbc") \
        .option("url", source_params["url"]) \
        .option("query", vehicle_query) \
        .option("user", source_params["user"]) \
        .option("password", source_params["password"]) \
        .option("driver", source_params["driver"]) \
        .load()

    print(f"Extracted {vehicle_df.count()} vehicle records from source")
except Exception as e:
    print(f"Error extracting vehicle data: {str(e)}")
    sys.exit(1)

# TRANSFORM - Rename columns to match dimension table
vehicle_dim_df = vehicle_df \
    .select(
        col("vehicleid").alias("vehicle_id"),
        col("biketypedescription").alias("type")
    )

# LOAD - Write to PostgreSQL
try:
    vehicle_dim_df.write \
        .format("jdbc") \
        .option("url", db_params["url"]) \
        .option("dbtable", "dim_vehicle") \
        .option("user", db_params["user"]) \
        .option("password", db_params["password"]) \
        .option("driver", db_params["driver"]) \
        .mode("overwrite") \
        .save()

    print("Vehicle dimension created successfully")
except Exception as e:
    print(f"Error loading vehicle dimension data: {str(e)}")
    sys.exit(1)

spark.stop()