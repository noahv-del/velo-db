from ConnectionConfig import setupEnvironment
setupEnvironment()

# EXTRACT, TRANSFORM, LOAD
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import psycopg2

# Initialize Spark
spark = SparkSession.builder \
    .appName("WeatherDimension") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

# Database connection parameters
db_params = {
    "url": "jdbc:postgresql://localhost:5435/ridesdwh",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Connect to PostgreSQL to create table
connection = psycopg2.connect(
    host="localhost",
    port=5435,
    database="ridesdwh",
    user="postgres",
    password="postgres"
)
cursor = connection.cursor()

# Create weather dimension table with SERIAL primary key
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_weather (
    weather_id SERIAL PRIMARY KEY,
    weather_type VARCHAR(50) NOT NULL
)
""")

connection.commit()
cursor.close()
connection.close()

# TRANSFORM - Create the weather data
weather_data = [
    (1, "Pleasant"),
    (2, "Unpleasant"),
    (3, "Neutral"),
    (4, "Unknown")
]

schema = StructType([
    StructField("weather_id", IntegerType(), False),
    StructField("weather_type", StringType(), False)
])

weather_df = spark.createDataFrame(weather_data, schema)

# LOAD - Write to PostgreSQL
weather_df.write \
    .format("jdbc") \
    .option("url", db_params["url"]) \
    .option("dbtable", "dim_weather") \
    .option("user", db_params["user"]) \
    .option("password", db_params["password"]) \
    .option("driver", db_params["driver"]) \
    .mode("overwrite") \
    .save()

print("Weather dimension created successfully")
spark.stop()