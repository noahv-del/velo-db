from ConnectionConfig import setupEnvironment
setupEnvironment()

# EXTRACT, TRANSFORM, LOAD
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import psycopg2

# Initialize Spark
spark = SparkSession.builder \
    .appName("ProcessWeatherData") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

# Database connection parameters
db_params = {
    "url": "jdbc:postgresql://localhost:5435/ridesdwh",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Create a staging table for processed weather data
connection = psycopg2.connect(
    host="localhost",
    port=5435,
    database="ridesdwh",
    user="postgres",
    password="postgres"
)
cursor = connection.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS weather_processed (
    postal_code VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    weather_type VARCHAR(50) NOT NULL,
    temperature NUMERIC,
    weather_id INTEGER,
    PRIMARY KEY (postal_code, timestamp)
)
""")
connection.commit()
cursor.close()
connection.close()

# EXTRACT - Read weather JSON files
weather_files_path = "data/weather"
weather_raw_df = spark.read.text(weather_files_path)

# Define schema for parsing the JSON
weather_schema = StructType([
    StructField("zipCode", StringType(), True),
    StructField("coord", StructType([
        StructField("lon", FloatType(), True),
        StructField("lat", FloatType(), True)
    ]), True),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True)
    ])), True),
    StructField("main", StructType([
        StructField("temp", FloatType(), True),
        StructField("feels_like", FloatType(), True),
        StructField("temp_min", FloatType(), True),
        StructField("temp_max", FloatType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True)
    ]), True),
    StructField("dt", LongType(), True)
])

# TRANSFORM - Parse JSON and extract relevant fields
parsed_df = weather_raw_df.withColumn("parsed_json", from_json(col("value"), weather_schema))

# Extract relevant fields
weather_data_df = parsed_df.select(
    col("parsed_json.zipCode").alias("postal_code"),
    from_unixtime(col("parsed_json.dt")).alias("timestamp"),
    col("parsed_json.weather")[0]["main"].alias("weather_condition"),
    (col("parsed_json.main.temp") - 273.15).alias("temperature")  # Convert from Kelvin to Celsius
)

# Map to weather dimension categories
def get_weather_type_id(condition, temp):
    if condition in ["Rain", "Snow", "Thunderstorm", "Drizzle"]:
        return 2  # Unpleasant
    elif condition == "Clear" and temp > 15.0:
        return 1  # Pleasant
    else:
        return 3  # Neutral

# Register UDF
get_weather_type_id_udf = udf(get_weather_type_id, IntegerType())

# Apply UDF to determine weather_id
weather_processed_df = weather_data_df.withColumn(
    "weather_id", 
    get_weather_type_id_udf(col("weather_condition"), col("temperature"))
)

# LOAD - Write to staging table
weather_processed_df.write \
    .format("jdbc") \
    .option("url", db_params["url"]) \
    .option("dbtable", "weather_processed") \
    .option("user", db_params["user"]) \
    .option("password", db_params["password"]) \
    .option("driver", db_params["driver"]) \
    .mode("overwrite") \
    .save()

print("Weather data processed successfully")
spark.stop()