# EXTRACT, TRANSFORM, LOAD
from ConnectionConfig import setupEnvironment
setupEnvironment()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import psycopg2
import sys
import os
from pyspark.sql.types import *

print("Starting ride fact table ETL process...")

try:
    spark = SparkSession.builder \
        .appName("RideFactTable") \
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
source_params = {
    "url": "jdbc:postgresql://localhost:5433/veloDB",
    "user": "postgres",
    "password": "Student_1234",
    "driver": "org.postgresql.Driver"
}

dwh_params = {
    "url": "jdbc:postgresql://localhost:5435/ridesdwh",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Get the last processed ride_id
try:
    connection = psycopg2.connect(
        host="localhost",
        port=5435,
        database="ridesdwh",
        user="postgres",
        password="postgres"
    )
    cursor = connection.cursor()

    # Create the fact_ride table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_ride (
        ride_sk SERIAL PRIMARY KEY,
        ride_id INTEGER NOT NULL,
        date_sk INTEGER,
        user_sk INTEGER,
        start_lock_id INTEGER,
        end_lock_id INTEGER,
        weather_id INTEGER,
        vehicle_id INTEGER,
        ride_distance NUMERIC,
        ride_duration NUMERIC
    )
    """)

    cursor.execute("SELECT COALESCE(MAX(ride_id), 0) FROM fact_ride")
    max_processed_ride_id = cursor.fetchone()[0]
    cursor.close()
    connection.commit()
    connection.close()
    print(f"Last processed ride_id: {max_processed_ride_id}")
except Exception as e:
    print(f"Database setup error: {str(e)}")
    max_processed_ride_id = 0

# Track total processed rides for this run
total_processed = 0
batch_size = 500000  # Process 50000 records per batch
current_max_id = max_processed_ride_id

# Process in batches
while True:
    # EXTRACT - Get ride data using haversine function for current batch
    print(f"Extracting ride data batch (after ride_id {current_max_id})...")
    rides_query = f"""
        SELECT r.rideid AS ride_id,
               sub.userid AS user_id,
               r.startlockid AS start_lock_id,
               r.endlockid AS end_lock_id,
               r.starttime AS start_time,
               r.endtime AS end_time,
               EXTRACT(EPOCH FROM (r.endtime - r.starttime))/60 AS ride_duration,

               -- Use actual ride start/end points for distance calculation
               CASE
                 WHEN r.startpoint IS NOT NULL AND r.endpoint IS NOT NULL THEN
                   haversine_km(
                     (r.startpoint)[1],  -- start latitude (Y coordinate)
                     (r.startpoint)[0],  -- start longitude (X coordinate)
                     (r.endpoint)[1],    -- end latitude (Y coordinate)
                     (r.endpoint)[0]     -- end longitude (X coordinate)
                   )
                 -- Fallback to station coordinates if ride points are null
                 WHEN sl.stationid IS NOT NULL AND el.stationid IS NOT NULL THEN
                   haversine_km(
                     (ss.gpscoord)[1],  -- start station latitude
                     (ss.gpscoord)[0],  -- start station longitude
                     (es.gpscoord)[1],  -- end station latitude
                     (es.gpscoord)[0]   -- end station longitude
                   )
                 ELSE 0
               END AS ride_distance,

               -- For dimension joins - use start lock station if available, otherwise use ride startpoint
               DATE(r.starttime) AS ride_date,
               DATE_TRUNC('hour', r.starttime) AS start_hour,
               CASE 
                 WHEN ss.zipcode IS NOT NULL THEN ss.zipcode
                 ELSE '0000'
               END AS start_postal_code,
               COALESCE(r.vehicleid, 0) AS vehicle_id

        FROM rides r
        LEFT JOIN locks sl ON r.startlockid = sl.lockid
        LEFT JOIN locks el ON r.endlockid = el.lockid
        LEFT JOIN stations ss ON sl.stationid = ss.stationid
        LEFT JOIN stations es ON el.stationid = es.stationid
        LEFT JOIN vehicles v ON r.vehicleid = v.vehicleid
        LEFT JOIN subscriptions sub ON r.subscriptionid = sub.subscriptionid
        -- Filter out invalid rides
        WHERE r.rideid > {current_max_id}
          AND r.starttime <= r.endtime
        ORDER BY r.rideid
        LIMIT {batch_size}
        """

    try:
        # Extract batch
        rides_df = spark.read \
            .format("jdbc") \
            .option("url", source_params["url"]) \
            .option("query", rides_query) \
            .option("user", source_params["user"]) \
            .option("password", source_params["password"]) \
            .option("driver", source_params["driver"]) \
            .load()

        batch_count = rides_df.count()
        print(f"Extracted {batch_count} ride records in current batch")

        if batch_count == 0:
            print("No more rides to process.")
            break  # Exit the loop if no more records

        # Create temp view for this batch
        rides_df.createOrReplaceTempView("rides_source")

        # READ DIMENSION TABLES from PostgreSQL
        print("Reading dimension tables from PostgreSQL...")
        try:
            dim_date = spark.read \
                .format("jdbc") \
                .option("url", dwh_params["url"]) \
                .option("dbtable", "dim_date") \
                .option("user", dwh_params["user"]) \
                .option("password", dwh_params["password"]) \
                .option("driver", dwh_params["driver"]) \
                .load()
            dim_date.createOrReplaceTempView("dim_date")
            print("Loaded date dimension")

            dim_user = spark.read \
                .format("jdbc") \
                .option("url", dwh_params["url"]) \
                .option("dbtable", "dim_user") \
                .option("user", dwh_params["user"]) \
                .option("password", dwh_params["password"]) \
                .option("driver", dwh_params["driver"]) \
                .load()
            dim_user.createOrReplaceTempView("dim_user")
            print("Loaded user dimension")

            dim_lock = spark.read \
                .format("jdbc") \
                .option("url", dwh_params["url"]) \
                .option("dbtable", "dim_lock") \
                .option("user", dwh_params["user"]) \
                .option("password", dwh_params["password"]) \
                .option("driver", dwh_params["driver"]) \
                .load()
            dim_lock.createOrReplaceTempView("dim_lock")
            print("Loaded lock dimension")

            dim_weather = spark.read \
                .format("jdbc") \
                .option("url", dwh_params["url"]) \
                .option("dbtable", "dim_weather") \
                .option("user", dwh_params["user"]) \
                .option("password", dwh_params["password"]) \
                .option("driver", dwh_params["driver"]) \
                .load()
            dim_weather.createOrReplaceTempView("dim_weather")
            print("Loaded weather dimension")

        except Exception as e:
            print(f"Error reading dimension tables: {str(e)}")
            sys.exit(1)

        # PROCESS WEATHER DATA
        print("Processing weather data from JSON files...")
        try:
            # Read weather JSON files
            weather_raw_df = spark.read.text("data/weather/*.json")

            # Parse weather data and create mapping
            weather_processed_df = weather_raw_df.select(
                get_json_object(col("value"), "$.zipCode").alias("zipcode"),
                get_json_object(col("value"), "$.dt").cast("long").alias("timestamp"),
                get_json_object(col("value"), "$.weather[0].main").alias("weather_condition"),
                get_json_object(col("value"), "$.main.temp").cast("double").alias("temperature")
            ).withColumn("weather_hour", from_unixtime(col("timestamp"), "yyyy-MM-dd HH:00:00")) \
                .withColumn("weather_type",
                            when((col("weather_condition") == "Rain") | (col("weather_condition").contains("rain")),
                                 "Unpleasant")
                            .when((col("weather_condition") == "Clear") & (col("temperature") > 288.15),
                                  "Pleasant")  # 15°C in Kelvin
                            .otherwise("Neutral")
                            )

            weather_processed_df.createOrReplaceTempView("weather_data")
            print(f"Processed {weather_processed_df.count()} weather records")

        except Exception as e:
            print(f"Weather processing error: {str(e)}")
            # Create fallback weather mapping
            spark.createDataFrame([(
                '0000',
                None,
                'Unknown'
            )], ["zipcode", "weather_hour", "weather_type"]).createOrReplaceTempView("weather_data")

        # BUILD FACT TABLE with surrogate keys
        print("Building fact table with dimension lookups...")
        try:
            rideFact = spark.sql("""
                SELECT
                    src.ride_id,
                    COALESCE(dd.date_sk, -1) as date_sk,
                    COALESCE(du.user_sk, -1) as user_sk,
                    COALESCE(dl_start.lock_id, -1) as start_lock_id,
                    COALESCE(dl_end.lock_id, -1) as end_lock_id,
                    COALESCE(dw.weather_id, -1) as weather_id,
                    src.vehicle_id,
                    src.ride_distance,
                    src.ride_duration
                FROM rides_source src
                LEFT JOIN dim_date dd ON src.ride_date = dd.date
                LEFT JOIN dim_user du ON src.user_id = du.user_id AND du.current = true
                LEFT JOIN dim_lock dl_start ON src.start_lock_id = dl_start.lock_id
                LEFT JOIN dim_lock dl_end ON src.end_lock_id = dl_end.lock_id
                LEFT JOIN weather_data wd ON src.start_postal_code = wd.zipcode
                    AND to_timestamp(date_trunc('hour', src.start_time)) = to_timestamp(wd.weather_hour)
                LEFT JOIN dim_weather dw ON COALESCE(wd.weather_type, 'Unknown') = dw.weather_type
            """)

            print(f"Built fact table with {rideFact.count()} records")

        except Exception as e:
            print(f"Error building fact table: {str(e)}")
            sys.exit(1)

        # LOAD - Write directly to PostgreSQL DWH
        try:
            print("Loading fact table to PostgreSQL DWH...")

            # Write to PostgreSQL
            rideFact.write \
                .format("jdbc") \
                .option("url", dwh_params["url"]) \
                .option("dbtable", "fact_ride") \
                .option("user", dwh_params["user"]) \
                .option("password", dwh_params["password"]) \
                .option("driver", dwh_params["driver"]) \
                .mode("append") \
                .save()

            # Update tracking variables
            if batch_count > 0:
                # Get max ride_id from this batch to use as the next starting point
                max_id_in_batch = rides_df.agg({"ride_id": "max"}).collect()[0][0]
                current_max_id = max_id_in_batch
                total_processed += batch_count
                print(f"Processed batch up to ride_id {current_max_id}")

            print("Batch loaded to PostgreSQL DWH successfully")

        except Exception as e:
            print(f"Failed to write data: {str(e)}")
            sys.exit(1)

    except Exception as e:
        print(f"Error processing batch: {str(e)}")
        break  # Exit on error

print(f"Ride fact table ETL process completed. Total records processed: {total_processed}")
if total_processed > 0:
    print("Exporting fact_ride data to Parquet...")

    # Create output directory if it doesn't exist
    output_dir = "../parquet_files/"
    os.makedirs(output_dir, exist_ok=True)

    # Export dimension tables first
    dim_tables = ["dim_date", "dim_user", "dim_lock", "dim_weather", "dim_vehicle"]
    for table in dim_tables:
        print(f"Exporting {table} dimension...")
        dim_df = spark.read \
            .format("jdbc") \
            .option("url", dwh_params["url"]) \
            .option("dbtable", table) \
            .option("user", dwh_params["user"]) \
            .option("password", dwh_params["password"]) \
            .option("driver", dwh_params["driver"]) \
            .load()
        dim_df.write.mode("overwrite").parquet(f"{output_dir}{table}")

    # Export fact_ride in larger chunks using year-month partitioning
    print("Exporting fact_ride table with year partitioning...")

    # Create a temporary view with year and month added
    connection = psycopg2.connect(
        host="localhost",
        port=5435,
        database="ridesdwh",
        user="postgres",
        password="postgres"
    )

    with connection.cursor() as cursor:
        cursor.execute("""
        CREATE OR REPLACE VIEW fact_ride_with_date AS
        SELECT fr.*, 
               EXTRACT(YEAR FROM d.date) AS year,
               EXTRACT(MONTH FROM d.date) AS month
        FROM fact_ride fr
        JOIN dim_date d ON fr.date_sk = d.date_sk
        """)
        connection.commit()

    connection.close()

    # Load in batches of 500,000 records to avoid memory issues
    fact_query = """
    SELECT count(*) FROM fact_ride
    """

    count_df = spark.read \
        .format("jdbc") \
        .option("url", dwh_params["url"]) \
        .option("query", fact_query) \
        .option("user", dwh_params["user"]) \
        .option("password", dwh_params["password"]) \
        .option("driver", dwh_params["driver"]) \
        .load()

    total_records = count_df.collect()[0][0]
    batch_size = 500000
    batches = (total_records // batch_size) + (1 if total_records % batch_size > 0 else 0)

    print(f"Exporting {total_records} records in {batches} batches")

    for batch in range(batches):
        offset = batch * batch_size
        print(f"Processing batch {batch + 1}/{batches} (offset {offset})")

        query = f"""
        SELECT fr.*, d.year, d.month 
        FROM fact_ride_with_date fr
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

        # Write with year-month partitioning and controlled file size
        batch_df.repartition(4).write \
            .mode(write_mode) \
            .partitionBy("year") \
            .parquet(f"{output_dir}fact_ride")

        print(f"Exported batch {batch + 1} with {batch_df.count()} records")

    # Clean up temporary view
    connection = psycopg2.connect(
        host="localhost",
        port=5435,
        database="ridesdwh",
        user="postgres",
        password="postgres"
    )
    with connection.cursor() as cursor:
        cursor.execute("DROP VIEW IF EXISTS fact_ride_with_date")
        connection.commit()
    connection.close()

    print("Parquet export completed successfully")
spark.stop()
