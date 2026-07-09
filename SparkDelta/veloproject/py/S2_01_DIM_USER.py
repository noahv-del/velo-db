# EXTRACT, TRANSFORM, LOAD
from ConnectionConfig import setupEnvironment
setupEnvironment()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import psycopg2
import sys

print("Starting user dimension ETL process...")

try:
    spark = SparkSession.builder \
        .appName("UserDimension") \
        .config("spark.jars", "postgresql-42.6.0.jar") \
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

# Set job runtime for SCD2
run_timestamp = datetime.now()
print(f"Run timestamp: {run_timestamp}")

# EXTRACT - Source data with explicit types
print("Extracting user data from source system...")
source_users_df = spark.read \
    .format("jdbc") \
    .option("url", source_params["url"]) \
    .option("dbtable", "velo_users") \
    .option("user", source_params["user"]) \
    .option("password", source_params["password"]) \
    .option("driver", source_params["driver"]) \
    .load()

# Transform with explicit casting to ensure no NULL/void types
source_users_df = source_users_df.select(
    col("userid").cast(IntegerType()).alias("source_user_id"),
    col("street").cast(StringType()).alias("source_street"),
    col("number").cast(StringType()).alias("source_number"),
    col("zipcode").cast(StringType()).alias("source_zipcode"),
    col("city").cast(StringType()).alias("source_city"),
    col("country_code").cast(StringType()).alias("source_country_code"),
    md5(concat_ws("|",
                  coalesce(col("street"), lit("")),
                  coalesce(col("number"), lit("")),
                  coalesce(col("zipcode"), lit("")),
                  coalesce(col("city"), lit("")),
                  coalesce(col("country_code"), lit(""))
                  )).cast(StringType()).alias("source_md5")
)

print(f"Extracted {source_users_df.count()} users from source")
source_users_df.createOrReplaceTempView("source_users")

# Check if dimension table exists and avoid recreation if it has dependencies
table_exists = False
try:
    print("Checking if dimension exists...")
    check_df = spark.read \
        .format("jdbc") \
        .option("url", dwh_params["url"]) \
        .option("dbtable", "dim_user") \
        .option("user", dwh_params["user"]) \
        .option("password", dwh_params["password"]) \
        .option("driver", dwh_params["driver"]) \
        .option("fetchsize", "1") \
        .load()

    table_exists = True
    print("dim_user table exists")
except Exception as e:
    print(f"dim_user table doesn't exist: {str(e)}")
    table_exists = False

if not table_exists:
    # TRANSFORM - Create initial dimension data with explicit types
    print("Creating new dimension with initial data...")

    # Create schema for the dim_user table
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("street", StringType(), True),
        StructField("number", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("start_date", TimestampType(), False),
        StructField("end_date", TimestampType(), False),
        StructField("current", BooleanType(), False),
        StructField("md5", StringType(), False)
    ])

    # For initial load, explicitly convert all columns
    initial_dim_data = source_users_df.rdd.map(lambda r: (
        r.source_user_id,  # user_id
        r.source_street,  # street
        r.source_number,  # number
        r.source_zipcode,  # zipcode
        r.source_city,  # city
        r.source_country_code,  # country_code
        run_timestamp,  # start_date
        datetime(2100, 12, 31),  # end_date
        True,  # current
        r.source_md5  # md5
    ))

    initial_dim_df = spark.createDataFrame(initial_dim_data, schema)

    # LOAD - Use psycopg2 for table creation to handle SERIAL type properly
    try:
        print("Creating dim_user table...")
        connection = psycopg2.connect(
            host="localhost",
            port=5435,
            database="ridesdwh",
            user="postgres",
            password="postgres"
        )
        cursor = connection.cursor()

        # Create table with consistent name (dim_user)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS dim_user (
            user_sk SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            street VARCHAR(255),
            number VARCHAR(50),
            zipcode VARCHAR(20),
            city VARCHAR(100),
            country_code VARCHAR(10),
            start_date TIMESTAMP NOT NULL,
            end_date TIMESTAMP NOT NULL,
            current BOOLEAN NOT NULL,
            md5 VARCHAR(32) NOT NULL
        )
        """
        cursor.execute(create_table_sql)
        connection.commit()
        cursor.close()
        connection.close()
        print("Table created successfully")

        # Now insert the initial data using regular Spark JDBC write
        initial_dim_df.write \
            .format("jdbc") \
            .option("url", dwh_params["url"]) \
            .option("dbtable", "dim_user") \
            .option("user", dwh_params["user"]) \
            .option("password", dwh_params["password"]) \
            .option("driver", dwh_params["driver"]) \
            .mode("append") \
            .save()

        print(f"Initial load: Created dim_user with {initial_dim_df.count()} records")

    except Exception as e:
        print(f"Error during initial load: {str(e)}")
        sys.exit(1)

else:
    # Read current dimension for incremental load
    print("Reading current dimension for incremental load...")
    current_dim_df = spark.read \
        .format("jdbc") \
        .option("url", dwh_params["url"]) \
        .option("dbtable", "dim_user") \
        .option("user", dwh_params["user"]) \
        .option("password", dwh_params["password"]) \
        .option("driver", dwh_params["driver"]) \
        .load()

    current_dim_df.createOrReplaceTempView("current_dim")
    print(f"Found {current_dim_df.count()} existing dimension records")

    # TRANSFORM - Detect changes using LEFT JOIN
    print("Detecting changes...")

    # 1. Find all records that need processing (changed or new)
    changes_df = spark.sql("""
        SELECT
            s.source_user_id,
            s.source_street,
            s.source_number,
            s.source_zipcode,
            s.source_city,
            s.source_country_code,
            s.source_md5,
            d.user_sk,
            d.md5 as dim_md5,
            d.current
        FROM source_users s
        LEFT JOIN current_dim d ON s.source_user_id = d.user_id AND d.current = TRUE
        WHERE d.user_id IS NULL OR s.source_md5 != d.md5
    """)
    changes_df.createOrReplaceTempView("detected_changes")

    # Count change types for logging
    new_records = spark.sql("""
        SELECT * FROM detected_changes WHERE user_sk IS NULL
    """)

    changed_records = spark.sql("""
        SELECT * FROM detected_changes WHERE user_sk IS NOT NULL
    """)

    new_count = new_records.count()
    changed_count = changed_records.count()

    print(f"Found {new_count} new records and {changed_count} changed records")

    # 2. Process the changes if any exists
    if changed_count > 0 or new_count > 0:
        print("Processing changes...")

        # First: Expire changed records
        if changed_count > 0:
            print(f"Processing {changed_count} changed records...")

            # Use psycopg2 for direct updates
            connection = psycopg2.connect(
                host="localhost",
                port=5435,
                database="ridesdwh",
                user="postgres",
                password="postgres"
            )
            cursor = connection.cursor()

            # Get list of user_ids to update
            user_ids = [row.source_user_id for row in changed_records.collect()]

            # Update all matching records in one SQL statement
            update_sql = f"""
                UPDATE dim_user
                SET end_date = %s,
                    current = FALSE
                WHERE user_id IN ({','.join(str(uid) for uid in user_ids)})
                AND current = TRUE
            """
            cursor.execute(update_sql, (run_timestamp,))
            connection.commit()
            cursor.close()
            connection.close()

            print(f"Expired {changed_count} old records")

        # Second: Insert new records and new versions of changed records
        if new_count > 0 or changed_count > 0:
            print(f"Inserting {new_count + changed_count} new or changed records...")

            # Build insert data with explicit types - FIX: Use detected_changes instead of separate collections
            inserts_data = []

            # Process ALL changes (both new and changed records) from the single detected_changes collection
            all_changes = spark.sql("SELECT * FROM detected_changes").collect()
            for row in all_changes:
                inserts_data.append((
                    row.source_user_id,  # user_id
                    row.source_street,  # street
                    row.source_number,  # number
                    row.source_zipcode,  # zipcode
                    row.source_city,  # city
                    row.source_country_code,  # country_code
                    run_timestamp,  # start_date
                    datetime(2100, 12, 31),  # end_date
                    True,  # current
                    row.source_md5  # md5
                ))

            # Create DataFrame for inserts with explicit schema
            schema = StructType([
                StructField("user_id", IntegerType(), False),
                StructField("street", StringType(), True),
                StructField("number", StringType(), True),
                StructField("zipcode", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("start_date", TimestampType(), False),
                StructField("end_date", TimestampType(), False),
                StructField("current", BooleanType(), False),
                StructField("md5", StringType(), False)
            ])

            inserts_df = spark.createDataFrame(inserts_data, schema)

            # Use append mode to insert the new records
            inserts_df.write \
                .format("jdbc") \
                .option("url", dwh_params["url"]) \
                .option("dbtable", "dim_user") \
                .option("user", dwh_params["user"]) \
                .option("password", dwh_params["password"]) \
                .option("driver", dwh_params["driver"]) \
                .mode("append") \
                .save()

            print(f"Inserted {len(inserts_data)} records")
    else:
        print("No changes detected")
print("User dimension ETL process completed")
spark.stop()