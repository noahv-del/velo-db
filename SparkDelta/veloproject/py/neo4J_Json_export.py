from ConnectionConfig import setupEnvironment
setupEnvironment()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import json

# Start Spark session
spark = SparkSession.builder \
    .appName("Neo4j Graph Export") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

# PostgreSQL JDBC connection parameters
jdbc_url = "jdbc:postgresql://localhost:5433/veloDB"
jdbc_url_dwh = "jdbc:postgresql://localhost:5435/ridesdwh"
connection_properties = {
    "user": "postgres",
    "password": "Student_1234",
    "driver": "org.postgresql.Driver"
}
dwh_connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Load data from source tables
rides = spark.read.jdbc(jdbc_url, "rides", properties=connection_properties)
vehicles = spark.read.jdbc(jdbc_url, "vehicles", properties=connection_properties)
locks = spark.read.jdbc(jdbc_url, "locks", properties=connection_properties)
stations = spark.read.jdbc(jdbc_url, "stations", properties=connection_properties)
subscriptions = spark.read.jdbc(jdbc_url, "subscriptions", properties=connection_properties)
velo_users = spark.read.jdbc(jdbc_url, "velo_users", properties=connection_properties)

# Join tables to get complete ride information
rides_df = rides.join(vehicles, "vehicleid") \
    .join(subscriptions, "subscriptionid") \
    .join(velo_users, "userid") \
    .join(locks.alias("start_lock"), col("startlockid") == col("start_lock.lockid")) \
    .join(stations.alias("start_station"), col("start_lock.stationid") == col("start_station.stationid")) \
    .join(locks.alias("end_lock"), col("endlockid") == col("end_lock.lockid"), "left") \
    .join(stations.alias("end_station"), col("end_lock.stationid") == col("end_station.stationid"), "left")

# Select and rename columns to flatten the dataframe
rides_df = rides_df.select(
    rides.rideid,
    rides.starttime,
    rides.endtime,
    vehicles.vehicleid,
    vehicles.serialnumber,
    velo_users.userid,
    velo_users.email,
    velo_users.city,
    col("start_station.stationid").alias("start_stationid"),
    col("start_station.street").alias("start_street"),
    col("start_station.district").alias("start_district"),
    col("start_station.number").alias("start_number"),
    col("start_station.zipcode").alias("start_zipcode"),
    col("end_station.stationid").alias("end_stationid"),
    col("end_station.street").alias("end_street"),
    col("end_station.district").alias("end_district"),
    col("end_station.number").alias("end_number"),
    col("end_station.zipcode").alias("end_zipcode")
)

# Filter rides for 1-2 days and ensure we have at least 10,000 rides
rides_df = rides_df.withColumn("date", to_date("starttime"))
date_filter = "date BETWEEN '2023-05-01' AND '2023-05-02'"
filtered_rides = rides_df.filter(date_filter)

# Check if we need to expand the date range to get at least 10,000 rides
ride_count = filtered_rides.count()
if ride_count < 10000:
    extended_filter = "date BETWEEN '2023-05-01' AND '2023-05-03'"
    filtered_rides = rides_df.filter(extended_filter)
    ride_count = filtered_rides.count()
    if ride_count < 10000:
        extended_filter = "date BETWEEN '2023-05-01' AND '2023-05-10'"
        filtered_rides = rides_df.filter(extended_filter)

# Limit to maximum needed rides
filtered_rides = filtered_rides.limit(15000)
print(f"Exporting {filtered_rides.count()} rides")

# Extract all unique stations for the graph
all_stations = stations.select(
    "stationid", "street", "district", "number", "zipcode"
).distinct()

# Extract all unique districts for the graph
all_districts = stations.select("district").distinct()

# Prepare data for Neo4j graph
nodes = []
relationships = []

# 1. Add district nodes
district_list = [{"id": f"d-{row.district}", "labels": ["District"], "properties": {"name": row.district}}
                for row in all_districts.collect()]
nodes.extend(district_list)

# 2. Add station nodes
station_list = [{"id": f"s-{row.stationid}",
                "labels": ["Station"],
                "properties": {"stationId": row.stationid,
                              "name": row.street + (" " + row.number if row.number else ""),
                              "zipcode": row.zipcode}}
               for row in all_stations.collect()]
nodes.extend(station_list)

# 3. Add LOCATED_IN relationships between stations and districts
station_district_rels = [{"id": f"sd-{row.stationid}",
                         "type": "LOCATED_IN",
                         "startNode": f"s-{row.stationid}",
                         "endNode": f"d-{row.district}",
                         "properties": {}}
                        for row in all_stations.collect()]
relationships.extend(station_district_rels)

# 4. Process rides to add vehicles, users, and ride relationships
rides_data = filtered_rides.collect()

# Add user nodes
user_nodes = []
added_users = set()
for row in rides_data:
    if row.userid not in added_users:
        user_nodes.append({
            "id": f"u-{row.userid}",
            "labels": ["User"],
            "properties": {
                "userId": row.userid,
                "email": row.email,
                "city": row.city if hasattr(row, 'city') else None
            }
        })
        added_users.add(row.userid)
nodes.extend(user_nodes)

# Add vehicle nodes
vehicle_nodes = []
added_vehicles = set()
for row in rides_data:
    if row.vehicleid not in added_vehicles:
        vehicle_nodes.append({
            "id": f"v-{row.vehicleid}",
            "labels": ["Vehicle"],
            "properties": {
                "vehicleId": row.vehicleid,
                "serialNumber": row.serialnumber
            }
        })
        added_vehicles.add(row.vehicleid)
nodes.extend(vehicle_nodes)

# Add ride nodes and their relationships
for row in rides_data:
    # Add ride node
    ride_id = f"r-{row.rideid}"
    nodes.append({
        "id": ride_id,
        "labels": ["Ride"],
        "properties": {
            "rideId": row.rideid,
            "startTime": str(row.starttime),
            "endTime": str(row.endtime) if row.endtime else None,
            "duration": (row.endtime - row.starttime).total_seconds() if row.endtime else None
        }
    })

    # Add relationships
    # 1. Ride STARTS_AT Station
    relationships.append({
        "id": f"rs-{row.rideid}",
        "type": "STARTS_AT",
        "startNode": ride_id,
        "endNode": f"s-{row.start_stationid}",
        "properties": {}
    })

    # 2. Ride ENDS_AT Station (if available)
    if row.end_stationid is not None:
        relationships.append({
            "id": f"re-{row.rideid}",
            "type": "ENDS_AT",
            "startNode": ride_id,
            "endNode": f"s-{row.end_stationid}",
            "properties": {}
        })

    # 3. Ride USES Vehicle
    relationships.append({
        "id": f"rv-{row.rideid}",
        "type": "USES",
        "startNode": ride_id,
        "endNode": f"v-{row.vehicleid}",
        "properties": {}
    })

    # 4. Ride TAKEN_BY User
    relationships.append({
        "id": f"ru-{row.rideid}",
        "type": "TAKEN_BY",
        "startNode": ride_id,
        "endNode": f"u-{row.userid}",
        "properties": {}
    })

# Combine nodes and relationships into a graph structure
graph_data = {
    "nodes": nodes,
    "relationships": relationships
}

# Save as JSON
with open("rides_graph.json", "w") as f:
    json.dump(graph_data, f, indent=2, default=str)

print(f"Graph data exported with {len(nodes)} nodes and {len(relationships)} relationships")
spark.stop()

