# Velo Bike-Sharing Data Analytics Pipeline

End-to-end data engineering project for a bike-sharing system (based on Antwerp's Velo). Implements an operational OLTP database, a star-schema data warehouse, Apache Spark ETL pipelines, graph export for Neo4j, Parquet export, and a Power BI report.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────┐
│  SQL Server  │────▶│  PostgreSQL  │────▶│   PySpark    │────▶│ PostgreSQL│
│  (Source)    │     │  OLTP (5433) │     │   ETL Jobs   │     │ DWH (5435)│
└─────────────┘     └──────────────┘     └──────────────┘     └───────────┘
                                                  │                  │
                                                  ▼                  ▼
                                           ┌──────────┐     ┌──────────────┐
                                           │  Neo4j   │     │    Parquet   │
                                           │  Graph   │     │    Export    │
                                           └──────────┘     └──────────────┘
                                                  │
                                                  ▼
                                           ┌──────────┐
                                           │ Power BI │
                                           │  Report  │
                                           └──────────┘
```

## Project Structure

```
Data&AI4/
├── veloDB/                         # Operational PostgreSQL database (OLTP)
│   ├── docker-compose.yml          # PostgreSQL 17.2 on port 5433
│   ├── veloscripts/
│   │   ├── 01_CRE_VELO.sql         # Schema creation (9 tables)
│   │   ├── 02_FILL_VELO.sql        # Seed data: stations, users, vehicles, rides
│   │   ├── 03_fill_velo_users.sql
│   │   ├── 04_fill_subscriptions.sql
│   │   ├── 05_fill_vehicles.sql
│   │   ├── 06_fill_locks.sql
│   │   ├── 07_fill_rides.sql
│   │   ├── 08_constraints.sql      # Foreign key constraints
│   │   ├── analysis.sql            # Analytical queries (ride patterns, retention, revenue)
│   │   └── conversions_SQL_SERVER.sql  # SQL Server → PostgreSQL migration scripts
│   └── .gitignore
│
├── SparkDelta/                     # Apache Spark + Delta Lake ETL
│   ├── docker-compose.yml          # PostgreSQL DWH on port 5435
│   ├── veloproject/                # ★ Main project deliverable
│   │   ├── py/
│   │   │   ├── S1_01_DIM_DATE.py               # Date dimension ETL (2015-2024)
│   │   │   ├── S1_02_DIM_WEATHER.py            # Weather dimension ETL
│   │   │   ├── S1_03_PROCESS_WEATHER_DATA.py   # Process raw weather JSON → staging
│   │   │   ├── S1_04_DIM_VEHICLE.py            # Vehicle dimension ETL (source→DWH)
│   │   │   ├── S2_01_DIM_USER.py               # User dimension ETL (SCD Type 2)
│   │   │   ├── S2_02_DIM_LOCK.py               # Lock/station dimension ETL
│   │   │   ├── S2_03_FACT_RIDE.py              # Fact ride table ETL
│   │   │   ├── setup_source_db_functions.py    # Create haversine distance function
│   │   │   ├── parquet_export.py               # Full DWH export to Parquet
│   │   │   └── neo4J_Json_export.py            # Graph export (Districts→Stations→Rides→Users→Vehicles)
│   │   ├── sql/
│   │   │   ├── createStarScheme.sql      # Star schema DDL
│   │   │   └── 01_CRE_VELO.sql          # OLTP schema (copy)
│   │   ├── py/data/weather/             # Raw weather JSON data files (per zipcode)
│   │   └── parquet_files/               # Exported Parquet output
│   │       ├── dim_date/
│   │       ├── dim_vehicle/
│   │       └── ...
│   ├── ConnectionConfig.py         # Spark session config & environment setup
│   ├── config.ini                  # DB connection profiles
│   ├── requirements.txt            # Python dependencies
│   ├── FileStore/tables/           # Sample data (employees, transactions, Shakespeare)
│   └── *.ipynb                     # Course notebooks: Spark basics → SQL → Delta → DWH → Kafka
│
├── velo4/                          # IntelliJ IDE project (star schema DDL)
│   ├── scripts/sql/create.sql      # Star schema DDL
│   └── data/weather_types.csv      # Weather classification data
│
└── week5report.pbix                # Power BI report
```

## Data Warehouse: Star Schema

| Table | Type | Description |
|---|---|---|
| `dim_date` | Dimension | Date attributes (year, quarter, month, day, weekday flag), 2015–2024 |
| `dim_weather` | Dimension | Weather types: Pleasant, Unpleasant, Neutral, Unknown |
| `dim_vehicle` | Dimension | Bike types (Velo Bike, Velo E-Bike, Step, Scooter) |
| `dim_user` | Dimension | User demographics with SCD (start/end date, is_current) |
| `dim_lock` | Dimension | Station/lock location details (street, district, GPS) |
| `fact_ride` | Fact | Ride metrics (distance, duration), FKs to all dimensions |

## Analytical Insights

Queries in `analysis.sql` cover:
- **Ride volume** by city and zip code
- **Average ride duration** per user
- **Weekday vs weekend** usage patterns
- **Station popularity** ranking
- **Bike type performance** (speed, distance by type)
- **User retention** rate
- **Revenue estimation** by subscription type
- **Average ride frequency** per user over time

## Tech Stack

| Component | Technology |
|---|---|
| Processing | Apache Spark 3.5.2, Delta Lake 3.2.0 |
| Databases | PostgreSQL 17.2 (OLTP + DWH) |
| Orchestration | PySpark ETL scripts |
| Streaming | Apache Kafka (examples) |
| Graph DB | Neo4j (JSON export) |
| Storage | Parquet, Delta Lake |
| Visualization | Power BI (week5report.pbix) |
| Containerization | Docker Compose |

## Getting Started

### Prerequisites
- Python 3.11
- Java 8–17
- Apache Spark 3.5.2 + Hadoop 3.4.0
- Docker

### Setup
1. **Start the databases:**
   ```bash
   cd veloDB && docker-compose up -d
   cd ../SparkDelta && docker-compose up -d
   ```

2. **Initialize the OLTP schema and data:**
   ```bash
   psql -h localhost -p 5433 -U postgres -d veloDB -f veloDB/veloscripts/01_CRE_VELO.sql
   psql -h localhost -p 5433 -U postgres -d veloDB -f veloDB/veloscripts/02_FILL_VELO.sql
   # Run remaining fill scripts (03–07) and 08_constraints.sql
   ```

3. **Run the DWH ETL pipeline (in order):**
   ```bash
   cd SparkDelta/veloproject/py
   python setup_source_db_functions.py        # Create haversine function in source DB
   python S1_01_DIM_DATE.py                   # Generate date dimension (2015-2024)
   python S1_02_DIM_WEATHER.py                # Create weather dimension
   python S1_03_PROCESS_WEATHER_DATA.py       # Process weather JSON → staging
   python S1_04_DIM_VEHICLE.py                # Extract vehicles → DWH dimension
   python S2_01_DIM_USER.py                   # Extract users with SCD Type 2
   python S2_02_DIM_LOCK.py                   # Extract stations/locks → dimension
   python S2_03_FACT_RIDE.py                  # Build fact ride table with metrics
   ```

4. **Export to Parquet / Neo4j:**
   ```bash
   python parquet_export.py
   python neo4J_Json_export.py
   ```

5. **Power BI:** Open `week5report.pbix` in Power BI Desktop.

### Python Dependencies
```
pandas==2.2.2
numpy==2.0.2
pyspark==3.5.2
delta-spark==3.2.0
kafka-python==2.0.2
```

## Notes
- OLTP DB runs on port **5433**, DWH DB on port **5435**
- Neo4j graph export produces `rides_graph.json` with ~15000 rides as a property graph
- The project was migrated from SQL Server to PostgreSQL (see `conversions_SQL_SERVER.sql`)
