import psycopg2
import sys

print("Creating haversine function in source database...")

try:
    # Connect to source database
    connection = psycopg2.connect(
        host="localhost",
        port=5433,
        database="veloDB",
        user="postgres",
        password="Student_1234"
    )
    cursor = connection.cursor()
    
    # Create haversine function with DOUBLE PRECISION instead of numeric
    cursor.execute("""
    CREATE OR REPLACE FUNCTION haversine_km(lat1 DOUBLE PRECISION, lon1 DOUBLE PRECISION, 
                                          lat2 DOUBLE PRECISION, lon2 DOUBLE PRECISION)
    RETURNS DOUBLE PRECISION AS $$
    DECLARE
        distance DOUBLE PRECISION;
    BEGIN
        -- Handle case where coordinates are identical
        IF lat1 = lat2 AND lon1 = lon2 THEN
            RETURN 0;
        END IF;
        
        SELECT 6371 * ACOS(
            COS(radians(lat1))
            * COS(radians(lat2))
            * COS(radians(lon2) - radians(lon1))
            + SIN(radians(lat1)) * SIN(radians(lat2)))
        INTO distance;
        RETURN distance;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE;
    """)
    
    connection.commit()
    print("Haversine function created successfully in source database")
    
    cursor.close()
    connection.close()
except Exception as e:
    print(f"Error creating haversine function: {str(e)}")
    sys.exit(1)