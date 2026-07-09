-- 1
SELECT vu.City,
       vu.Zipcode,
       COUNT(r.rideId) AS ride_count
FROM rides r
         JOIN subscriptions s ON r.SubscriptionId = s.subscriptionid
         JOIN velo_users vu ON s.UserId = vu.userid
WHERE r.StartTime >= s.ValidFrom
GROUP BY vu.City, vu.Zipcode
ORDER BY ride_count DESC;
-- 2a
SELECT AVG(user_avg) AS overall_avg_ride_duration_minutes
FROM (
         SELECT
             s.UserId,
             AVG(EXTRACT(EPOCH FROM (r.EndTime - r.StartTime)) / 60) AS user_avg
         FROM rides r
                  JOIN subscriptions s ON r.SubscriptionId = s.subscriptionid
         WHERE r.EndTime IS NOT NULL
           AND EXTRACT(EPOCH FROM (r.EndTime - r.StartTime)) >= 0
         GROUP BY s.UserId
     ) AS per_user;


-- 2b

-- 3
SELECT
    CASE
        WHEN EXTRACT(DOW FROM r.StartTime) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
        END AS day_type,
    COUNT(*) AS ride_count
FROM rides r
         JOIN subscriptions s ON r.SubscriptionId = s.subscriptionid
GROUP BY day_type;
-- 4
SELECT
    TO_CHAR(StartTime, 'Day') AS day_of_week,
    AVG(EXTRACT(EPOCH FROM (EndTime - StartTime)) / 60) AS avg_ride_duration_minutes
FROM rides
WHERE EndTime IS NOT NULL
  AND EXTRACT(EPOCH FROM (EndTime - StartTime)) > 0
  AND SubscriptionId IS NOT NULL
GROUP BY day_of_week
ORDER BY avg_ride_duration_minutes DESC;
-- 5
WITH ride_lock_usage AS (
    -- Gather lock usages from ride start and end points
    SELECT Startlockid AS lockid
    FROM rides
    WHERE Startlockid IS NOT NULL
    UNION ALL
    SELECT EndLockId AS lockid
    FROM rides
    WHERE EndLockId IS NOT NULL
),
     station_usage AS (
         -- Count usage per station by joining with locks table
         SELECT l.stationid, COUNT(*) AS usage_count
         FROM ride_lock_usage rlu
                  JOIN locks l ON rlu.lockid = l.lockid
         GROUP BY l.stationid
     )
SELECT s.StationId,
       s.StationNr,
       s.Street,
       COALESCE(su.usage_count, 0) AS usage_count
FROM stations s
         LEFT JOIN station_usage su ON s.StationId = su.stationid
ORDER BY usage_count DESC;
-- 7
SELECT
    bt.biketypedescription,
    COUNT(r.rideId) AS ride_count
FROM rides r
         JOIN vehicles v ON r.VehicleId = v.vehicleid
         JOIN bikelots bl ON v.BikeLotId = bl.bikelotid
         JOIN bike_types bt ON bl.biketypeid = bt.biketypeid
WHERE subscriptionid is not null
AND endtime > r.starttime
GROUP BY bt.biketypedescription
ORDER BY ride_count DESC;
-- 8
WITH ride_counts AS (
    SELECT
        s.UserId,
        COUNT(r.rideId) AS total_rides
    FROM rides r
             JOIN subscriptions s ON r.SubscriptionId = s.subscriptionid
    WHERE r.subscriptionid is not null
    GROUP BY s.UserId
)
SELECT
    COUNT(CASE WHEN total_rides > 1 THEN 1 END) * 100.0 / COUNT(*) AS retention_rate_percentage
FROM ride_counts;
-- 9
WITH ride_speeds AS (
    SELECT
        v.vehicleid,
        bl.biketypeid,
        -- Calculate Euclidean distance (in kilometers)
        (SQRT(
                 POWER(r.StartPoint[0] - r.EndPoint[0], 2) + -- Difference in X
                 POWER(r.StartPoint[1] - r.EndPoint[1], 2)    -- Difference in Y
         ) * 111.32) AS distance_km, -- Approximation of distance in km (1 degree ~ 111.32 km)
        EXTRACT(EPOCH FROM (r.EndTime - r.StartTime)) / 3600 AS duration_hours -- Convert seconds to hours
    FROM rides r
             JOIN vehicles v ON r.VehicleId = v.vehicleid
             JOIN bikelots bl ON v.BikeLotId = bl.bikelotid
             JOIN subscriptions s ON r.SubscriptionId = s.subscriptionid -- Ensure the ride is from a subscribed user
    WHERE r.StartTime IS NOT NULL
      AND r.EndTime IS NOT NULL
      AND EXTRACT(EPOCH FROM (r.EndTime - r.StartTime)) > 0 -- Exclude rides with negative or zero duration
)
SELECT
    bt.biketypedescription,
    AVG(rs.distance_km / NULLIF(rs.duration_hours, 0)) AS avg_speed_kmh
FROM ride_speeds rs
         JOIN bike_types bt ON rs.biketypeid = bt.biketypeid
WHERE rs.distance_km IS NOT NULL AND rs.duration_hours > 0
GROUP BY bt.biketypedescription
ORDER BY avg_speed_kmh DESC;

-- 10
WITH ride_distances AS (
    SELECT
        v.vehicleid,
        bl.biketypeid,
        -- Calculate Euclidean distance (in kilometers)
        (SQRT(
                 POWER(r.StartPoint[0] - r.EndPoint[0], 2) + -- Difference in X
                 POWER(r.StartPoint[1] - r.EndPoint[1], 2)    -- Difference in Y
         ) * 111.32) AS distance_km -- Approximation of distance in km (1 degree ~ 111.32 km)
    FROM rides r
             JOIN vehicles v ON r.VehicleId = v.vehicleid
             JOIN bikelots bl ON v.BikeLotId = bl.bikelotid
             JOIN subscriptions s ON r.SubscriptionId = s.subscriptionid -- Ensure the ride is from a subscribed user
    WHERE r.StartTime IS NOT NULL
      AND r.EndTime IS NOT NULL
      AND EXTRACT(EPOCH FROM (r.EndTime - r.StartTime)) > 0 -- Exclude rides with negative or zero duration
)
SELECT
    bt.biketypedescription,
    AVG(rd.distance_km) AS avg_distance_km
FROM ride_distances rd
         JOIN bike_types bt ON rd.biketypeid = bt.biketypeid
GROUP BY bt.biketypedescription
ORDER BY avg_distance_km DESC;

-- most revenue
SELECT
    st.description AS subscription_type,
    COUNT(s.subscriptionid) AS num_subscriptions,
    SUM(EXTRACT(EPOCH FROM (r.EndTime - r.StartTime)) / 3600) * 10 AS estimated_revenue -- Assumes $10 per hour for the ride
FROM subscriptions s
         JOIN subscription_types st ON s.SubscriptionTypeId = st.subscriptiontypeid
         JOIN rides r ON s.subscriptionid = r.SubscriptionId
WHERE s.ValidFrom <= CURRENT_DATE
GROUP BY st.description
ORDER BY estimated_revenue DESC;

--What is the average ride frequency per user over time?
SELECT
    EXTRACT(YEAR FROM r.StartTime) AS ride_year,
    COUNT(r.rideId) AS total_rides,
    COUNT(DISTINCT s.UserId) AS total_users,
    COUNT(r.rideId) / NULLIF(COUNT(DISTINCT s.UserId), 0) AS avg_rides_per_user
FROM rides r
         JOIN subscriptions s ON r.SubscriptionId = s.subscriptionid
WHERE r.StartTime IS NOT NULL
  AND r.EndTime IS NOT NULL
  AND EXTRACT(EPOCH FROM (r.EndTime - r.StartTime)) > 0 -- Ensure positive ride duration
GROUP BY ride_year
ORDER BY ride_year DESC;












