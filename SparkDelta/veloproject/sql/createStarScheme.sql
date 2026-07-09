CREATE TABLE dim_date
(
    date_sk    SERIAL PRIMARY KEY,
    date       DATE NOT NULL,
    year       INT  NOT NULL,
    quarter    INT,
    month_nr   INT,
    month_name VARCHAR(20),
    day_nr     INT,
    day_name   VARCHAR(20),
    is_weekday BOOLEAN
);

CREATE TABLE dim_weather
(
    weather_id   SERIAL PRIMARY KEY,
    weather_type VARCHAR(50) NOT NULL CHECK (
        weather_type IN ('Pleasant', 'Unpleasant', 'Neutral', 'Unknown')
        )
);
INSERT INTO dim_weather(weather_type)
VALUES ('Pleasant'),
       ('Unpleasant'),
       ('Neutral'),
       ('Unknown');


CREATE TABLE dim_vehicle
(
    vehicle_id SERIAL PRIMARY KEY,
    type       VARCHAR(50)
);

CREATE TABLE dim_user
(
    user_sk      SERIAL PRIMARY KEY,
    user_id      VARCHAR(50) NOT NULL,
    street       VARCHAR(100),
    number       VARCHAR(100),
    zipcode      VARCHAR(100),
    city         VARCHAR(100),
    country_code VARCHAR(100),
    start_date   TIMESTAMP   NOT NULL,
    end_date     TIMESTAMP   NOT NULL,
    is_current   BOOLEAN     NOT NULL
);


CREATE TABLE dim_lock
(
    lock_id    SERIAL PRIMARY KEY,
    station_id VARCHAR(50),
    station_nr INT,
    street     VARCHAR(100),
    number     VARCHAR(10),
    zipcode    VARCHAR(20),
    district   VARCHAR(50),
    gps_coord  VARCHAR(100)
);
INSERT INTO dim_lock (station_id, station_nr, street, number, zipcode, district, gps_coord)
VALUES ('NOLOCK', NULL, 'N/A', 'N/A', '00000', 'N/A', '0,0');

CREATE TABLE fact_ride
(
    ride_id       SERIAL PRIMARY KEY,

    -- Foreign Keys to Dimension Tables
    user_sk       INT            NOT NULL REFERENCES dim_user (user_sk),
    start_lock_id INT            NOT NULL REFERENCES dim_lock (lock_id),
    end_lock_id   INT            NOT NULL REFERENCES dim_lock (lock_id),
    date_sk       INT            NOT NULL REFERENCES dim_date (date_sk),
    weather_id    INT            NOT NULL REFERENCES dim_weather (weather_id),
    vehicle_id    INT            NOT NULL REFERENCES dim_vehicle (vehicle_id),

    -- Metrics
    ride_distance DECIMAL(10, 2) NOT NULL, -- in kilometers or meters
    ride_duration INT            NOT NULL  -- in minutes or seconds
);


