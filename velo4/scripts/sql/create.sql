CREATE TABLE DateDim
(
    date_sk    SERIAL PRIMARY KEY,
    date       DATE NOT NULL,
    year       INT  NOT NULL,
    quarter    INT,
    mont_nr    INT,
    month_name VARCHAR(20),
    day_nr     INT,
    day_name   VARCHAR(20),
    is_weekday BOOLEAN
);

CREATE TABLE WeatherDim (
                            weather_id SERIAL PRIMARY KEY,
                            weather_type VARCHAR(50) NOT NULL CHECK (
                                weather_type IN ('Pleasant', 'Unpleasant', 'Neutral', 'Unknown')
                                )
);
INSERT INTO WeatherDim(weather_type)
VALUES
    ('Pleasant'),
    ('Unpleasant'),
    ('Neutral'),
    ('Unknown');


CREATE TABLE VehicleDim
(
    vehicle_id SERIAL PRIMARY KEY,
    type       VARCHAR(50)
);

CREATE TABLE UserDim (
                         user_sk SERIAL PRIMARY KEY,
                         user_id VARCHAR(50) NOT NULL,
                         street VARCHAR(100),
                         number VARCHAR(10),
                         zipcode VARCHAR(20),
                         city VARCHAR(50),
                         country_code VARCHAR(10),
                         effective_date DATE NOT NULL,
                         end_date DATE,
                         is_current BOOLEAN DEFAULT TRUE
);


CREATE TABLE LockDim (
                         lock_id SERIAL PRIMARY KEY,
                         station_id VARCHAR(50),
                         station_nr INT,
                         street VARCHAR(100),
                         number VARCHAR(10),
                         zipcode VARCHAR(20),
                         district VARCHAR(50),
                         gps_coord VARCHAR(100)
);
INSERT INTO LockDim (station_id, station_nr, street, number, zipcode, district, gps_coord)
VALUES ('NOLOCK', NULL, 'N/A', 'N/A', '00000', 'N/A', '0,0');

CREATE TABLE RidesFact (
                           ride_id SERIAL PRIMARY KEY,

    -- Foreign Keys to Dimension Tables
                           user_sk INT NOT NULL REFERENCES UserDim(user_sk),
                           start_lock_id INT NOT NULL REFERENCES LockDim(lock_id),
                           end_lock_id INT NOT NULL REFERENCES LockDim(lock_id),
                           date_sk INT NOT NULL REFERENCES DateDim(date_sk),
                           weather_id INT NOT NULL REFERENCES WeatherDim(weather_id),
                           vehicle_id INT NOT NULL REFERENCES VehicleDim(vehicle_id),

    -- Metrics
                           ride_distance DECIMAL(10, 2) NOT NULL,  -- in kilometers or meters
                           ride_duration INT NOT NULL              -- in minutes or seconds
);


