DROP TABLE IF EXISTS rides CASCADE ;
DROP TABLE IF EXISTS vehicles CASCADE ;
DROP TABLE IF EXISTS locks CASCADE ;
DROP TABLE IF EXISTS subscriptions CASCADE ;
DROP TABLE IF EXISTS stations CASCADE ;
DROP TABLE IF EXISTS velo_users CASCADE ;
DROP TABLE IF EXISTS bikelots CASCADE ;
DROP TABLE IF EXISTS subscription_types CASCADE ;
DROP TABLE IF EXISTS bike_types CASCADE ;

CREATE TABLE bike_types
(
    biketypeid INTEGER NOT NULL
        CONSTRAINT pk_biketypes PRIMARY KEY,
    biketypedescription varchar(200)
) ;

CREATE TABLE subscription_types
(
    subscriptiontypeid INTEGER NOT NULL
        CONSTRAINT pk_subscriptiontypes PRIMARY KEY,
    description varchar(50)) ;

CREATE TABLE bikelots(
     bikelotid INTEGER
         GENERATED ALWAYS AS IDENTITY
         CONSTRAINT pk_bikelots PRIMARY KEY,
     deliverydate DATE,
     biketypeid INTEGER
         REFERENCES bike_types(biketypeid));

CREATE TABLE velo_users
(    userid INTEGER
         GENERATED ALWAYS AS IDENTITY
         CONSTRAINT pk_velo_users PRIMARY KEY,
    Name        varchar(100),
    Email       varchar(100) NOT NULL,
    Street      varchar(100),
    Number      varchar(10),
    Zipcode     varchar(10),
    City        varchar(100),
    Country_Code varchar(3));


CREATE TABLE stations
(
    StationId INTEGER
        GENERATED ALWAYS AS IDENTITY
        CONSTRAINT pk_stations PRIMARY KEY,
    ObjectId    VARCHAR(20) NOT NULL,
    StationNr   VARCHAR(20) NOT NULL,
    Type        VARCHAR(20) NOT NULL,
    Street      VARCHAR(100) NOT NULL,
    Number      VARCHAR(10) NULL,
    ZipCode     VARCHAR(10) NOT NULL,
    District    VARCHAR(100) NOT NULL,
    GPSCoord    point,
    AdditionalInfo  varchar(100),
    LabelId     INTEGER,
    CityId      INTEGER);

CREATE TABLE subscriptions
(   subscriptionid INTEGER
        GENERATED ALWAYS AS IDENTITY
        CONSTRAINT pk_subscriptions PRIMARY KEY,
    ValidFrom   DATE NOT NULL,
    SubscriptionTypeId INTEGER NOT NULL,
    UserId INTEGER NOT NULL
);

CREATE TABLE locks
(
    lockid INTEGER
        GENERATED ALWAYS AS IDENTITY
        CONSTRAINT pk_locks PRIMARY KEY,
    stationlocknr   INTEGER NOT NULL,
    stationid       INTEGER NOT NULL,
    vehicleid       INTEGER NULL);

CREATE TABLE vehicles
(
    vehicleid INTEGER
        GENERATED ALWAYS AS IDENTITY
        CONSTRAINT pk_vehicles PRIMARY KEY,
    SerialNumber        VARCHAR(20) NOT NULL,
    BikeLotId           INTEGER NOT NULL,
    LastMaintenanceOn   TIMESTAMP,
    LockId              INTEGER,
    position            POINT
);

CREATE TABLE rides
(
    rideId INTEGER
        GENERATED ALWAYS AS IDENTITY
        CONSTRAINT pk_rides PRIMARY KEY,
    StartPoint      POINT,
    EndPoint        POINT,
    StartTime       TIMESTAMP,
    EndTime         TIMESTAMP,
    VehicleId       INTEGER,
    SubscriptionId  INTEGER,
    Startlockid     INTEGER,
    EndLockId       INTEGER
    );

