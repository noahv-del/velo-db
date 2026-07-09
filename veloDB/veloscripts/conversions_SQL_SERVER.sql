-- CONVERSION SQL SERVER TO POSTGRESS
SELECT CONCAT('INSERT INTO velo_users (name,email,street,number,zipcode,city,country_code) VALUES (''',
              [Name], ''' ,'''
           ,[Email], ''' ,'''
           ,REPLACE([Street],'''', ''''''), ''' ,'''
           ,[Number], ''' ,'''
           ,[Zipcode], ''' ,'''
           ,[City], ''' ,'''
           ,[CountryCode],
              ''');')
FROM [Velo].[dbo].[Users];


SELECT CONCAT('INSERT INTO stations (name,email,street,number,zipcode,city,country_code) VALUES (''',
              [Name], ''' ,'''
           ,[Email], ''' ,'''
           ,REPLACE([Street],'''', ''''''), ''' ,'''
           ,[Number], ''' ,'''
           ,[Zipcode], ''' ,'''
           ,[City], ''' ,'''
           ,[CountryCode],
              ''');')
FROM [Velo].[dbo].[Users];

INSERT INTO stations (objectid, stationnr,type,street,number,zipcode,district, gpscoord , additionalinfo, labelid, cityid) VALUES ('33204' ,'020' ,'ENKELZIJDIG' ,'Groenkerkhofstraat (2000)' ,'2' ,'2000' ,'ANTWERPEN' ,POINT(51.2187,4.40066) ,'thv Nationalestraat' ,NULL ,NULL);

SELECT  CONCAT(
                'INSERT INTO stations (objectid, stationnr,type,street,number,zipcode,district, gpscoord , additionalinfo, labelid, cityid) VALUES (''',
                ObjectId,''' ,''',
                StationNr, ''' ,''',
                Type, ''' ,''',
                [Street], ''' ,''',
                [Number], ''' ,''',
                ZipCode, ''' ,''',
                District, ''' ,POINT(',
                GPSCoord.STX, ' ,',
                GPSCoord.STY , ') ,''',
                AdditionalInfo, ''' , NULL, NULL );')
FROM stations;


SELECT CONCAT('INSERT INTO subscriptions (ValidFrom,SubscriptionTypeId,UserId) VALUES (TO_DATE(''',
              ValidFrom, ''',''YYYY-MM-DD'') ,'''
           ,SubscriptionTypeId, ''' ,'''
           ,UserId,
              ''');')
FROM subscriptions;

SELECT CONCAT('INSERT INTO locks (StationLockNr,StationId,VehicleId) VALUES (',
              StationLockNr, ' ,',
              StationId, ' ,',
              COALESCE(CAST(VehicleId as varchar), 'NULL'),
              ');')
FROM locks;


SELECT  CONCAT(
                'INSERT INTO stations (objectid, stationnr,type,street,number,zipcode,district, gpscoord , additionalinfo, labelid, cityid) VALUES (''',
                ObjectId,''' ,''',
                StationNr, ''' ,''',
                Type, ''' ,''',
                [Street], ''' ,''',
                [Number], ''' ,''',
                ZipCode, ''' ,''',
                District, ''' ,POINT(',
                GPSCoord.STX, ' ,',
                GPSCoord.STY , ') ,''',
                AdditionalInfo, ''' , NULL, NULL );')
FROM stations;


SELECT  CONCAT( 'INSERT INTO vehicles (serialNumber, bikelotid,lastmaintenanceon,lockid,position) VALUES (''',
                SerialNumber,''' ,''',
                BikeLotId,''' ,TO_TIMESTAMP(''',
                CONVERT(VARCHAR, LastMaintenanceOn, 20) ,''',''YYYY-MM-DD HH24:MI:SS'') ,',
                COALESCE(CAST(LockId as varchar), 'NULL'), ' ,POINT(',
                point.STX, ' ,',
                point.STY , '));')
FROM vehicles;


SELECT CONCAT('INSERT INTO rides (startpoint, endpoint,starttime,endtime,vehicleid,subscriptionid,startlockid,endlockid) VALUES (POINT(',
              StartPoint.STX, ' ,',
              StartPoint.STY , '),POINT(',
              EndPoint.STX, ' ,',
              EndPoint.STY , '),TO_TIMESTAMP(''',
              CONVERT(VARCHAR, StartTime, 20) ,''',''YYYY-MM-DD HH24:MI:SS'') ,TO_TIMESTAMP(''',
              CONVERT(VARCHAR, EndTime, 20) ,''',''YYYY-MM-DD HH24:MI:SS'') ,',
              COALESCE(CAST(VehicleId as varchar), 'NULL'), ' ,',
              COALESCE(CAST(SubscriptionId as varchar), 'NULL'), ' ,',
              COALESCE(CAST(Startlockid as varchar), 'NULL'), ' ,',
              COALESCE(CAST(EndLockId as varchar), 'NULL') , ');')
FROM rides;




UPDATE rides
SET rides.endtime = ride_end.new_end_time
FROM rides
         JOIN
     (SELECT	ride.RideId,
                ride.distance,
                ride.distance/1000 distance_km,
                (ride.distance/(4+(CRYPT_GEN_RANDOM(2) % 300)/100.00))/(60*60*24) time_days,  --riding betwwen 15 and 25 Kph in minutes
                ride.StartTime,
                ride.StartTime+(ride.distance/(4+(CRYPT_GEN_RANDOM(2) % 300)/100.00))/(60*60*24) new_end_time --riding betwwen 15 and 25 Kph
      FROM
          (
              SELECT
                  r.RideId,
                  r.StartTime,
                  r.EndTime,
                  r.VehicleId ,
                  geography::STGeomFromText([startpoint].STAsText(),4326) .STDistance(geography::STGeomFromText([endpoint].STAsText(),4326)) distance
              FROM rides r) as ride) as ride_end
     ON rides.RideId = ride_end.RideId;

update Rides set StartTime = dateadd(year, 4,StartTime)
update Rides set EndTime = dateadd(year, 4,EndTime)
update Subscriptions set ValidFrom =dateadd(year, 4,ValidFrom)
update Vehicles set LastMaintenanceOn = dateadd(year,4, LastMaintenanceOn)
update Bikelots set DeliveryDate = dateadd(year,4, DeliveryDate)



SELECT * FROM rides;

-- Ondernomen stappen:
-- eerste alle tabellen overgezet van SQLSERVER images nar postgreSQL scripts
-- TODO update rides end time met 4 jaar
-- ride end_time terug opnieuw berekend

-- Alle FKs er terug op gelegd

