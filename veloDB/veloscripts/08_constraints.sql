ALTER TABLE rides
    ADD CONSTRAINT fk_rides_locks_end FOREIGN KEY (endlockid)
        REFERENCES Locks(lockid),
    ADD CONSTRAINT fk_rides_locks_start FOREIGN KEY (startlockid)
        REFERENCES Locks(lockid),
    ADD CONSTRAINT fk_rides_subscriptions FOREIGN KEY (subscriptionid)
        REFERENCES subscriptions(subscriptionid),
    ADD CONSTRAINT fk_rides_vehicles FOREIGN KEY (vehicleid)
        REFERENCES vehicles(vehicleid);

ALTER TABLE Bikelots
    ADD CONSTRAINT fk_bikelots_biketypes FOREIGN KEY (biketypeid)
        REFERENCES bike_types(biketypeid);

ALTER TABLE Locks
    ADD CONSTRAINT FK_Locks_Stations FOREIGN KEY (StationId)
        REFERENCES Stations(StationId),
    ADD CONSTRAINT FK_Locks_Vehicles FOREIGN KEY (VehicleId)
        REFERENCES Vehicles(VehicleId);

ALTER TABLE subscriptions
    ADD CONSTRAINT fk_subscriptions_subscriptiontypes FOREIGN KEY (subscriptiontypeid)
        REFERENCES subscription_types(subscriptiontypeid),
    ADD CONSTRAINT fk_subscriptions_users FOREIGN KEY (userid)
        REFERENCES velo_users(userid);

ALTER TABLE vehicles
    ADD CONSTRAINT fk_vehicles_bikelots FOREIGN KEY (bikelotid)
        REFERENCES bikelots(bikelotid),
    ADD CONSTRAINT fk_vehicles_locks FOREIGN KEY (lockid)
        REFERENCES locks(lockid);




