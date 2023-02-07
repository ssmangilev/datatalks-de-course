select count(*) from `instant-pivot-375017.trips_data_all.fhv_external_data_rides`;

SELECT distinct affiliated_base_number FROM `instant-pivot-375017.trips_data_all.fhv_external_data_rides`;
SELECT distinct affiliated_base_number FROM `instant-pivot-375017.trips_data_all.fhv_rides`;

select count(*) from `instant-pivot-375017.trips_data_all.fhv_external_data_rides`
where PUlocationID is Null and DOlocationID is null

CREATE OR REPLACE TABLE `instant-pivot-375017.trips_data_all.fhv_rides_part`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS 
select * from `instant-pivot-375017.trips_data_all.fhv_external_data_rides`;

select distinct affiliated_base_number FROM `instant-pivot-375017.trips_data_all.fhv_rides_part`
where DATE(pickup_datetime) between '2019-03-01' and '2019-03-31'