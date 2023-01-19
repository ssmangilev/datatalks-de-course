
select * from yellow_taxi_trips 
-- 20530
select distinct count(*) from yellow_taxi_trips where date_trunc('day', lpep_pickup_datetime) = '2019-01-15' and date_trunc('day', lpep_dropoff_datetime) = '2019-01-15';

-- 2019-01-15 00:00:00
select max(trip_distance) as trip_distance, date_trunc('day', lpep_pickup_datetime) from yellow_taxi_trips ytd 
where date_trunc('day', lpep_pickup_datetime) in ('2019-01-18', '2019-01-28', '2019-01-15', '2019-01-10')
group by date_trunc('day', lpep_pickup_datetime);

-- 1282
select count(*) from yellow_taxi_trips ytd where passenger_count = 2
and date_trunc('day', lpep_pickup_datetime) = '2019-01-01';
-- 254
select count(*) from yellow_taxi_trips ytd where passenger_count = 3
and date_trunc('day', lpep_pickup_datetime) = '2019-01-01';
-- Long Island City/Queens Plaza
select max(y.tip_amount), z2."Zone" from yellow_taxi_trips y , zones z, zones z2
where y."PULocationID" = z."LocationID"
and y."DOLocationID" = z2."LocationID"
and z."LocationID" = 7
group by z2."Zone"
order by max(y.tip_amount) desc
limit 1;