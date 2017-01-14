use airline;

-- create external table for carriers
create external table pq_carriers (
    cdde varchar(4), 
    description varchar(30)
) 
stored as parquet
location '/user/cloudera/output/airline/carriers';

insert overwrite table pq_carriers 
select * from carriers where cdde != 'Code';


-- create external table for plane information
create external table pq_plane_info (
    tailnum varchar(4), 
    type varchar(30),
    manufacturer string,
    issue_date varchar(16), 
    model varchar(10), 
    status varchar(10),
    aircraft_type varchar(30),
    pyear int
)  
stored as parquet
location '/user/cloudera/output/airline/plane_infos';


insert overwrite table pq_plane_info
select * from plane_info where tailnum != 'tailnum';

-- create external table for airports
create external table pq_airports (
    iata string, 
    airport string, 
    city string,
    state string, 
    country string, 
    geolat float, 
    geolong float
)
stored as parquet
location '/user/cloudera/output/airline/airports';

insert overwrite table pq_airports
select * from airports where iata != 'iata';

-- create external table for flight
-- run this from impala and check the number of output files created on hdfs
create external table pq_flight 
    (year smallint,month tinyint,dayofmonth tinyint,dayofweek tinyint,
    deptime smallint, crsdeptime smallint, arrtime smallint, crsarrtime smallint, 
    uniquecarrier string, flightnum string, tailnum string, actualelapsedtime smallint,
    crselapsedtime smallint, airtime smallint, arrdelay smallint, depdelay smallint, 
    origin string, dest string, distance smallint, taxiin string, taxiout string,
    cancelled string, cancellationcode string, diverted string, carrierdelay smallint,
    weatherdelay smallint, nasdelay smallint, securitydelay smallint, lateaircraftdelay smallint)
stored as parquet
location '/user/cloudera/output/airline/pq_flight';

insert overwrite table pq_flight 
select * from txt_flight;