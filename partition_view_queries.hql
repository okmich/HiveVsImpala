-- give the avg fligh delay by month in the year 2006 --query runs average (1.95s)
select year, month, avg(arrdelay) arrdelay, avg(depdelay) depdelay from pq_flight where year = 2006
group by year, month;
--
select year, month, avg(arrdelay) arrdelay, avg(depdelay) depdelay from pq_flight_part where year in(2003,2004, 2005)
group by year, month;
--
select year, month, avg(arrdelay) arrdelay, avg(depdelay) depdelay from pq_flight_part where year between 2003 and 2005
group by year, month;



create external table pq_flight_part 
    (month tinyint, dayofmonth tinyint,dayofweek tinyint,
    deptime smallint, crsdeptime smallint, arrtime smallint, crsarrtime smallint, 
    uniquecarrier string, flightnum string, tailnum string, actualelapsedtime smallint,
    crselapsedtime smallint, airtime smallint, arrdelay smallint, depdelay smallint, 
    origin string, dest string, distance smallint, taxiin string, taxiout string,
    cancelled string, cancellationcode string, diverted string, carrierdelay smallint,
    weatherdelay smallint, nasdelay smallint, securitydelay smallint, lateaircraftdelay smallint)
partitioned by (year smallint)
stored as parquet
location '/user/cloudera/output/airline/pq_flight_part';


alter table pq_flight_part add partition(year=2003);
alter table pq_flight_part add partition(year=2004);
alter table pq_flight_part add partition(year=2005);
alter table pq_flight_part add partition(year=2006);
alter table pq_flight_part add partition(year=2007);
alter table pq_flight_part add partition(year=2008);

set mapred.reduce.tasks=1;
insert overwrite table pq_flight_part partition (year=2004)
select distinct
month, dayofmonth,dayofweek, deptime, crsdeptime, arrtime, crsarrtime, 
    uniquecarrier, flightnum, tailnum, actualelapsedtime,
    crselapsedtime, airtime, arrdelay, depdelay, 
    origin, dest, distance, taxiin, taxiout,
    cancelled, cancellationcode, diverted, carrierdelay,
    weatherdelay, nasdelay, securitydelay, lateaircraftdelay
    from pq_flight where year = 2004;


insert overwrite table pq_flight_part partition (year)
select 
month, dayofmonth,dayofweek, deptime, crsdeptime, arrtime, crsarrtime, 
    uniquecarrier, flightnum, tailnum, actualelapsedtime,
    crselapsedtime, airtime, arrdelay, depdelay, 
    origin, dest, distance, taxiin, taxiout,
    cancelled, cancellationcode, diverted, carrierdelay,
    weatherdelay, nasdelay, securitydelay, lateaircraftdelay, year
    from pq_flight where year != 2003;


create view v_flights_denom as 
select year,month, cast(concat(cast(year as string), cast(month as string)) as smallint) yearmonth, dayofmonth,
        case dayofweek 
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Fridday'
            when 6 then 'Saturday'
            when 6 then 'Sunday'
            else 'Unknown'
        end dayofweek, 
        flightnum, deptime, crsdeptime, arrtime, crsarrtime, 
      actualelapsedtime,
    crselapsedtime, airtime, arrdelay, depdelay, 
    origin, dest, distance, taxiin, taxiout,
    cancelled, cancellationcode, diverted, carrierdelay,
    weatherdelay, nasdelay, securitydelay, lateaircraftdelay
    ,pao.airport origin_airport, pao.city origin_city, pao.state origin_state, pao.country origin_country
    ,pad.airport dest_airport, pad.city dest_city, pad.state dest_state, pad.country dest_country
    ,pf.uniquecarrier, pc.description carrier   
    ,pf.tailnum, ppi.type plane_type, ppi.manufacturer, ppi.issue_date, 
    if ((pf.year - cast(substr(issue_date, 7,4) as int) < 0), 0, pf.year - cast(substr(issue_date, 7,4) as int)) age_of_plane, 
    ppi.model, ppi.status, ppi.aircraft_type, ppi.pyear
    from pq_flight_part pf
left join pq_carriers pc on pc.cdde = pf.uniquecarrier
left join pq_plane_info ppi on ppi.tailnum = pf.tailnum
left join pq_airports pao on pao.iata = pf.origin
left join pq_airports pad on pad.iata = pf.dest;


create view v_flight_2003 as 
select * from v_flights_denom where year = 2003;

create view v_flight_2003_2005 as 
select * from v_flights_denom where year in (2003,2004,2005);