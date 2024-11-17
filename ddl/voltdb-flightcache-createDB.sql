load classes ../jars/flightcacheprocs.jar;

file voltdb-flightcache-removeDB.sql;
 
Create table city_pair_flight
(dep_city varchar(4) not null 
,arr_city varchar(4) not null 
,flight_date   timestamp not null
,flight_id varchar(7) not null
,tail_number varchar(8) not null 
,primary key(dep_city,arr_city,flight_date,flight_id));

PARTITION TABLE city_pair_flight ON COLUMN dep_city;



Create table flights
(dep_city varchar(4) not null 
,arr_city varchar(4) not null 
,flight_date   timestamp not null
,flight_id varchar(7) not null
,tail_number varchar(8) not null 
,primary key(flight_id,dep_city,arr_city,flight_date));

PARTITION TABLE flights ON COLUMN flight_id;


Create table flight_inventory
(flight_id varchar(7) not null
,flight_date   timestamp not null
,ticket_class varchar(1)
,seat_count bigint not null
,base_price float not null
,primary key(flight_id,flight_date,ticket_class));

PARTITION TABLE flight_inventory ON COLUMN flight_id;

CREATE VIEW city_pairs AS
SELECT dep_city, arr_city, min(flight_date) min_flight_date, max(flight_date) max_flight_date, count(*) how_many
FROM city_pair_flight
GROUP BY dep_city, arr_city;

Create stream flight_sale
PARTITION ON COLUMN flight_id
(flight_id varchar(7) not null
,flight_date   timestamp not null
,ticket_class varchar(1) not null
,ticket_price float not null
,person_id    varchar(30) not null);

Create view flight_sale_summary AS
SELECT flight_id, flight_date, ticket_class, sum(ticket_price) total_sale_value, count(*) tickets_sold
from flight_sale
group by flight_id, flight_date, ticket_class;


CREATE COMPOUND PROCEDURE  FROM CLASS flightcache.CompoundUpsertFlight;

CREATE COMPOUND PROCEDURE  FROM CLASS flightcache.CompoundListFlight;

CREATE PROCEDURE GET_FLIGHTS_FOR_CITY_PAIR 
PARTITION ON TABLE CITY_PAIR_FLIGHT COLUMN dep_city
AS 
SELECT FLIGHT_ID 
FROM CITY_PAIR_FLIGHT 
WHERE dep_city = ? 
AND arr_city = ? 
AND FLIGHT_DATE BETWEEN TRUNCATE(DAY, ?) AND DATEADD(DAY, 1, TRUNCATE(DAY, ?))
ORDER BY FLIGHT_ID;

CREATE PROCEDURE GET_FLIGHT_DETAILS 
PARTITION ON TABLE flight_inventory COLUMN flight_id
AS 
SELECT * 
FROM flight_inventory 
WHERE flight_id = ? 
AND flight_date BETWEEN TRUNCATE(DAY, ?) AND DATEADD(DAY, 1, TRUNCATE(DAY, ?))
ORDER BY ticket_class;









