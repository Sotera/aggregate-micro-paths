use ${hiveconf:database};
drop table ais_small;
create external table ais_small
(
  ais_type string,
  mmsi string,
  name string,
  imo string,
  latitude string,
  longitude string,
  speed string,
  heading string,
  time string,
  dest string,
  eta string
)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ','
location '/tmp/ais_smallone/';

drop table ais_small_final;
create table ais_small_final as
select trim(mmsi) as mmsi, trim(latitude) as latitude, trim(longitude) as longitude, concat('20',substr(trim(time),1,2),'-',substr(trim(time),3,2),'-',substr(trim(time),5,2),' ',substr(trim(time),7,2),':',substr(trim(time),9,2),':00') as dt
from ais_small;
