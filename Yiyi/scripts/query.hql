// create table for air data
create external table air (air_date string, stateCode string, air_state string, AQI float, TEMP float, PRESS float)
    row format delimited fields terminated by ','
    location '/user/yt2239_nyu_edu/hiveAir/';

// create table for covid data
create external table covid (covid_date string, covid_state string, infected int, recovered int)
    row format delimited fields terminated by ','
    location '/user/yt2239_nyu_edu/hiveCovid/';

// join air and covid tables and create a new table
create table air_covid as select * from air, covid
    where air.air_date = covid.covid_date AND air.air_state = covid.covid_state;