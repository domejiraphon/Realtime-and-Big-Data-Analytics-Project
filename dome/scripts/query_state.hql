DROP TABLE cases;
DROP TABLE response;
DROP TABLE byState;

CREATE EXTERNAL TABLE cases (case_date date, case_state string, newCases int, recoveryCases int)
    row format delimited fields terminated by ','
    location '/user/jy3694_nyu_edu/hiveStateCases/';

CREATE EXTERNAL TABLE  response (response_date date, response_state string, 
          schoolClosing float, work_closing float, incomeSupport float, 
          debtSupport float, testingPolicy float, contactPolicy float, 
          facialCoveringPolicy float)
    row format delimited fields terminated by ','
    location '/user/jy3694_nyu_edu/hiveStateResponses/';

CREATE EXTERNAL TABLE  weather (air_date date, stateCode string, weather_state string, AQI float, TEMP float, PRESS float)
    row format delimited fields terminated by ','
    location '/user/jy3694_nyu_edu/hiveStateWeather/';


CREATE TABLE byState AS SELECT * FROM cases 
    JOIN response ON (cases.case_date = response.response_date AND cases.case_state = response.response_state)
    JOIN weather ON (response.response_date = weather.air_date AND response.response_state = weather.weather_state);

INSERT OVERWRITE DIRECTORY 'byState/' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM byState;