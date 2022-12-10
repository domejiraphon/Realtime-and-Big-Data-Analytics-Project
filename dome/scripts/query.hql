DROP TABLE cases;
DROP TABLE trends;
DROP TABLE response;
DROP TABLE joinTable;
DROP TABLE tweets;

CREATE EXTERNAL TABLE cases (case_date date, newCases int, recoveryCases int)
    row format delimited fields terminated by ','
    location '/user/jy3694_nyu_edu/hivecases/';

CREATE EXTERNAL TABLE  trends (trends_date date, trend string)
    row format delimited fields terminated by ','
    location '/user/jy3694_nyu_edu/hiveTrends/';

CREATE EXTERNAL TABLE  response (response_date date, schoolClosing float, work_closing float, incomeSupport float, 
          debtSupport float, testingPolicy float, contactPolicy float, facialCoveringPolicy float)
    row format delimited fields terminated by ','
    location '/user/jy3694_nyu_edu/hiveResponse/';

CREATE EXTERNAL TABLE  tweets (tweets_date date, score float)
    row format delimited fields terminated by ','
    location '/user/jy3694_nyu_edu/hiveTweets/';


CREATE TABLE joinTable AS SELECT * FROM cases 
    JOIN trends ON (cases.case_date = trends.trends_date)
    JOIN response ON (trends.trends_date = response.response_date)
    JOIN tweets ON (tweets.tweets_date = response.response_date);

INSERT OVERWRITE DIRECTORY 'hiveCases/' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM joinTable;