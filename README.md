# RBDA_project

# Jiraphon's code
| Data source  | Information |
| ------------- | ------------- |
| Covid search data | This aggregated, anonymized dataset shows trends in search patterns for symptoms and is intended to help researchers to better understand the impact of COVID-19. For more information, click [here](https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-search-trends.md)  |
| Covid government response  | Summary of a government's response to the events. For more information, click [here](https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-government-response.md)  |
| Covid cases  | Information related to the COVID-19 infections for each date-region pair. For more information, click [here](https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-epidemiology.md) |

### To run mapreduce code
```
cd dome/src
sh run.sh
```
### To run Hive query
```
cd dome/scripts
hive -f query_state.hql
hive -f query.hql
```
## Weather Data
The input weather datasets include air quality index (AQI), temperature (TEMP) and barometric pressure (PRESS).
The output records of MR contain the fields: local date, state code, state name, AQI, temperature, pressure.



## Tweets Data
The input tweets datasets include tweets ID and sentiment score of each tweet.
The output of MapReduce jobs contains date and average tweets sentiment score from that date.

## Join table format for tweets and Covid
The join table is out.txt. The columns are as follows:
- Date 
- newCovidCases (int)
- newRecoveryCases (int)
- googleSearchTop5 (string) 
- schoolClosingPolicy (float)
- workClosingPolicy (float)
- incomeSupport (float)
- debtSupport (float) 
- testingPolicy (float)
- contactPolicy (float)
- facialCoveringPolicy (float)
- tweetsScore (float)
