# Realtime and Big Data Analytics Project CSCI-GA.2436
This study examines the relationship between weather patterns and the spread of COVID-19 using MapReduce, Hive, and Tableau. Our results show that while a negative correlation between COVID-19 cases and factors such as temperature and air quality may exist, it is not significant due to the influence of other factors that affect social distancing behaviors. Furthermore, we found that the relationship between COVID-19 cases and sentiment expressed in tweets is not significant due to the presence of confounding variables. These findings highlight the need for further research to fully understand the complex relationship between weather and the spread of COVID-19.

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
# Yiyi's code
## Weather Data
The input weather datasets include air quality index (AQI), temperature (TEMP) and barometric pressure (PRESS).
The output records of MR contain the fields: local date, state code, state name, AQI, temperature, pressure.

### To run mapreduce code
```
cd Yiyi/src
sh run.sh
```
### To run Hive query
```
cd Yiyi/scripts
hive -f query.hql
```
# Wenni's code
## Tweets Data
The input tweets datasets include tweets ID and sentiment score of each tweet.
The output of MapReduce jobs contains date and average tweets sentiment score from that date.

### To run mapreduce code
```
hadoop fs -mkdir TweetsProject
javac -classpath `hadoop classpath` TweetsMapper.java
javac -classpath `hadoop classpath` TweetsReducer.java
javac -classpath `hadoop classpath`:. Tweets.java
jar cvf TweetsAnalysis.jar *.class
hadoop fs -put combined-tweets-files.csv TweetsProject
hadoop jar TweetsAnalysis.jar Tweets TweetsProject/combined-tweets-files.csv TweetsProject/output
```
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
