# RBDA_project
## Join table format 
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

## Weather Data
The input weather datasets include air quality index (AQI), temperature (TEMP) and barometric pressure (PRESS).
The output records of MR contain the fields: local date, state code, state name, AQI, temperature, pressure.

## Covid search data
This aggregated, anonymized dataset shows trends in search patterns for symptoms and is intended to help researchers to better understand the impact of COVID-19.
```
https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-search-trends.md
```

## Covid government response
Summary of a government's response to the events
```
https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-government-response.md
```

## Covid 
Information related to the COVID-19 infections for each date-region pair.
```
https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-epidemiology.md
```