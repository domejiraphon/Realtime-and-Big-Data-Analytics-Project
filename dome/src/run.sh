#!bin/bash
javac -classpath `hadoop classpath` covidMapper.java covidAttributes.java
javac -classpath `hadoop classpath` covidReducer.java covidAttributes.java
javac -classpath `hadoop classpath`:. covid.java
jar cvf covid.jar covid.class covidMapper.class covidAttributes.class covidReducer.class
hadoop fs -rm -r output_cases
hadoop jar covid.jar covid data/snippet_epi.csv output_cases

javac -classpath `hadoop classpath` trendMapper.java
javac -classpath `hadoop classpath` trendReducer.java
javac -classpath `hadoop classpath`:. trend.java
jar cvf trend.jar trend.class trendMapper.class trendReducer.class
hadoop fs -rm -r output_trends
hadoop jar trend.jar trend data/snippet_search.csv output data/header.txt 
#hadoop jar trend.jar trend data/trends.csv output data/header.txt 

javac -classpath `hadoop classpath` responseMapper.java responseTuple.java
javac -classpath `hadoop classpath` responseReducer.java responseTuple.java
javac -classpath `hadoop classpath`:. response.java
jar cvf response.jar response.class responseMapper.class responseReducer.class responseTuple.class
hadoop fs -rm -r output_response
hadoop jar response.jar response data/snippet_govern.csv output 
