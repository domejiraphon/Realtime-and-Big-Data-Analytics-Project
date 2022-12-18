#!bin/bash

# Transfer Files
gcloud config set project hpc-dataproc-19b8
gcloud config set compute/zone us-central1-f

gcloud compute scp MYFILE nyu-dataproc-m:~/RBDA/project
gcloud compute scp --recurse ./FinalProject/src nyu-dataproc-m:~/RBDA/project

# compile
javac -classpath `hadoop classpath` *.java
jar cvf AirAggregate.jar *.class

# run
hadoop jar AirAggregate.jar AirAggregate project/data/AQI project/data/TEMP project/data/PRESS project/aggregate

# Debug
yarn logs -applicationId {applicationID} -log_files stdout  // view print out

# Get Results from HDFS
hadoop fs -get project/aggregate

javac -classpath `hadoop classpath` covidMapper.java covidAttributes.java
