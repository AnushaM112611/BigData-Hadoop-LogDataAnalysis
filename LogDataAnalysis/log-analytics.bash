#!/bin/bash

# Creating Local project files lookup path, Application path and hdfs project path
PROJECT_PATH=/user/anusha/finalProjects/clickStream
APP_PATH=$PROJECT_PATH/oozie
LOCAL_APP_PATH=/home/anusha/finalProjects/ClickStreamLogDataAnalytics/oozie
OOZIE_SERVER=http://172.17.5.4:11000/oozie

# Deleting all the old directories
hdfs dfs -rm -r -f $PROJECT_PATH/*


# Make application path and file holding directories

hdfs dfs -mkdir $APP_PATH
hdfs dfs -mkdir $APP_PATH/lib
hdfs dfs -mkdir $PROJECT_PATH/oozie-coordinator


# Copy required library files, dependencies to hdfs application path
hdfs dfs -put $LOCAL_APP_PATH/hive-site.xml $APP_PATH/
hdfs dfs -put  $LOCAL_APP_PATH/workflow.xml $APP_PATH/
hdfs dfs -put $LOCAL_APP_PATH/coordinator.xml $PROJECT_PATH/oozie-coordinator/
hdfs dfs -put $LOCAL_APP_PATH/lib/* $APP_PATH/lib/
hdfs dfs -put $LOCAL_APP_PATH/*.{hive,pig} $APP_PATH/

# Running the oozie job
oozie job --oozie $OOZIE_SERVER -config $LOCAL_APP_PATH/coordinator.properties -run
