#!/bin/bash
if [ -z "$1" ]
  then
    echo "Please input path to property file for dataset"
    exit 1
fi

cat $1 > src/main/resources/config.properties
#cp $1 src/main/resources/config.properties 
mvn clean package 


