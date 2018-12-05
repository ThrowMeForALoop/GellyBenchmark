#!/bin/bash
if [ -z "$1" ]
  then
    echo "Please input path to property file for dataset"
    exit 1
fi

cp $1 src/main/resources/config.properties 
mvn clean package 


