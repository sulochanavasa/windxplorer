#!/bin/bash

JARS=/usr/local/spark/jars/aws-java-sdk-1.7.4.jar
JARS=$JARS,/usr/local/spark/jars/hadoop-aws-2.7.3.jar
JARS=$JARS,/usr/local/spark/jars/postgresql-42.2.6.jar

#PROJECT_DIR=/usr/local/spark/windexplorer
PROJECT_DIR=/home/ubuntu/programs/windexplorer/

echo "Project Directory: ${PROJECT_DIR}"

# Environment variables for spark application
export PYSPARK_PYTHON=/usr/bin/python

PYWTK_CACHE_DIR=/home/ubuntu/data

SPARK_MASTER_IP=10.0.0.8
SPARK_MASTER_PORT=7077
SPARK_MASTER="spark://${SPARK_MASTER_IP}:${SPARK_MASTER_PORT}"

echo "SPARK MASTER: ${SPARK_MASTER}"

SPARK_SUBMIT=/usr/local/spark/bin/spark-submit

if [ -z "$1" ]; then
    echo "No process specified. Please specify the job (e.g. job_load_site_info.py)"
    exit 1
fi

# Run spark jobs to load site meta data, convert hdf5 to parquet, parquet to aggregate and save in postgre-sql
LOAD_SITE_INFO_JOB=${PROJECT_DIR}/spark/job_load_site_info.py

$SPARK_SUBMIT --master "$SPARK_MASTER" --jars "$JARS" --conf PYWTK_CACHE_DIR=${PYWTK_CACHE_DIR} ${LOAD_SITE_INFO_JOB}
