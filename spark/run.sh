#!/bin/bash

JARS=/usr/local/spark/jars/aws-java-sdk-1.7.4.jar
JARS=$JARS,/usr/local/spark/jars/hadoop-aws-2.7.3.jar
JARS=$JARS,/usr/local/spark/jars/postgresql-42.2.6.jar

#PROJECT_DIR=/usr/local/spark/windexplorer
PROJECT_DIR=/home/ubuntu/programs/windexplorer/

echo "Project Directory: ${PROJECT_DIR}"

# Environment variables for spark application
export PYSPARK_PYTHON=/usr/bin/python

# TODO(): Figure out a way to set this for all slaves spark-env.sh?  PYWTK_CACHE_DIR=/home/ubuntu/data

SPARK_MASTER_IP=10.0.0.8
SPARK_MASTER_PORT=7077
SPARK_MASTER="spark://${SPARK_MASTER_IP}:${SPARK_MASTER_PORT}"

echo "SPARK MASTER: ${SPARK_MASTER}"

SPARK_SUBMIT=/usr/local/spark/bin/spark-submit

if [ -z "$1" ]; then
    echo "No process specified. Please specify the job (job_load_site_info.py, job_site_data_hdf5_to_parquet.py, job_process_site_data.py, job_site_score.py)"
    exit 1
fi

# Run spark jobs to load site meta data, convert hdf5 to parquet, parquet to aggregate and save in postgre-sql
LOAD_SITE_INFO_JOB=${PROJECT_DIR}/spark/job_load_site_info.py
LOAD_SITE_DATA_JOB=${PROJECT_DIR}/spark/job_site_data_hdf5_to_parquet.py
PROCESS_DATA_JOB=${PROJECT_DIR}/spark/job_process_site_data.py
PROCESS_SITE_SCORE_JOB=${PROJECT_DIR}/spark/job_site_score.py

#$SPARK_SUBMIT --master "$SPARK_MASTER" --jars "$JARS" ${LOAD_SITE_INFO_JOB}
#$SPARK_SUBMIT --master "$SPARK_MASTER" --jars "$JARS" ${LOAD_SITE_DATA_JOB}
#$SPARK_SUBMIT --master "$SPARK_MASTER" --jars "$JARS" ${PROCESS_DATA_JOB}
$SPARK_SUBMIT --master "$SPARK_MASTER" --jars "$JARS" ${PROCESS_SITE_SCORE_JOB}
