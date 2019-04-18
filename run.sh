#!/bin/bash

source /cvmfs/sft.cern.ch/lcg/views/LCG_95/x86_64-centos7-gcc7-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-setconf.sh analytix

site=$1

year=$2

n=$3

spark-submit --master yarn --conf spark.dynamicAllocation.enabled=false --num-executors 1 --executor-memory 8g --executor-cores 16 --driver-memory 4g --conf spark.sql.shuffle.partitions=8 --conf spark.sql.autoBroadcastJoinThreshold=100000000 --conf spark.pyspark.python=/cvmfs/sft.cern.ch/lcg/views/LCG_95/x86_64-centos7-gcc7-opt/bin/python ./final.py $site $year $n
