#!bin/bash
source /etc/profile

spark-submit --master yarn-client --driver-memory 2G --executor-memory 2G --executor-cores 1 --num-executors 2 /home/dc/fmmodel/daily_precision_recall.py
