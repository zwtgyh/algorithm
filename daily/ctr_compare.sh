#!bin/bash
source /etc/profile

spark-submit --master yarn-client --driver-memory 2G --executor-memory 1G --num-executors 4 /home/dc/fmmodel/ctr_compare.py
