#!/bin/bash
source /etc/profile
if [ $# -eq 1 ]; then
  datetime1=$1
else
  datetime1=$(date +%Y%m%d -d  '-1 days')
fi

nohup spark-submit --class com.bianxianmao.offlinemodel.deploy.AdvertFMApp \
--master yarn \
--deploy-mode client \
--num-executors 10 \
--driver-memory 2g \
--executor-memory 12g \
--executor-cores  6 \
/home/dc/fmmodel/offlinemodel_deploy-3.1.0.jar \
--modelKeyId  mid_fm_ctr_v001 \
--partitionNums 60 \
--batchSize 30000 \
--sep '\t' \
--env pred \
--inputTraining  hdfs://nameservice1/home/dc/warehouse/model_sample/online_train/stat_date=$(date "+%Y-%m-%d") > \
/home/dc/fmmodel/logs/fm_model.log 2>&1 &
