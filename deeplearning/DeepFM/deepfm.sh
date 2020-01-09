#!/bin/bash
set -e
today=$(date "+%Y%m%d")
data_dir=$"/home/dc/deeplearning/DeepFM/data/"$today
spark-submit --master yarn-client --driver-memory 2G --executor-memory 2G --num-executors 4 /home/dc/deeplearning/DeepFM/make_one_hot_dict.py
python3 /home/dc/deeplearning/DeepFM/dict_to_mongo.py
spark-submit --master yarn-client --driver-memory 2G --executor-memory 2G --num-executors 4 /home/dc/deeplearning/DeepFM/data_to_libsvm.py
feature_size=$(python3 /home/dc/deeplearning/DeepFM/cal_one_hot_dict_length.py)
field_size=$(python3 /home/dc/deeplearning/DeepFM/cal_field_size.py)
python3 /home/dc/deeplearning/DeepFM/DeepFM_tf.py \
--task_type=train \
--learning_rate=0.0005 \
--optimizer=Adam \
--num_epochs=2 \
--batch_size=1024 \
--field_size=$field_size \
--feature_size=$feature_size \
--deep_layers=400,400,400 \
--dropout=0.5,0.5,0.5 \
--log_steps=30 \
--num_threads=12 \
--model_dir=/home/dc/deeplearning/DeepFM/model_ckpt/model_ckpt \
--data_dir=$data_dir \
--clear_existing_model=True 
#> ./deepfm_model_train.log 2>&1 &
python3 /home/dc/deeplearning/DeepFM/DeepFM_tf.py \
--task_type=eval \
--learning_rate=0.0005 \
--optimizer=Adam \
--num_epochs=2 \
--batch_size=1024 \
--field_size=$field_size \
--feature_size=$feature_size \
--deep_layers=400,400,400 \
--dropout=0.5,0.5,0.5 \
--log_steps=30 \
--num_threads=12 \
--model_dir=/home/dc/deeplearning/DeepFM/model_ckpt/model_ckpt \
--data_dir=$data_dir 
#> ./deepfm_model_eval.log 2>&1 &
#python3 /home/dc/deeplearning/DeepFM/DeepFM_tf.py --task_type=infer --learning_rate=0.0005 --optimizer=Adam --num_epochs=2 --batch_size=1024 --field_size=$field_size --feature_size=$feature_size --deep_layers=400,400,400 --dropout=0.5,0.5,0.5 --log_steps=1024 --num_threads=12 --model_dir=/home/dc/deeplearning/DeepFM/model_ckpt/model_ckpt --data_dir=$data_dir
servable_model_dir=$"/home/dc/deeplearning/DeepFM/servable_model/""servable_model_"$today
if [ -d $servable_model_dir ];then
rm -rf /home/dc/deeplearning/DeepFM/servable_model/old/
mv $servable_model_dir /home/dc/deeplearning/DeepFM/servable_model/old
mkdir $servable_model_dir
else
mkdir $servable_model_dir
fi
python3 /home/dc/deeplearning/DeepFM/DeepFM_tf.py \
--task_type=export \
--learning_rate=0.0005 \
--optimizer=Adam \
--num_epochs=2 \
--batch_size=1024 \
--field_size=$field_size \
--feature_size=$feature_size \
--deep_layers=400,400,400 \
--dropout=0.5,0.5,0.5 \
--log_steps=30 \
--num_threads=12 \
--model_dir=/home/dc/deeplearning/DeepFM/model_ckpt/model_ckpt \
--servable_model_dir=$servable_model_dir
