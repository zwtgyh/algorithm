from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)
sc.stop()
sc = SparkContext(conf=conf)

#生成one_hot_dict
hive_context = HiveContext(sc)
sql = '''SELECT
*
FROM ads.tmp_ctr_model_sample_train
where pt='2019-12-25'
AND bxm_id is not null
AND bxm_id != 'b'
'''
df = hive_context.sql(sql)
column_name_list = df.columns
# ['ctr_label', 'bxm_id', 'position', 'sample_date', 'pt']
# [0, 1, 15, 58, 59]
del column_name_list[59]
del column_name_list[58]
del column_name_list[15]
del column_name_list[1]
del column_name_list[0]

one_hot_dict = {}
i = 0

for column_name in column_name_list:
	tmp_list = eval("df.select(\'{}\').distinct().collect()".format(column_name))
	tmp_list = [n[0].lower() for n in tmp_list]
	tmp_list = list(set(tmp_list))
	column_name_code = column_name_dict.get(column_name)
	for id, cn in enumerate(tmp_list):
		if cn == "":
			cn = "null"
		tmp_name = "{}_{}".format(column_name_code, cn)
		one_hot_dict[tmp_name] = i
		i += 1
	tmp_name = "{}_null".format(column_name_code)
	if one_hot_dict.get(tmp_name) == None:
		one_hot_dict[tmp_name] = i
		i += 1


with open('/home/dc/deeplearning/DeepFM/one_hot_dict.txt', 'w') as f:
	f.write(str(one_hot_dict))
	f.close()



#将Hive数据转换为libsvm
column_name_dict = open('/home/dc/deeplearning/DeepFM/column_name_dict.txt', 'r')
column_name_dict = eval(column_name_dict.read())

one_hot_dict = open('/home/dc/deeplearning/DeepFM/one_hot_dict.txt', 'r')
one_hot_dict = eval(one_hot_dict.read())



data_ = sc.textFile('hdfs://nameservice1/home/dc/warehouse/model_sample/test/stat_date=2019-12-25/part-00003-085a7cd4-e98b-4839-8a32-cbb86b41d7d1.csv')
data = data_.collect()

# ['ctr_label', 'bxm_id', 'position', 'sample_date', 'pt']
# [0, 1, 15, 58, 59]

f = open("/home/dc/deeplearning/DeepFM/data/va3.libsvm", "w")

for i in range(len(data)):
	tmp_list = data[i].split("\t")
	label = str(tmp_list[0])
	tmp_list_f = []
	del tmp_list[59]
	del tmp_list[58]
	del tmp_list[15]
	del tmp_list[1]
	del tmp_list[0]
	for ii, cn in enumerate(tmp_list):
		column_name_code = column_name_dict.get(column_name_list[ii])
		if cn == "":
			cn = "null"
		name = "{}_{}".format(column_name_code, cn.lower())
		name_ = "{}_null".format(column_name_code)
		one_hot = one_hot_dict.get(name, one_hot_dict.get(name_))
		tmp_list_f.append(one_hot)
	tmp_list_f = ["{}:1".format(iii) for iii in tmp_list_f]
	tmp_list_f.insert(0, label)
	if i == len(data)-1:
		f.write(" ".join(tmp_list_f))
	else:
		f.write(" ".join(tmp_list_f))
		f.write("\n")


f.close()





#!/bin/bash
spark-submit --master yarn-client --driver-memory 2G --executor-memory 2G --num-executors 4 /home/dc/test/deeplearning/DeepFM/make_one_hot_dict.py
spark-submit --master yarn-client --driver-memory 2G --executor-memory 2G --num-executors 4 /home/dc/test/deeplearning/DeepFM/data_to_libsvm.py
feature_size=$(python3 /home/dc/test/deeplearning/DeepFM/cal_one_hot_dict_length.py)
python3 /home/dc/test/deeplearning/DeepFM/DeepFM_tf.py --task_type=train --clear_existing_model=True --learning_rate=0.0005 --optimizer=Adam --num_epochs=3 --batch_size=1024 --field_size=55 --feature_size=$feature_size --deep_layers=400,400,400 --dropout=0.5,0.5,0.5 --log_steps=1024 --num_threads=12 --model_dir=/home/dc/test/deeplearning/DeepFM/model_ckpt --data_dir=/home/dc/test/deeplearning/DeepFM/data





#训练模式
python3 DeepFM_tf.py --task_type=train --clear_existing_model=True --learning_rate=0.0005 --optimizer=Adam --num_epochs=3 --batch_size=256 --field_size=55 --feature_size=9391 --deep_layers=521,256,128 --dropout=0.55,0.65,0.75 --log_steps=256 --num_threads=12 --model_dir=/home/dc/deeplearning/DeepFM/model_ckpt --data_dir=/home/dc/deeplearning/DeepFM/data
#评估模式
python3 DeepFM_tf.py --task_type=eval --learning_rate=0.0005 --optimizer=Adam --num_epochs=3 --batch_size=256 --field_size=55 --feature_size=9391 --deep_layers=521,256,128 --dropout=0.55,0.65,0.75 --log_steps=256 --num_threads=12 --model_dir=/home/dc/deeplearning/DeepFM/model_ckpt --data_dir=/home/dc/deeplearning/DeepFM/data







