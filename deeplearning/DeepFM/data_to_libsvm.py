#hive数据转换为libsvm    --pyspark执行
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
import datetime
import os
conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)
sc.stop()
sc = SparkContext(conf=conf)
today_0 = (datetime.date.today()).strftime('%Y%m%d')
today = (datetime.date.today()).strftime('%Y%m%d')
yesterday = (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
a = list(today)
a.insert(4, '-')
a.insert(7, '-')
today = ''.join(a)
hive_context = HiveContext(sc)
sql = '''SELECT
*
FROM ads.tmp_ctr_model_sample_train
where pt=\'{}\'
AND bxm_id is not null
AND bxm_id != 'b'
'''.format(today)

df = hive_context.sql(sql)

#column_name_dict = open('/home/dc/test/deeplearning/DeepFM/column_name_dict.txt', 'r')
#column_name_dict = eval(column_name_dict.read())
column_name_dict = {'position': 'f2006', 'pre_id': 'f1001', 'ticket_tag_1': 'f1002', 'ticket_tag_2': 'f1003', 'ticket_tag_3': 'f1004', 'advertiser_id': 'f1005', 'account_type': 'f1006', 'agent_id': 'f1007', 'asset_id': 'f1008', 'source_type': 'f1009', 'media_id': 'f2001', 'media_class_id': 'f2002', 'media_child_class_id': 'f2003', 'provider_id': 'f2004', 'activityid': 'f3001', 'ip_pre': 'f4003', 'appos': 'f4004', 'os_type': 'f4005', 'device_type': 'f4008', 'device_brand': 'f4009', 'isp_domain': 'f4012', 'province': 'f4015', 'city': 'f4016', 'hour': 'f4017', 'week': 'f4018', 'user_current_activity_bxm_id_rank': 'f6001', 'user_day_ticket_repeat_rank': 'f6003', 'user_current_activity_ticket_repeat_rank': 'f6019', 'browser': 'f4007', 'media_sys_type': 'f2005', 'position_entrance': 'f2007', 'activity_type': 'f3002', 'is_venue': 'f3003', 'is_automatic': 'f3004', 'launch_control': 'f3005', 'ticket_stat_ctr': 'f5001', 'media_stat_ctr': 'f5002', 'position_stat_ctr': 'f5003', 'activity_stat_ctr': 'f5004', 'ticket_media_stat_ctr': 'f5005', 'ticket_position_stat_ctr': 'f5006', 'ticket_activity_stat_ctr': 'f5007', 'provider_stat_ctr': 'f5008', 'ticket_provider_stat_ctr': 'f5009', 'user_day_bxm_id_rank': 'f6002', 'user_day_activity_bxm_id_rank': 'f6004', 'user_bxm_id_rank': 'f6005', 'user_activity_bxm_id_rank': 'f6006', 'user_day_last_activityid_if_same': 'f6007', 'user_day_last_pre_id_if_same': 'f6008', 'user_last_bxm_id_if_click': 'f6015', 'user_activity_last_bxm_id_if_click': 'f6016', 'user_activity_last_bxm_id_intervel_type': 'f6017', 'user_last_bxm_id_intervel_type': 'f6018', 'hw': 'f4013', 'device_name': 'f4010'}
#column_name_list = ['position', 'pre_id', 'ticket_tag_1', 'ticket_tag_2', 'advertiser_id', 'account_type', 'agent_id', 'media_id', 'media_class_id', 'media_child_class_id', 'provider_id', 'activityid', 'ip_pre', 'appos', 'os_type', 'device_type', 'device_brand', 'isp_domain', 'province', 'city', 'hour', 'week', 'user_current_activity_bxm_id_rank', 'user_day_ticket_repeat_rank', 'browser', 'media_sys_type', 'position_entrance', 'activity_type', 'is_venue', 'is_automatic', 'launch_control', 'ticket_stat_ctr', 'media_stat_ctr', 'position_stat_ctr', 'activity_stat_ctr', 'ticket_media_stat_ctr', 'ticket_position_stat_ctr', 'ticket_activity_stat_ctr', 'provider_stat_ctr', 'ticket_provider_stat_ctr', 'user_day_bxm_id_rank', 'user_day_activity_bxm_id_rank', 'user_bxm_id_rank', 'user_activity_bxm_id_rank', 'user_day_last_activityid_if_same', 'user_day_last_pre_id_if_same', 'user_last_bxm_id_if_click', 'user_activity_last_bxm_id_if_click', 'user_activity_last_bxm_id_intervel_type', 'user_last_bxm_id_intervel_type', 'device_name']

with open ('/home/dc/deeplearning/DeepFM/field_name.txt', 'r') as f:
	column_name_list = eval(f.read())

column_name_list_sql = df.columns
drop_list = []
drop_list_index = []
for k in column_name_list_sql:
	if k not in column_name_list:
		drop_list.append(k)

for k in drop_list:
	drop_list_index.append(column_name_list_sql.index(k))

drop_list_index.sort(reverse=True)

one_hot_filename = '/home/dc/deeplearning/DeepFM/one_hot_dict/one_hot_dict_{}.txt'.format(today_0)
one_hot_dict = open(one_hot_filename, 'r')
one_hot_dict = eval(one_hot_dict.read())

hdfs_list_train = []
#hdfs://nameservice1/home/dc/warehouse/model_sample/train/stat_date=2019-12-25/part-00003-085a7cd4-e98b-4839-8a32-cbb86b41d7d1.csv
command = "hadoop fs -ls hdfs://nameservice1/home/dc/warehouse/model_sample/train/stat_date={}/".format(today)
result = os.popen(command).readlines()
for i in range(2, len(result)):
	tmp_list = list(result[i].split(" ")[-1])
	del tmp_list[-1]
	hdfs_list_train.append("".join(tmp_list))

command_mkdir = "/home/dc/deeplearning/DeepFM/data/{}".format(today_0)
if os.path.isdir(command_mkdir):
	command_remove = "rm -rf {}".format(command_mkdir)
	os.system(command_remove)
	os.mkdir(command_mkdir)
else:
	os.mkdir(command_mkdir)

def reverse_train_data(hdfs_path, n):
	data_ = sc.textFile(hdfs_path)
	data = data_.collect()
	file_name = "/home/dc/deeplearning/DeepFM/data/{1}/tr{0}_{1}.libsvm".format(n, today_0)
	f = open(file_name, 'w')
	for i in range(len(data)):
		tmp_list = data[i].split("\t")
		label = str(tmp_list[0])
		tmp_list_f = []
		# ['ctr_label', 'bxm_id', 'position', 'sample_date', 'pt']
		# [0, 1, 15, 58, 59]
		#del tmp_list[59]
		#del tmp_list[58]
		#del tmp_list[15]
		#del tmp_list[1]
		#del tmp_list[0]
		for k in drop_list_index:
			del tmp_list[k]
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

for ii, cn in enumerate(hdfs_list_train):
	reverse_train_data(cn, ii)



hdfs_list_test = []
command_2 = "hadoop fs -ls hdfs://nameservice1/home/dc/warehouse/model_sample/test/stat_date={}/".format(today)
result_2 = os.popen(command_2).readlines()
for i in range(2, len(result_2)):
	tmp_list = list(result_2[i].split(" ")[-1])
	del tmp_list[-1]
	hdfs_list_test.append("".join(tmp_list))


def reverse_test_data(hdfs_path, n):
	data_ = sc.textFile(hdfs_path)
	data = data_.collect()
	file_name = "/home/dc/deeplearning/DeepFM/data/{1}/va{0}_{1}.libsvm".format(n, today_0)
	f = open(file_name, 'w')
	for i in range(len(data)):
		tmp_list = data[i].split("\t")
		label = str(tmp_list[0])
		tmp_list_f = []
		#del tmp_list[59]
		#del tmp_list[58]
		#del tmp_list[15]
		#del tmp_list[1]
		#del tmp_list[0]
		for k in drop_list_index:
			del tmp_list[k]
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

for ii, cn in enumerate(hdfs_list_test):
	reverse_test_data(cn, ii)




