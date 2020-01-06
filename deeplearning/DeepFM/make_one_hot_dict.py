#生成one_hot_dict    --pyspark执行
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import lower
import datetime
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
#column_name_list = df.columns
# ['ctr_label', 'bxm_id', 'position', 'sample_date', 'pt']
# [0, 1, 15, 58, 59]
#del column_name_list[59]
#del column_name_list[58]
#del column_name_list[15]
#del column_name_list[1]
#del column_name_list[0]
#column_name_list = ['position', 'pre_id', 'ticket_tag_1', 'ticket_tag_2', 'advertiser_id', 'account_type', 'agent_id', 'media_id', 'media_class_id', 'media_child_class_id', 'provider_id', 'activityid', 'ip_pre', 'appos', 'os_type', 'device_type', 'device_brand', 'isp_domain', 'province', 'city', 'hour', 'week', 'user_current_activity_bxm_id_rank', 'user_day_ticket_repeat_rank', 'browser', 'media_sys_type', 'position_entrance', 'activity_type', 'is_automatic', 'launch_control', 'ticket_stat_ctr', 'media_stat_ctr', 'position_stat_ctr', 'activity_stat_ctr', 'ticket_media_stat_ctr', 'ticket_position_stat_ctr', 'ticket_activity_stat_ctr', 'provider_stat_ctr', 'ticket_provider_stat_ctr', 'user_day_bxm_id_rank', 'user_day_activity_bxm_id_rank', 'user_bxm_id_rank', 'user_activity_bxm_id_rank', 'user_day_last_activityid_if_same', 'user_day_last_pre_id_if_same', 'user_last_bxm_id_if_click', 'user_activity_last_bxm_id_if_click', 'user_activity_last_bxm_id_intervel_type', 'user_last_bxm_id_intervel_type', 'device_name']
#column_name_list = ['position', 'pre_id', 'ticket_tag_1', 'ticket_tag_2', 'advertiser_id', 'account_type', 'agent_id', 'media_id', 'media_class_id', 'media_child_class_id', 'provider_id', 'activityid', 'ip_pre', 'appos', 'os_type', 'device_type', 'device_brand', 'isp_domain', 'province', 'city', 'hour', 'week', 'user_current_activity_bxm_id_rank', 'user_day_ticket_repeat_rank', 'browser', 'media_sys_type', 'position_entrance', 'activity_type', 'is_venue', 'is_automatic', 'launch_control', 'ticket_stat_ctr', 'media_stat_ctr', 'position_stat_ctr', 'activity_stat_ctr', 'ticket_media_stat_ctr', 'ticket_position_stat_ctr', 'ticket_activity_stat_ctr', 'provider_stat_ctr', 'ticket_provider_stat_ctr', 'user_day_bxm_id_rank', 'user_day_activity_bxm_id_rank', 'user_bxm_id_rank', 'user_activity_bxm_id_rank', 'user_day_last_activityid_if_same', 'user_day_last_pre_id_if_same', 'user_last_bxm_id_if_click', 'user_activity_last_bxm_id_if_click', 'user_activity_last_bxm_id_intervel_type', 'user_last_bxm_id_intervel_type', 'device_name']

with open('/home/dc/deeplearning/DeepFM/field_name.txt', 'r') as f:
        column_name_list = eval(f.read())

#column_name_dict = open('/home/dc/test/deeplearning/DeepFM/column_name_dict.txt', 'r')
#column_name_dict = eval(column_name_dict.read())
column_name_dict = {'position': 'f2006', 'pre_id': 'f1001', 'ticket_tag_1': 'f1002', 'ticket_tag_2': 'f1003', 'ticket_tag_3': 'f1004', 'advertiser_id': 'f1005', 'account_type': 'f1006', 'agent_id': 'f1007', 'asset_id': 'f1008', 'source_type': 'f1009', 'media_id': 'f2001', 'media_class_id': 'f2002', 'media_child_class_id': 'f2003', 'provider_id': 'f2004', 'activityid': 'f3001', 'ip_pre': 'f4003', 'appos': 'f4004', 'os_type': 'f4005', 'device_type': 'f4008', 'device_brand': 'f4009', 'isp_domain': 'f4012', 'province': 'f4015', 'city': 'f4016', 'hour': 'f4017', 'week': 'f4018', 'user_current_activity_bxm_id_rank': 'f6001', 'user_day_ticket_repeat_rank': 'f6003', 'user_current_activity_ticket_repeat_rank': 'f6019', 'browser': 'f4007', 'media_sys_type': 'f2005', 'position_entrance': 'f2007', 'activity_type': 'f3002', 'is_venue': 'f3003', 'is_automatic': 'f3004', 'launch_control': 'f3005', 'ticket_stat_ctr': 'f5001', 'media_stat_ctr': 'f5002', 'position_stat_ctr': 'f5003', 'activity_stat_ctr': 'f5004', 'ticket_media_stat_ctr': 'f5005', 'ticket_position_stat_ctr': 'f5006', 'ticket_activity_stat_ctr': 'f5007', 'provider_stat_ctr': 'f5008', 'ticket_provider_stat_ctr': 'f5009', 'user_day_bxm_id_rank': 'f6002', 'user_day_activity_bxm_id_rank': 'f6004', 'user_bxm_id_rank': 'f6005', 'user_activity_bxm_id_rank': 'f6006', 'user_day_last_activityid_if_same': 'f6007', 'user_day_last_pre_id_if_same': 'f6008', 'user_last_bxm_id_if_click': 'f6015', 'user_activity_last_bxm_id_if_click': 'f6016', 'user_activity_last_bxm_id_intervel_type': 'f6017', 'user_last_bxm_id_intervel_type': 'f6018', 'hw': 'f4013', 'device_name': 'f4010'}


one_hot_dict = {}
i = 0
for column_name in column_name_list:
	#tmp_list = eval("df.select(\'{}\').distinct().collect()".format(column_name))
	#tmp_list = [n[0].lower() for n in tmp_list]
	#tmp_list = list(set(tmp_list))
	tmp_list = eval("df.select(lower(df[\'{}\'])).distinct().collect()".format(column_name))
	column_name_code = column_name_dict.get(column_name)
	for id, nn in enumerate(tmp_list):
		cn = nn[0]
		if cn == "":
			cn = "null"
		tmp_name = "{}_{}".format(column_name_code, cn)
		one_hot_dict[tmp_name] = i
		i += 1
	tmp_name = "{}_null".format(column_name_code)
	if one_hot_dict.get(tmp_name) == None:
		one_hot_dict[tmp_name] = i
		i += 1

one_hot_filename = '/home/dc/deeplearning/DeepFM/one_hot_dict/one_hot_dict_{}.txt'.format(today_0)
with open(one_hot_filename, 'w') as f:
	f.write(str(one_hot_dict))






