from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
import numpy as np
import pandas as pd
#计算信息墒
def calculate_entropy(x):
	x_value_list = set([x[i] for i in range(x.shape[0])])
	entropy = 0.0
	for x_value in x_value_list:
		p= float(x[x==x_value].shape[0])/x.shape[0]
		entropy -= p*np.log2(p)
	return entropy
#计算条件墒
def calculate_condition_entropy(x, y):
	x_value_list = set([x[i] for i in range(x.shape[0])])
	condition_entropy = 0.0
	for x_value in x_value_list:
		sub_y = y[x==x_value]
		tmp_entropy = calculate_entropy(sub_y)
		condition_entropy += (float(sub_y.shape[0])/y.shape[0])*tmp_entropy
	return condition_entropy
#计算信息增益
def calculate_entropy_grap(x, y):
	base_entropy = calculate_entropy(y)
	condition_entropy = calculate_condition_entropy(x, y)
	entropy_grap = base_entropy - condition_entropy
	return entropy_grap
#计算信息增益率
def calculate_entropy_grap_radio(x, y):
	entropy_grap = calculate_entropy_grap(x, y)
	entropy_grap_radio = entropy_grap/calculate_entropy(x)
	return entropy_grap_radio
#按列名计算信息增益率
def calculate_column_entropy_grap_radio(data):
	y = data['ctr_label']
	y = np.array(y)
	column_name_list = list(data.columns)
	column_name_list.remove('ctr_label')
	entropy_grap_radio_list = []
	for column_name in column_name_list:
		x = data[column_name]
		x = np.array(x)
		entropy_grap_radio = calculate_entropy_grap_radio(x, y)
		entropy_grap_radio_list.append(entropy_grap_radio)
	dd = {'column_name':column_name_list, 'entropy_grap_radio':entropy_grap_radio_list}
	result = pd.DataFrame(dd)
	result = result.sort_values(by='entropy_grap_radio')
	return result
#读数
conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)
sc.stop()
sc = SparkContext(conf=conf)
hive_context = HiveContext(sc)
sql = '''SELECT
os_type, city, isp_domain, time, user_current_ticket_repeat_rank, hour, price, provider_stat_ctr, asset_id, launch_control, media_sys_type, device_type, user_day_activity_bxm_id_rank, ticket_stat_ctr, ticket_name, week, user_day_ticket_repeat_rank, ticket_media_stat_ctr, ticket_tag_3, province, is_automatic, bxm_id, hw, ip, valid_start_date, user_day_last_activityid, ticket_tag_4, provider_id, ticket_position_stat_ctr, position, ctr_label, position_id, user_day_last_activityid_if_same, ticket_tag, user_activity_last_bxm_id_if_click, activityid, user_day_last_pre_id_if_same, idfa, ticket_type, weekth, spm, uid, sw, user_activity_last_bxm_id_intervel_type, budget_daily, user_day_last_pre_id, pt, media_class_id, imei, advertiser_id, source_type, media_child_class_id, agent_id, browser, pre_id, position_entrance, ticket_activity_stat_ctr, position_stat_ctr, user_last_bxm_id_intervel_type, media_stat_ctr, media_id, ticket_tag_2, net_type, ip_pre, offer_price, user_activity_bxm_id_rank, device_name, appos, month, is_venue, device_brand, account_type, valid_end_date, appkey, activity_type, sh, country, ticket_provider_stat_ctr, user_current_activity_bxm_id_rank, activity_stat_ctr, company, user_bxm_id_rank, user_current_activity_ticket_repeat_rank, user_day_bxm_id_rank, settle_type, user_last_bxm_id_if_click, ticket_tag_1
FROM ads.ad_algorithm_feature_data
where pt='2019-11-30'
AND bxm_id is not null
AND bxm_id != 'b'
'''
df = hive_context.sql(sql)
data = df.toPandas()
_list = list(data)
drop_x_list = ['bxm_id','ip','time','net_type','position','offer_price','device_type','week','ticket_tag_3','ticket_tag_4',
               'idfa','weekth','spm','uid','pt','imei','month','device_name','source_type','appkey']
drop_list = []
for i in range(len(_list)):
    if _list[i] in drop_x_list:
        drop_list.append(i)
    else:
        continue
drop_list.sort(reverse=True)  #drop_list倒置,防止下一步按索引删除列的时候出现索引错位
for i in drop_list:
    del _list[i]
data = data[_list]
data = data.fillna(value='null')
#计算并排序、写入
result = calculate_column_entropy_grap_radio(data)
out = SQLContext(sc).createDataFrame(result)
out.registerTempTable("tmp_table")
sql_insert = 'INSERT OVERWRITE TABLE ads.tmp_calculate_entropy_grap_radio  SELECT * FROM tmp_table'
hive_context.sql(sql_insert)
sc.stop()