#将onehot字典存入mongodb
import pymongo
import datetime
today_0 = (datetime.date.today()).strftime('%Y%m%d')

one_hot_filename = '/home/dc/deeplearning/DeepFM/one_hot_dict/one_hot_dict_{}.txt'.format(today_0)
id = 'deep_fm_ctr_v001_onehot_dict_{}'.format(today_0)
one_hot_dict = open(one_hot_filename, 'r')
one_hot_dict = eval(one_hot_dict.read())

key = list(one_hot_dict.keys())
value = list(one_hot_dict.values())

key = str(key)
key = key.replace('.', '@#_@')
key = eval(key)
one_hot_dict = dict(zip(key,value))

with open ('/home/dc/deeplearning/DeepFM/field_name.txt', 'r') as f:
	column_name_list = eval(f.read())

column_name_dict = {'position': 'f2006', 'pre_id': 'f1001', 'ticket_tag_1': 'f1002', 'ticket_tag_2': 'f1003', 'ticket_tag_3': 'f1004', 'advertiser_id': 'f1005', 'account_type': 'f1006', 'agent_id': 'f1007', 'asset_id': 'f1008', 'source_type': 'f1009', 'media_id': 'f2001', 'media_class_id': 'f2002', 'media_child_class_id': 'f2003', 'provider_id': 'f2004', 'activityid': 'f3001', 'ip_pre': 'f4003', 'appos': 'f4004', 'os_type': 'f4005', 'device_type': 'f4008', 'device_brand': 'f4009', 'isp_domain': 'f4012', 'province': 'f4015', 'city': 'f4016', 'hour': 'f4017', 'week': 'f4018', 'user_current_activity_bxm_id_rank': 'f6001', 'user_day_ticket_repeat_rank': 'f6003', 'user_current_activity_ticket_repeat_rank': 'f6019', 'browser': 'f4007', 'media_sys_type': 'f2005', 'position_entrance': 'f2007', 'activity_type': 'f3002', 'is_venue': 'f3003', 'is_automatic': 'f3004', 'launch_control': 'f3005', 'ticket_stat_ctr': 'f5001', 'media_stat_ctr': 'f5002', 'position_stat_ctr': 'f5003', 'activity_stat_ctr': 'f5004', 'ticket_media_stat_ctr': 'f5005', 'ticket_position_stat_ctr': 'f5006', 'ticket_activity_stat_ctr': 'f5007', 'provider_stat_ctr': 'f5008', 'ticket_provider_stat_ctr': 'f5009', 'user_day_bxm_id_rank': 'f6002', 'user_day_activity_bxm_id_rank': 'f6004', 'user_bxm_id_rank': 'f6005', 'user_activity_bxm_id_rank': 'f6006', 'user_day_last_activityid_if_same': 'f6007', 'user_day_last_pre_id_if_same': 'f6008', 'user_last_bxm_id_if_click': 'f6015', 'user_activity_last_bxm_id_if_click': 'f6016', 'user_activity_last_bxm_id_intervel_type': 'f6017', 'user_last_bxm_id_intervel_type': 'f6018', 'hw': 'f4013', 'device_name': 'f4010'}

feature_code = []
for n in column_name_list:
	feature_code.append(column_name_dict.get(n))

field_size = len(column_name_list)

client = pymongo.MongoClient("mongodb://admin:root_pwd123@47.97.189.222:27017/")
db = client["admin"]
collection = db["deepfm"]
#collection.insert_one({'_id':id, 'one_hot_dict':one_hot_dict, 'feature_size':len(one_hot_dict), 'modelKey':'deep_fm_ctr_v001', 'pt':today_0})

#if collection.find_one_and_replace({'_id':id}, {'_id':id, 'one_hot_dict':one_hot_dict, 'feature_size':len(one_hot_dict), 'modelKey':'deep_fm_ctr_v001', 'pt':today_0}) == None:
#	collection.insert_one({'_id':id, 'one_hot_dict':one_hot_dict, 'feature_size':len(one_hot_dict), 'modelKey':'deep_fm_ctr_v001', 'pt':today_0})
#else:
#	collection.find_one_and_replace({'_id':id}, {'_id':id, 'one_hot_dict':one_hot_dict, 'feature_size':len(one_hot_dict), 'modelKey':'deep_fm_ctr_v001', 'pt':today_0})

collection.replace_one({'_id':id}, {'_id':id, 'one_hot_dict':one_hot_dict, 'feature_size':len(one_hot_dict), 'field_size':field_size, 'feature_id_collect':feature_code, 'modelKey':'deep_fm_ctr_v001', 'pt':today_0}, upsert=True)
collection.replace_one({'_id':'deep_fm_ctr_v001_onehot_dict_last'}, {'_id':'deep_fm_ctr_v001_onehot_dict_last', 'one_hot_dict':one_hot_dict, 'feature_size':len(one_hot_dict), 'field_size':field_size, 'feature_id_collect':feature_code, 'modelKey':'deep_fm_ctr_v001', 'pt':today_0}, upsert=True)
