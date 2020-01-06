#日常计算各特征线上线下模型空值情况
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
import pandas as pd
import datetime
def find_null(ctr):
    tmp_data_online_2 = df_online_2[ctr]
    tmp_data_offline_2 = df_offline_2[ctr]
    length = len(tmp_data_online_2)
    s1 = 0
    s2 = 0
    s3 = 0
    s4 = 0
    s5 = 0
    s6 = 0
    for i in range(len(df_online_2[ctr])):
        if tmp_data_online_2.iloc[i]==True:
            s1 += 1
        else:
            continue
    p1 = s1/length
    for i in range(len(df_offline_2[ctr])):
        if tmp_data_offline_2.iloc[i]==True:
            s2 += 1
        else:
            continue
    p2 = s2/length
    for i in range(len(df_online_2[ctr])):
        if (tmp_data_online_2.iloc[i]==True and tmp_data_offline_2.iloc[i]==True):
            s3 += 1
        else:
            continue
    p3 = s3/length
    for i in range(len(df_online_2[ctr])):
        if (tmp_data_online_2.iloc[i]==True and tmp_data_offline_2.iloc[i]!=True):
            s4 += 1
        else:
            continue
    p4 = s4/length
    for i in range(len(df_online_2[ctr])):
        if (tmp_data_online_2.iloc[i]!=True and tmp_data_offline_2.iloc[i]==True):
            s5 += 1
        else:
            continue
    p5 = s5/length
    for i in range(len(df_online_2[ctr])):
        if (tmp_data_online_2.iloc[i]!=True and tmp_data_offline_2.iloc[i]!=True):
            if tmp_data_online_2.iloc[i] != tmp_data_offline_2.iloc[i]:
                s6 += 1
        else:
            continue
    p6 = s6/length
    p1 = round(p1, 5)
    p2 = round(p2, 5)
    p3 = round(p3, 5)
    p4 = round(p4, 5)
    p5 = round(p5, 5)
    p6 = round(p6, 5)
    return p1, p2, p3, p4, p5, p6
conf = SparkConf().setAppName('ctr_compare')
# conf = SparkConf().config('spark.debug.maxToStringFields', '100')
sc = SparkContext(conf=conf)
sc.stop()
sc = SparkContext(conf=conf)
hive_context = HiveContext(sc)

yesterday = (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
today = (datetime.date.today()).strftime('%Y%m%d')
a = list(today)
a.insert(4, '-')
a.insert(7, '-')
today = ''.join(a)
drop_day = (datetime.date.today() + datetime.timedelta(days=-7)).strftime('%Y%m%d')
a = list(drop_day)
a.insert(4, '-')
a.insert(7, '-')
drop_day = ''.join(a)
sql_create_table_1 = '''CREATE TABLE IF NOT EXISTS ads.tmp_ctr_online_3500_sample_zwt_1 AS
SELECT
uid,user_day_bxm_id_rank, user_bxm_id_rank, user_current_activity_ticket_repeat_rank, position_entrance, ticket_position_stat_ctr, time, device_brand, media_sys_type, agent_id, browser, user_day_last_activityid_if_same, advertiser_id, asset_id, user_day_activity_bxm_id_rank, user_activity_last_bxm_id_intervel_type, pre_id, ticket_tag_1, provider_id, device_name, hour, spm, user_day_last_pre_id_if_same, hw, bxm_id, account_type, media_stat_ctr, provider_stat_ctr, week, launch_control, ticket_provider_stat_ctr, isp_domain, settle_type, source_type, activity_type, os_type, ticket_tag_2, pt, is_automatic, activity_stat_ctr, user_activity_last_bxm_id_if_click, user_last_bxm_id_if_click, ticket_stat_ctr, city, activityid, user_day_ticket_repeat_rank, ticket_tag_3, ticket_activity_stat_ctr, position_stat_ctr, position, ip_pre, media_id, province, media_child_class_id, user_activity_bxm_id_rank, device_type, user_current_activity_bxm_id_rank, best_ticketid, ticket_media_stat_ctr, appos, media_class_id, is_venue, user_last_bxm_id_intervel_type
FROM ads.fm_online_feature_map_parse_di
WHERE pt=\'{}\'
AND bxm_id IS NOT NULL
ORDER BY rand()
LIMIT 20000'''.format(yesterday)
sql_create_table_2 = '''CREATE TABLE IF NOT EXISTS ads.tmp_ctr_offline_3500_sample_zwt AS
SELECT a.* FROM (SELECT * FROM ads.tmp_ctr_model_sample_test WHERE pt=\'{}\') AS a LEFT JOIN ads.tmp_ctr_online_3500_sample_zwt_1 AS b
WHERE a.bxm_id = b.bxm_id
AND b.bxm_id IS NOT NULL'''.format(today)
sql_create_table_3 = 'CREATE TABLE IF NOT EXISTS ads.tmp_ctr_online_3500_sample_zwt AS SELECT a.* FROM ads.tmp_ctr_online_3500_sample_zwt_1 AS a LEFT JOIN ads.tmp_ctr_offline_3500_sample_zwt AS b WHERE a.bxm_id=b.bxm_id AND b.bxm_id IS NOT NULL'
sql_get_online = 'SELECT * FROM ads.tmp_ctr_online_3500_sample_zwt'
sql_get_offline = 'SELECT * FROM ads.tmp_ctr_offline_3500_sample_zwt'
sql_drop_table_1 = 'DROP TABLE IF EXISTS ads.tmp_ctr_online_3500_sample_zwt_1'
sql_drop_table_2 = 'DROP TABLE IF EXISTS ads.tmp_ctr_offline_3500_sample_zwt'
sql_drop_table_3 = 'DROP TABLE IF EXISTS ads.tmp_ctr_online_3500_sample_zwt'
sql_drop_table_4 = 'ALTER TABLE ads.tmp_daily_ctr_compare_zwt DROP IF EXISTS PARTITION (pt=\'{}\')'.format(drop_day)
sql_deccribe_online = 'DESCRIBE EXTENDED ads.tmp_ctr_online_3500_sample_zwt'
sql_deccribe_offline = 'DESCRIBE EXTENDED ads.tmp_ctr_offline_3500_sample_zwt'
sql_insert = 'INSERT OVERWRITE TABLE ads.tmp_daily_ctr_compare_zwt PARTITION(pt=\'{}\') SELECT * FROM tmp_table'.format(today)
#sql_insert = 'INSERT INTO ads.tmp_daily_ctr_compare_zwt SELECT * FROM tmp_table'
hive_context.sql(sql_drop_table_1)
hive_context.sql(sql_drop_table_2)
hive_context.sql(sql_drop_table_3)
hive_context.sql(sql_create_table_1)
hive_context.sql(sql_create_table_2)
hive_context.sql(sql_create_table_3)
df_online = hive_context.sql(sql_get_online)
df_offline = hive_context.sql(sql_get_offline)
df_online = df_online.toPandas()
df_offline = df_offline.toPandas()

table_info_online = hive_context.sql(sql_deccribe_online).toPandas()
columns_name_online = []
for i in range(len(table_info_online)-2):
    columns_name_online.append(table_info_online.iloc[i][0])
table_info_offline = hive_context.sql(sql_deccribe_offline).toPandas()
columns_name_offline = []
for i in range(len(table_info_offline)-2):
    columns_name_offline.append(table_info_offline.iloc[i][0])

df_online = df_online.drop_duplicates(subset=['bxm_id'], keep='first')
df_offline = df_offline.drop_duplicates(subset=['bxm_id'], keep='first')
ctr_together = [i for i in df_online.columns.values.tolist() if i in df_offline.columns.values.tolist()]
df_online_2 = df_online[ctr_together]
df_offline_2 = df_offline[ctr_together]
df_online_2 = df_online_2.replace('', True)
df_offline_2 = df_offline_2.replace('', True)
df_online_2 = df_online_2.sort_values(by='bxm_id')
df_offline_2 = df_offline_2.sort_values(by='bxm_id')
l1 = []
l2 = [df_online_2['pt'].iloc[0][0:8]]*len(ctr_together)
l3 = [len(df_online_2)]*len(ctr_together)
l4 = []
l5 = []
l6 = []
l7 = []
l8 = []
l9 = []
for ctr in ctr_together:
    p1, p2, p3, p4, p5, p6 = find_null(ctr)
    l1.append(ctr)
    l4.append(p1)
    l5.append(p2)
    l6.append(p3)
    l7.append(p4)
    l8.append(p5)
    l9.append(p6)
dd = {'column_name':l1,'check_date':l2,'sample_cnt':l3,'online_is_null':l4,'offline_is_null':l5,'online_offline_is_null':l6,
      'online_is_null_offline_not_null':l7,'online_not_null_offline_is_null':l8,'online_not_equal_offline':l9}
out = pd.DataFrame(dd)
out2 = SQLContext(sc).createDataFrame(out)
out2.registerTempTable("tmp_table")
hive_context.sql(sql_insert)
hive_context.sql(sql_drop_table_1)
hive_context.sql(sql_drop_table_2)
hive_context.sql(sql_drop_table_3)
hive_context.sql(sql_drop_table_4)
sc.stop()
