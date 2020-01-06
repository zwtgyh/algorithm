#日常计算模型准确率、AUC等参数
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from sklearn.metrics import roc_auc_score
from sklearn.metrics import log_loss
import pandas as pd
import datetime
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
conf = SparkConf().setAppName('precision_recall')
sc = SparkContext(conf=conf)
sc.stop()
sc = SparkContext(conf=conf)
hive_context = HiveContext(sc)
sql_create_1 = '''CREATE TABLE IF NOT EXISTS ads.tmp_pre_ctr_online AS
SELECT bxm_id, pre_ctr, 19-FLOOR(pre_ctr/0.05) AS position_list
FROM ads.fm_online_feature_map_parse_di
WHERE pt=\'{}\'
AND bxm_id IS NOT NULL
AND length(bxm_id)>0'''.format(yesterday)
sql_create_2 = '''CREATE TABLE IF NOT EXISTS ads.tmp_pre_ctr_offline AS
SELECT a.bxm_id, a.ctr_label
FROM (SELECT bxm_id, ctr_label
FROM ads.tmp_ctr_model_sample_test
WHERE pt=\'{}\') AS a LEFT JOIN ads.tmp_pre_ctr_online AS b
WHERE a.bxm_id=b.bxm_id
AND b.bxm_id IS NOT NULL'''.format(today)
sql_create_3 = 'CREATE TABLE IF NOT EXISTS ads.tmp_pre_ctr_online_offline AS SELECT a.bxm_id, a.position_list, a.pre_ctr AS pre_ctr_online, b.ctr_label AS ctr_label_offline FROM ads.tmp_pre_ctr_online AS a RIGHT JOIN ads.tmp_pre_ctr_offline AS b WHERE a.bxm_id=b.bxm_id'
sql_drop_1 = 'DROP TABLE IF EXISTS ads.tmp_pre_ctr_online'
sql_drop_2 = 'DROP TABLE IF EXISTS ads.tmp_pre_ctr_offline'
sql_drop_3 = 'DROP TABLE IF EXISTS ads.tmp_pre_ctr_online_offline'
sql_drop_4 = 'ALTER TABLE ads.tmp_pre_ctr_PandR DROP IF EXISTS PARTITION (pt=\'{}\')'.format(drop_day)
sql_insert = 'INSERT OVERWRITE TABLE ads.tmp_pre_ctr_PandR PARTITION(pt=\'{}\') SELECT * FROM tmp_table'.format(today)
hive_context.sql(sql_drop_1)
hive_context.sql(sql_drop_2)
hive_context.sql(sql_drop_3) 
hive_context.sql(sql_create_1) 
hive_context.sql(sql_create_2)
hive_context.sql(sql_create_3)
df = hive_context.sql('SELECT * FROM ads.tmp_pre_ctr_online_offline')


#y_true、y_predict
oo = df.toPandas()
oo[['pre_ctr_online','ctr_label_offline']] = oo[['pre_ctr_online','ctr_label_offline']].astype('float')
y_predict = oo['pre_ctr_online']
y_true = oo['ctr_label_offline']

#线上、线下ctr均值列表、线下ctr为0、1的计数列表
online_ctr_avg = []
offline_ctr_avg = []
list_0_cnt = []
list_1_cnt = []

for i in range(20):
	sql_get = 'SELECT * FROM ads.tmp_pre_ctr_online_offline WHERE position_list=\'{}\''.format(i)
	df = hive_context.sql(sql_get)
	#线上ctr均值计算
	online_ctr_avg.append(eval(df.describe(['pre_ctr_online']).collect()[1]['pre_ctr_online']))
	#线下ctr均值计算
	offline_ctr_avg.append(eval(df.describe(['ctr_label_offline']).collect()[1]['ctr_label_offline']))
	#线下ctr_label为1的个数
	list_1_cnt.append(int(eval(df.describe(['ctr_label_offline']).collect()[0]['ctr_label_offline'])*eval(df.describe(['ctr_label_offline']).collect()[1]['ctr_label_offline'])))
	#线下ctr_label为0的个数
	list_0_cnt.append(int(eval(df.describe(['ctr_label_offline']).collect()[0]['ctr_label_offline'])-eval(df.describe(['ctr_label_offline']).collect()[0]['ctr_label_offline'])*eval(df.describe(['ctr_label_offline']).collect()[1]['ctr_label_offline'])))

#生成区间列
section_list = []
a = 1
b = 1.05
while True:
        if a > 0:
                a = round(a-0.05, 2)
                b = round(b-0.05, 2)
                out = '[{}, {})'.format(a, b)
                section_list.append(out)
        else:
                break

#生成每个数据的分组索引
#position_list = []
#for i in range(len(df)):
#        a = 0.95
#        n = 0
#        while True:
#                if eval(df.iloc[i]['pre_ctr_online'])<a:
#                        a = round(a-0.05, 2)
#                        n += 1
#                else:
#                        position_list.append(n)
#                        break

#生成ctr_label_0和ctr_label_1的计数列、线上ctr求和列、y_true、y_predict列
#list_0_cnt = [0] * len(section_list)
#list_1_cnt = [0] * len(section_list)
#online_ctr_sum = [0] * len(section_list)
#y_true = []
#y_predict = []

#计算ctr_label_0和ctr_label_1的计数变量
#for i in range(len(position_list)):
#        n = position_list[i]
#        if eval(df.iloc[i]['ctr_label_offline']) == 0:
#                list_0_cnt[n] = list_0_cnt[n] + 1
#                online_ctr_sum[n] += eval(df.iloc[i]['pre_ctr_online'])
#                y_true.append(eval(df.iloc[i]['ctr_label_offline']))
#                y_predict.append(eval(df.iloc[i]['pre_ctr_online']))
#        elif eval(df.iloc[i]['ctr_label_offline']) == 1:
#                list_1_cnt[n] = list_1_cnt[n] + 1
#                online_ctr_sum[n] += eval(df.iloc[i]['pre_ctr_online'])
#                y_true.append(eval(df.iloc[i]['ctr_label_offline']))
#                y_predict.append(eval(df.iloc[i]['pre_ctr_online']))

#计算线上、线下ctr均值列(如果该分组没有数据，抛出null)
#online_ctr_avg = []
#offline_ctr_avg = []
#for i in range(len(online_ctr_sum)):
#        if list_0_cnt[i] + list_1_cnt[i] == 0:
#                online_ctr_avg.append('null')
#                offline_ctr_avg.append('null')
#        else:
#                online_ctr_avg.append(online_ctr_sum[i]/(list_0_cnt[i]+list_1_cnt[i]))
#                offline_ctr_avg.append(list_1_cnt[i]/(list_0_cnt[i]+list_1_cnt[i]))

#线下为0、1的样本总数
# sum(list_0_cnt)
# sum(list_1_cnt)

#recall
recall_list = []
for i in range(len(section_list)):
        s = 0
        for k in range(i+1):
                s+=list_1_cnt[k]
        recall_list.append(s/sum(list_1_cnt))

#precision
precision_list = []
for i in range(len(section_list)):
        s1 = 0
        s2 = 0
        for k in range(i+1):
                s1+=list_0_cnt[k]
                s2+=list_1_cnt[k]
        if s1+s2==0:
                precision_list.append('null')
        else:
                precision_list.append(s2/(s1+s2))

#F1_score_list
F1_score_list = []
for i in range(len(section_list)):
    if type(precision_list[i]) == float:
        F1_score_list.append(2*precision_list[i]*recall_list[i]/(precision_list[i]+recall_list[i]))
    else:
        F1_score_list.append('null')

#sample_date
sample_date = [yesterday] * len(section_list)

section_list.append('总计')
list_0_cnt.append(sum(list_0_cnt))
list_1_cnt.append(sum(list_1_cnt))
online_ctr_avg.append('auc')
offline_ctr_avg.append(roc_auc_score(y_true, y_predict))
precision_list.append('logloss')
recall_list.append(log_loss(y_true, y_predict))
F1_score_list.append('null')
sample_date.append('null')
dd = {'pre_ctr_section':section_list, '0_cnt':list_0_cnt, '1_cnt':list_1_cnt, 'pre_ctr_avg':online_ctr_avg, 'stat_ctr':offline_ctr_avg, 'precision':precision_list, 'recall':recall_list, 'F1_score':F1_score_list, 'sample_date':sample_date}
out = pd.DataFrame(dd, dtype=str)
out2 = SQLContext(sc).createDataFrame(out)
out2.registerTempTable("tmp_table")
hive_context.sql(sql_insert)
hive_context.sql(sql_drop_1)
hive_context.sql(sql_drop_2)
hive_context.sql(sql_drop_3)
hive_context.sql(sql_drop_4)
sc.stop()
