## 2019.11.28 by zhangwentao 非定时任务 spark_id cal_egr ip:172.20.2.5 path:/home/dc/fmmodel sh:calculate_entropy_grap_radio.sh spark-submit --master yarn-client --driver-memory 2G --executor-memory 1G --num-executors 4 /home/dc/fmmodel/calculate_entropy_grap_radio_spark.py
##SparkDataFrame常见操作说明，避免调用.collect()，避免转换为pandas.DataFrame，会变成单机，有慢和内存溢出问题
##https://www.cnblogs.com/nucdy/p/6559318.html
##https://blog.csdn.net/sinat_26917383/article/details/80500349
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
import numpy as np
import pandas as pd
import datetime
today = (datetime.date.today()).strftime('%Y%m%d')
a = list(today)
a.insert(4, '-')
a.insert(7, '-')
today = ''.join(a)
#计算信息墒
  #按列名计算信息墒
def calculate_entropy_1(column_name, data):
	df_tmp = eval("data.groupby(\'{0}\').agg({{\'{0}\':'count'}})".format(column_name))
	entropy = 0
	p_list = []  #优化这里
	# for i in range(len(df_tmp.collect())):
	# 	p_list.append(df_tmp.collect()[i][1])
	tmp_list = df_tmp.collect()
	for i in range(len(tmp_list)):
		p_list.append(tmp_list[i][1])
	p_list = [i/sum(p_list) for i in p_list]
	for p in p_list:
		entropy -= p*np.log2(p)
	return entropy


  #按数据框计算信息墒
# def calculate_entropy_2(data_frame):
# 	entropy = 0
# 	p_list = []  #优化
# 	# for i in range(len(data_frame.collect())):
# 	# 	p_list.append(data_frame.collect()[i][1])
# 	tmp_list = data_frame.collect()
# 	for i in range(len(tmp_list)):
# 		p_list.append(tmp_list[i][1])
# 	p_list = [i/sum(p_list) for i in p_list]
# 	for p in p_list:
# 		entropy -= p*np.log2(p)
# 	return entropy


#计算条件墒
def calculate_condition_entropy(column_name_1, column_name_2, data):
	dd = eval("data.groupby(\'{0}\',\'{1}\').agg({{\'{1}\':\'count\'}}).orderBy(\'{0}\',\'{1}\')".format(column_name_1, column_name_2))
	column_name_1_value_list = []    #优化
	# len_data = dd.count()
	# for n in range(len_data):
	# 	column_name_1_value_list.append(dd.collect()[n][0])
	# column_name_1_value_list = list(set(column_name_1_value_list))    #去重
	tmp_list = eval("dd.select(\'{}\').distinct().collect()".format(column_name_1))
	for i in range(len(tmp_list)):
		column_name_1_value_list.append(tmp_list[i][0])
	bigp_list = []
	entropy_list = []
	dd_pandas = dd.toPandas()
	# for column_name_1_value in column_name_1_value_list:
	# 	tmp_dd_1 = eval("dd.where(\"{}=\'{}\'\")".format(column_name_1, column_name_1_value))
	# 	p_sum = 0
	# 	for nn in range(tmp_dd_1.count()):
	# 		p_sum += tmp_dd_1.collect()[nn][2]
	# 	bigp_list.append(p_sum)
	# 	tmp_dd_2 = eval("tmp_dd_1.drop(\'{}\')".format(column_name_1))
	# 	entropy_list.append(calculate_entropy_2(tmp_dd_2))
	for column_name_1_value in column_name_1_value_list:
		p_list = []
		entropy = 0
		tmp_pandas = eval("dd_pandas[dd_pandas[\'{}\']==\'{}\']".format(column_name_1, column_name_1_value))
		for i in range(tmp_pandas.shape[0]):
			p_list.append(tmp_pandas.iloc[i][2])
		bigp_list.append(sum(p_list))
		p_list = [i/sum(p_list) for i in p_list]
		for p in p_list:
			entropy -= p*np.log2(p)
		entropy_list.append(entropy)
	bigp_list = [i/sum(bigp_list) for i in bigp_list]
	condition_entropy = sum(np.multiply(np.array(bigp_list),np.array(entropy_list)))
	return condition_entropy


#计算信息增益
def calculate_entropy_grap(column_name_1, column_name_2, data):
	base_entropy = calculate_entropy_1(column_name_2, data)
	condition_entropy = calculate_condition_entropy(column_name_1, column_name_2, data)
	entropy_grap = base_entropy - condition_entropy
	return entropy_grap


#计算信息增益率
def calculate_entropy_grap_radio(column_name_1, column_name_2, data):
	entropy_grap = calculate_entropy_grap(column_name_1, column_name_2, data)
	entropy_grap_radio = entropy_grap/calculate_entropy_1(column_name_1, data)
	return entropy_grap_radio


#按列计算信息增益率
def calculate_column_entropy_grap_radio(data):
	entropy_grap_radio_list = []
	column_name_list = data.columns
	column_name_list.remove('ctr_label')
	name_2 = 'ctr_label'
	for column_name in column_name_list:
		entropy_grap_radio_list.append(calculate_entropy_grap_radio(column_name, name_2, data))
	out = {'column_name':column_name_list, 'entropy_grap_radio':entropy_grap_radio_list}
	result = pd.DataFrame(out)
	result = result.sort_values(by='entropy_grap_radio')
	return result


#取数
conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)
sc.stop()
sc = SparkContext(conf=conf)
hive_context = HiveContext(sc)
sql = '''SELECT
*
FROM ads.tmp_ctr_model_sample_train
where pt=\'{}\'
AND bxm_id is not null
AND bxm_id != 'b'
'''.format(today)
df = hive_context.sql(sql)
df = df.na.fill('null')   #注意这里，在特征'ticket_media_stat_ctr'的测试中，发现calculate_entropy_1计算时候出现nan，是因为计数出现{Nano:0}，使用字符串填补空值，不清楚为什么会出现统计次数为0的情况
#去除删除列
drop_list = ['bxm_id', 'ticket_tag_3', 'sample_date', 'pt']
#这里删除列标准是空值过多、数据信息重复、明显不是模型特征、特征类别过多计算开销过大，对于空值比例高的列，是通过hive筛选2000条数据人工判断的，有概率出错，可以考虑加上空值比例判断语句
#自动划分删除列，特征类别过多的列也可以通过语句判断
#判断特征列过多、判断空值比例
# data_name = data.columns
# count_list = []
# null_list = []
# for name in data_name:
# 	count_list.append(eval("data.select(\'{}\').distinct().count()".format(name)))
# 	null_list.append(eval("data.filter(\"{} is null\").count()".format(name)))
# s = data.count()
# null_list = [i/s for i in null_list]
for column_name_drop in drop_list:
	df = eval("df.drop(\'{}\')".format(column_name_drop))


#计算
result = calculate_column_entropy_grap_radio(df)
out = SQLContext(sc).createDataFrame(result)
out.registerTempTable("tmp_table")
sql_insert = 'INSERT OVERWRITE TABLE ads.tmp_calculate_entropy_grap_radio  SELECT * FROM tmp_table'
hive_context.sql(sql_insert)
sc.stop()

