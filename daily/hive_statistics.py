# 该类主要功能是对hive对象做统计，各个函数功能如下：
# cal_null_percent：计算指定数据库的指定列数据在指定日期的空值比例
# 当作函数使用
# 复制class类的内容
# Q = hive_statistics()
# Q.date_list = ['20200115', '20200116']
# Q.column_name_list = ['mac', 'imei']
# Q.table = 'ads.src_encourage_video_sdk_access_log'
# Q.cal_null_percent()
# 当作包使用，将文件夹放在python运行路径下
# import hive_statistics
# Q = hive_statistics.get_hive()
# OR 假设hive_statistics存放在路径： /home/dc/test，并且和python运行路径不一致
# import sys
# sys.path.append('/home/dc/test')
# import hive_statistics
# 
# 
def get_hive():
	class hive_statistics(object):
		import datetime
		import time
		yesterday = (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

		def __init__(self, date_list=[yesterday], column_name_list=['mac'], table='ads.src_encourage_video_sdk_access_log'):
			self.date_list = date_list
			self.column_name_list = column_name_list
			self.table = table

		def cal_null_percent(self):
			from pyspark import SparkConf,SparkContext
			from pyspark.sql import HiveContext
			conf = SparkConf().setAppName(str(time.time())[-6:])
			try:
				sc.stop()
			except:
				pass
			sc = SparkContext(conf=conf)
			hive_context = HiveContext(sc)
			final_d = {}
			for yesterday in self.date_list:
				sql_0 = '''
				SELECT COUNT(1)
				FROM {1}
				WHERE pt=\'{0}\'
				AND model_type=13001
				'''.format(yesterday, self.table)
				s = hive_context.sql(sql_0).collect()[0][0]
				d = {}
				for column_name in self.column_name_list:
					sql_1 = '''
					SELECT COUNT(1)
					FROM {2}
					WHERE pt=\'{0}\'
					AND model_type=13001
					AND {1} IS NOT NULL
					AND {1} != ''
					'''.format(yesterday, column_name, self.table)
					t = hive_context.sql(sql_1).collect()[0][0]
					d[column_name] = "%.5f%%" % ((1-t/s) * 100)
				final_d[yesterday] = d
				sc.stop()
			return final_d
	out = hive_statistics()
	return out