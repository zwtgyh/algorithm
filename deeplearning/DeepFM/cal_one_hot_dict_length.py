#计算one_hot_dict长度，作为feature_size的值
import datetime
today_0 = (datetime.date.today()).strftime('%Y%m%d')
yesterday = (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
one_hot_filename = '/home/dc/deeplearning/DeepFM/one_hot_dict/one_hot_dict_{}.txt'.format(today_0)
one_hot_dict = open(one_hot_filename, 'r')
one_hot_dict = eval(one_hot_dict.read())
print(len(one_hot_dict))
