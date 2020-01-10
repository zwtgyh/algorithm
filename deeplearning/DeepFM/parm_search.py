#神经网络自动调参脚本
import os
import re
import time

batch_size_list = [256, 512, 1024, 2560, 5120, 10240, 20480, 50000]
learning_rate_list = [0.001, 0.01, 0.1, 0.0005, 0.0015, 0.002, 0.0004, 0.0006]
optimizer_list = ['Adam', 'Adagrad', 'Momentum', 'ftrl']
deep_layers_list = ['400,400,400', '100,100,100', '200,200,200', '64,128,256', '128,256,512', '256,128,64', '512,256,128', '64,512,64', '512,64,512']
dropout_list = ['0.5,0.5,0.5', '0.4,0.4,0.4', '0.3,0.3,0.3', '0.2,0.2,0.2', '0.66,0.77,0.88', '0.9,0.9,0.9']



#os.system(command_train)
#os.popen('stat -c %Y  ./test/log/deepfm_train.log').read()
#log = open("./test/log/deepfm_train.log", "r").read()

#log = popen(command_train)

#auc = eval(re.findall(r"auc = (.+?),",log)[0])

#dict = 

i_log = 0
auc_dict = {}

auc_batch_size = []
auc_learning_rate = []
auc_optimizer = []
auc_deep_layers = []

for batch_size in batch_size_list:
	command_train = """python3 /home/dc/deeplearning/DeepFM/DeepFM_tf.py \
	--task_type=train \
	--learning_rate=0.001 \
	--optimizer=Adam \
	--num_epochs=2 \
	--batch_size={1} \
	--field_size=51 \
	--feature_size=8945 \
	--deep_layers=400,400,400 \
	--dropout=0.5,0.5,0.5 \
	--log_steps=30 \
	--num_threads=12 \
	--model_dir=/home/dc/test/deeplearning/DeepFM/model_ckpt/model_ckpt \
	--data_dir=/home/dc/deeplearning/DeepFM/data/20200109/ \
	--clear_existing_model=True \
	> /home/dc/test/log/deepfm_train_{0}.log 2>&1 &""".format(i_log, batch_size)
	os.system(command_train)
	log_path = "/home/dc/test/log/deepfm_train_{}.log".format(i_log)
	log_time = "stat -c %Y /home/dc/test/log/deepfm_train_{}.log".format(i_log)
	final_write_time = [0, 1, 2]
	while True:
		final_write_time.append(os.popen(log_time).read())
		if final_write_time[-1] == final_write_time[-2]:
			if final_write_time[-2] == final_write_time[-3]:
				break
		time.sleep(60)
	log = open(log_path, "r").read()
	auc = eval(max(re.findall(r"auc = (.+?),",log)))
	auc_batch_size.append(auc)
	dict_key = "batch_size={}".format(batch_size)
	auc_dict[dict_key] = auc
	with open("/home/dc/test/auc_dict.txt", "w") as f:
		f.write(str(auc_dict))
	i_log += 1


batch_size_index = auc_batch_size.index(max(auc_batch_size))
batch_size = batch_size_list[batch_size_index]

for learning_rate in learning_rate_list:
	command_train = """python3 /home/dc/deeplearning/DeepFM/DeepFM_tf.py \
	--task_type=train \
	--learning_rate={2} \
	--optimizer=Adam \
	--num_epochs=2 \
	--batch_size={1} \
	--field_size=51 \
	--feature_size=8945 \
	--deep_layers=400,400,400 \
	--dropout=0.5,0.5,0.5 \
	--log_steps=30 \
	--num_threads=12 \
	--model_dir=/home/dc/test/deeplearning/DeepFM/model_ckpt/model_ckpt \
	--data_dir=/home/dc/deeplearning/DeepFM/data/20200109/ \
	--clear_existing_model=True \
	> /home/dc/test/log/deepfm_train_{0}.log 2>&1 &""".format(i_log, batch_size, learning_rate)
	os.system(command_train)
	log_path = "/home/dc/test/log/deepfm_train_{}.log".format(i_log)
	log_time = "stat -c %Y /home/dc/test/log/deepfm_train_{}.log".format(i_log)
	final_write_time = [0, 1, 2]
	while True:
		final_write_time.append(os.popen(log_time).read())
		if final_write_time[-1] == final_write_time[-2]:
			if final_write_time[-2] == final_write_time[-3]:
				break
		time.sleep(60)
	log = open(log_path, "r").read()
	auc = eval(max(re.findall(r"auc = (.+?),",log)))
	auc_learning_rate.append(auc)
	dict_key = "batch_size={},learning_rate={}".format(batch_size, learning_rate)
	auc_dict[dict_key] = auc
	with open("/home/dc/test/auc_dict.txt", "w") as f:
		f.write(str(auc_dict))
	i_log += 1

learning_rate_index = auc_learning_rate.index(max(auc_learning_rate))
learning_rate = learning_rate_list[learning_rate_index]


for optimizer in optimizer_list:
	command_train = """python3 /home/dc/deeplearning/DeepFM/DeepFM_tf.py \
	--task_type=train \
	--learning_rate={2} \
	--optimizer={3} \
	--num_epochs=2 \
	--batch_size={1} \
	--field_size=51 \
	--feature_size=8945 \
	--deep_layers=400,400,400 \
	--dropout=0.5,0.5,0.5 \
	--log_steps=30 \
	--num_threads=12 \
	--model_dir=/home/dc/test/deeplearning/DeepFM/model_ckpt/model_ckpt \
	--data_dir=/home/dc/deeplearning/DeepFM/data/20200109/ \
	--clear_existing_model=True \
	> /home/dc/test/log/deepfm_train_{0}.log 2>&1 &""".format(i_log, batch_size, learning_rate, optimizer)
	os.system(command_train)
	log_path = "/home/dc/test/log/deepfm_train_{}.log".format(i_log)
	log_time = "stat -c %Y /home/dc/test/log/deepfm_train_{}.log".format(i_log)
	final_write_time = [0, 1, 2]
	while True:
		final_write_time.append(os.popen(log_time).read())
		if final_write_time[-1] == final_write_time[-2]:
			if final_write_time[-2] == final_write_time[-3]:
				break
		time.sleep(60)
	log = open(log_path, "r").read()
	auc = eval(max(re.findall(r"auc = (.+?),",log)))
	auc_optimizer.append(auc)
	dict_key = "batch_size={},learning_rate={},optimizer={}".format(batch_size, learning_rate, optimizer)
	auc_dict[dict_key] = auc
	with open("/home/dc/test/auc_dict.txt", "w") as f:
		f.write(str(auc_dict))
	i_log += 1

optimizer_index = auc_optimizer.index(max(auc_optimizer))
optimizer = optimizer_list[optimizer_index]


for deep_layers in deep_layers_list:
	command_train = """python3 /home/dc/deeplearning/DeepFM/DeepFM_tf.py \
	--task_type=train \
	--learning_rate={2} \
	--optimizer={3} \
	--num_epochs=2 \
	--batch_size={1} \
	--field_size=51 \
	--feature_size=8945 \
	--deep_layers={4} \
	--dropout=0.5,0.5,0.5 \
	--log_steps=30 \
	--num_threads=12 \
	--model_dir=/home/dc/test/deeplearning/DeepFM/model_ckpt/model_ckpt \
	--data_dir=/home/dc/deeplearning/DeepFM/data/20200109/ \
	--clear_existing_model=True \
	> /home/dc/test/log/deepfm_train_{0}.log 2>&1 &""".format(i_log, batch_size, learning_rate, optimizer, deep_layers)
	os.system(command_train)
	log_path = "/home/dc/test/log/deepfm_train_{}.log".format(i_log)
	log_time = "stat -c %Y /home/dc/test/log/deepfm_train_{}.log".format(i_log)
	final_write_time = [0, 1, 2]
	while True:
		final_write_time.append(os.popen(log_time).read())
		if final_write_time[-1] == final_write_time[-2]:
			if final_write_time[-2] == final_write_time[-3]:
				break
		time.sleep(60)
	log = open(log_path, "r").read()
	auc = eval(max(re.findall(r"auc = (.+?),",log)))
	auc_deep_layers.append(auc)
	dict_key = "batch_size={},learning_rate={},optimizer={},deep_layers={}".format(batch_size, learning_rate, optimizer, deep_layers)
	auc_dict[dict_key] = auc
	with open("/home/dc/test/auc_dict.txt", "w") as f:
		f.write(str(auc_dict))
	i_log += 1

deep_layers_index = auc_deep_layers.index(max(auc_deep_layers))
deep_layers = deep_layers_list[deep_layers_index]


for dropout in dropout_list:
	command_train = """python3 /home/dc/deeplearning/DeepFM/DeepFM_tf.py \
	--task_type=train \
	--learning_rate={2} \
	--optimizer={3} \
	--num_epochs=2 \
	--batch_size={1} \
	--field_size=51 \
	--feature_size=8945 \
	--deep_layers={4} \
	--dropout={5} \
	--log_steps=30 \
	--num_threads=12 \
	--model_dir=/home/dc/test/deeplearning/DeepFM/model_ckpt/model_ckpt \
	--data_dir=/home/dc/deeplearning/DeepFM/data/20200109/ \
	--clear_existing_model=True \
	> /home/dc/test/log/deepfm_train_{0}.log 2>&1 &""".format(i_log, batch_size, learning_rate, optimizer, deep_layers, dropout)
	os.system(command_train)
	log_path = "/home/dc/test/log/deepfm_train_{}.log".format(i_log)
	log_time = "stat -c %Y /home/dc/test/log/deepfm_train_{}.log".format(i_log)
	final_write_time = [0, 1, 2]
	while True:
		final_write_time.append(os.popen(log_time).read())
		if final_write_time[-1] == final_write_time[-2]:
			if final_write_time[-2] == final_write_time[-3]:
				break
		time.sleep(60)
	log = open(log_path, "r").read()
	auc = eval(max(re.findall(r"auc = (.+?),",log)))
	auc_deep_layers.append(auc)
	dict_key = "batch_size={},learning_rate={},optimizer={},deep_layers={},dropout={}".format(batch_size, learning_rate, optimizer, deep_layers, dropout)
	auc_dict[dict_key] = auc
	with open("/home/dc/test/auc_dict.txt", "w") as f:
		f.write(str(auc_dict))
	i_log += 1

deep_layers_index = auc_deep_layers.index(max(auc_deep_layers))
deep_layers = deep_layers_list[deep_layers_index]

bestdict = {}
bestdict['batch_size'] = batch_size
bestdict['learning_rate'] = learning_rate
bestdict['optimizer'] = optimizer
bestdict['deep_layers'] = deep_layers
bestdict['dropout'] = dropout

with open("/home/dc/test/final.txt", "w") as f:
	f.write(str(bestdict))
