#!/usr/bin/env python
#coding=utf-8
"""
TensorFlow Implementation of <<DeepFM: A Factorization-Machine based Neural Network for CTR Prediction>> with the fellowing features：
#1 Input pipline using Dataset high level API, Support parallel and prefetch reading
#2 Train pipline using Coustom Estimator by rewriting model_fn
#3 Support distincted training using TF_CONFIG
#4 Support export_model for TensorFlow Serving
by lambdaji
"""
#from __future__ import absolute_import
#from __future__ import division
#from __future__ import print_function

#import argparse
import shutil
#import sys
import os
import json
import glob
from datetime import date, timedelta
from time import time
#import gc
#from multiprocessing import Process
from math import log
#import math
import random
#import pandas as pd
#import numpy as np
import tensorflow as tf   #会报错，使用下面的代码替代
# import tensorflow.compat.v1 as tf
# tf.disable_v2_behavior()

#################### CMD Arguments ####################
#用法参考--https://www.jianshu.com/p/b4c2d43d81c6
FLAGS = tf.app.flags.FLAGS
tf.app.flags.DEFINE_integer("dist_mode", 0, "distribuion mode {0-loacal, 1-single_dist, 2-multi_dist}")    #分布式选项
tf.app.flags.DEFINE_string("ps_hosts", '', "Comma-separated list of hostname:port pairs")
tf.app.flags.DEFINE_string("worker_hosts", '', "Comma-separated list of hostname:port pairs")
tf.app.flags.DEFINE_string("job_name", '', "One of 'ps', 'worker'")
tf.app.flags.DEFINE_integer("task_index", 0, "Index of task within the job")
tf.app.flags.DEFINE_integer("num_threads", 16, "Number of threads")    #线程数(通过 "GPU":0 设置使用CPU训练，进而指定CPU线程数)
tf.app.flags.DEFINE_integer("feature_size", 0, "Number of features")   #特征维度(one hot后的)
tf.app.flags.DEFINE_integer("field_size", 0, "Number of fields")    #特征维度(one hot前的，即初始特征数量)
tf.app.flags.DEFINE_integer("embedding_size", 32, "Embedding size")    #embedding长度
tf.app.flags.DEFINE_integer("num_epochs", 10, "Number of epochs")    #epoch训练几轮
tf.app.flags.DEFINE_integer("batch_size", 64, "Number of batch size")    #batch
tf.app.flags.DEFINE_integer("log_steps", 1000, "save summary every steps")    #保存log的步长
tf.app.flags.DEFINE_float("learning_rate", 0.0005, "learning rate")
tf.app.flags.DEFINE_float("l2_reg", 0.0001, "L2 regularization")
tf.app.flags.DEFINE_string("loss_type", 'log_loss', "loss type {square_loss, log_loss}")
tf.app.flags.DEFINE_string("optimizer", 'Adam', "optimizer type {Adam, Adagrad, GD, Momentum, ftrl}")    #优化算法
tf.app.flags.DEFINE_string("deep_layers", '256,128,64', "deep layers")  #每层隐层的长度
tf.app.flags.DEFINE_string("dropout", '0.5,0.5,0.5', "dropout rate")    #防止过拟合的参数，注意:len(dropout) == len(layers)
tf.app.flags.DEFINE_boolean("batch_norm", False, "perform batch normaization (True or False)")    #是否进行BN
tf.app.flags.DEFINE_float("batch_norm_decay", 0.9, "decay for the moving average(recommend trying decay=0.9)")    #BN衰变系数
tf.app.flags.DEFINE_string("data_dir", '', "data dir")
tf.app.flags.DEFINE_string("dt_dir", '', "data dt partition")    #if dt_dir == "" : FLAGS.dt_dir = (date.today() + timedelta(-1)).strftime('%Y%m%d')
tf.app.flags.DEFINE_string("model_dir", '', "model check point dir")
tf.app.flags.DEFINE_string("servable_model_dir", '', "export servable model for TensorFlow Serving")
tf.app.flags.DEFINE_string("task_type", 'train', "task type {train, infer, eval, export}")
tf.app.flags.DEFINE_boolean("clear_existing_model", False, "clear existing model or not")
tf.app.flags.DEFINE_string("reg_type", 'l2', "Regularization type {l1 ,l2}")
#tf.app.flags.DEFINE_string("loss_type", 'cross', "loss type {cross, square}")  #设置损失函数类型，交叉熵/平方损失

#os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

#hooks = [tf.train.ProfilerHook(output_dir="/home/dc/test/time", save_steps=500, show_dataflow=True, show_memory=True)]

#hooks = [tf.train.SummarySaverHook(save_steps=500, output_dir="/home/dc/test/time2", scaffold=tf.train.Scaffold(summary_op=tf.summary.merge_all()))]


#1 1:0.5 2:0.03519 3:1 4:0.02567 7:0.03708 8:0.01705 9:0.06296 10:0.18185 11:0.02497 12:1 14:0.02565 15:0.03267 17:0.0247 18:0.03158 20:1 22:1 23:0.13169 24:0.02933 27:0.18159 31:0.0177 34:0.02888 38:1 51:1 63:1 132:1 164:1 236:1
#input_fn是estimator的格式化输入数据函数，要求返回一个字典和一个标签,字典格式为"特征名":feature_value,其中值为Tensor或Sparse Tensor，标签格式为Tensor
def input_fn(filenames, batch_size=32, num_epochs=1, perform_shuffle=False):
    print('Parsing', filenames)
    def decode_libsvm(line):
        #columns = tf.decode_csv(value, record_defaults=CSV_COLUMN_DEFAULTS)
        #features = dict(zip(CSV_COLUMNS, columns))
        #labels = features.pop(LABEL_COLUMN)
        columns = tf.string_split([line], ' ')
        labels = tf.strings.to_number(columns.values[0], out_type=tf.float32)
        splits = tf.string_split(columns.values[1:], ':')
        id_vals = tf.reshape(splits.values,splits.dense_shape)
        feat_ids, feat_vals = tf.split(id_vals,num_or_size_splits=2,axis=1)
        feat_ids = tf.strings.to_number(feat_ids, out_type=tf.int32)
        feat_vals = tf.strings.to_number(feat_vals, out_type=tf.float32)
        #feat_ids = tf.reshape(feat_ids,shape=[-1,FLAGS.field_size])
        #for i in range(splits.dense_shape.eval()[0]):
        #    feat_ids.append(tf.string_to_number(splits.values[2*i], out_type=tf.int32))
        #    feat_vals.append(tf.string_to_number(splits.values[2*i+1]))
        #return tf.reshape(feat_ids,shape=[-1,field_size]), tf.reshape(feat_vals,shape=[-1,field_size]), labels
        return {"feat_ids": feat_ids, "feat_vals": feat_vals}, labels

    # Extract lines from input files using the Dataset API, can pass one filename or filename list
    #参考https://blog.csdn.net/xinjieyuan/article/details/90698038  tf.data.TextLineDataset(filenames)函数会自动构造一个Dataset，原始文中一行，就是一个元素；.map对每个元素进行操作
    #num_parallel_calls并行数量，CPU几核心设置为几
    dataset = tf.data.TextLineDataset(filenames).map(decode_libsvm, num_parallel_calls=10).prefetch(batch_size)    # multi-thread pre-process then prefetch,prefetch(n),n=batch_size

    # Randomizes input using a window of 256 elements (read into memory)
    #打乱数据，避免过拟合
    if perform_shuffle:
        dataset = dataset.shuffle(buffer_size=256)

    # epochs from blending together.
    #每个数据重复的个数 repeat
    dataset = dataset.repeat(num_epochs)
    dataset = dataset.batch(batch_size) # Batch size to use,每批次取出batch_size的数据

    #return dataset.make_one_shot_iterator()
    #iterator解释详见：https://blog.csdn.net/briblue/article/details/80962728
    iterator = dataset.make_one_shot_iterator()
    batch_features, batch_labels = iterator.get_next()  #是一个迭代器，一直取数据
    #return tf.reshape(batch_ids,shape=[-1,field_size]), tf.reshape(batch_vals,shape=[-1,field_size]), batch_labels
    return batch_features, batch_labels

def model_fn(features, labels, mode, params):
    """Bulid Model function f(x) for Estimator."""
    #超参数------hyperparameters----
    field_size = params["field_size"]  #one hot前
    feature_size = params["feature_size"]  #one hot后
    embedding_size = params["embedding_size"]
    l2_reg = params["l2_reg"]
    learning_rate = params["learning_rate"]
    #batch_norm_decay = params["batch_norm_decay"]
    #optimizer = params["optimizer"]
    layers = map(int, params["deep_layers"].split(','))   #python3中map变成迭代器，需要改写取数模块的代码
    layers = list(layers) 
    dropout = map(float, params["dropout"].split(','))
    dropout = list(dropout)
    #reg_type = params["reg_type"]

    #------bulid weights------
    FM_B = tf.compat.v1.get_variable(name='fm_bias', shape=[1], initializer=tf.constant_initializer(0.0))   #截距项
    FM_W = tf.compat.v1.get_variable(name='fm_w', shape=[feature_size], initializer=tf.glorot_normal_initializer())   #就是FM部分一次项的权重 #它从以0为中心的截断正态分布中抽取样本,一维数组
    FM_V = tf.compat.v1.get_variable(name='fm_v', shape=[feature_size, embedding_size], initializer=tf.glorot_normal_initializer())  #embedding权重,feature_size代表将离散特征转换成one hot后的特征长度，embedding_size代表每个embedding的长度，即one hot向Dense ector转换时候的Vik的k，Vik长度为:feature_size*embedding_size

    #------build feaure-------
    feat_ids = features['feat_ids']
    feat_ids = tf.reshape(feat_ids,shape=[-1,field_size])      #shape[-1],代表根据列来计算适配的行
    feat_vals = features['feat_vals']
    feat_vals = tf.reshape(feat_vals,shape=[-1,field_size])    #使用one hot前的数据维度来适配，一行数据就是一个样本点

    #------build f(x)------
    with tf.compat.v1.variable_scope("First-order"):
        feat_wgts = tf.nn.embedding_lookup(FM_W, feat_ids)              # None * F * 1  #根据feat_id找到FM_W
        y_w = tf.reduce_sum(tf.multiply(feat_wgts, feat_vals),1)        # 对应位置元素相乘后再按行求和,就是y_w

    with tf.compat.v1.variable_scope("Second-order"):
        embeddings = tf.nn.embedding_lookup(FM_V, feat_ids)             # None * F * K  #根据id找到FM_V，即Vik
        feat_vals = tf.reshape(feat_vals, shape=[-1, field_size, 1])    # 把所有数据用"[]"框起来，便于下一步矩阵运算
        embeddings = tf.multiply(embeddings, feat_vals)                 # vij*xi
        sum_square = tf.square(tf.reduce_sum(embeddings,1))             # 参考FM交互项转换公式
        square_sum = tf.reduce_sum(tf.square(embeddings),1)
        y_v = 0.5*tf.reduce_sum(tf.subtract(sum_square, square_sum),1)	# None * 1

    with tf.compat.v1.variable_scope("Deep-part"):
        # 148-159代码batch_norm模块是没用到的 #更正，写了新的batch_norm_layer进行batch_norm，这里主要传递train_phase等参数
        if FLAGS.batch_norm:
            #normalizer_fn = tf.contrib.layers.batch_norm
            #normalizer_fn = tf.layers.batch_normalization
            if mode == tf.estimator.ModeKeys.TRAIN:    #训练模式
                train_phase = True
                #normalizer_params = {'decay': batch_norm_decay, 'center': True, 'scale': True, 'updates_collections': None, 'is_training': True, 'reuse': None}
            else:
                train_phase = False
                #normalizer_params = {'decay': batch_norm_decay, 'center': True, 'scale': True, 'updates_collections': None, 'is_training': False, 'reuse': True}
        else:
            normalizer_fn = None
            normalizer_params = None

        deep_inputs = tf.reshape(embeddings,shape=[-1,field_size*embedding_size]) # None * (F*K) 一行是一个样本点
        for i in range(len(layers)):
            #if FLAGS.batch_norm:
            #    deep_inputs = batch_norm_layer(deep_inputs, train_phase=train_phase, scope_bn='bn_%d' %i)
                #normalizer_params.update({'scope': 'bn_%d' %i})
            deep_inputs = tf.contrib.layers.fully_connected(inputs=deep_inputs, num_outputs=layers[i], \
                #normalizer_fn=normalizer_fn, normalizer_params=normalizer_params, \
                weights_regularizer=tf.contrib.layers.l2_regularizer(l2_reg), scope='mlp%d' % i)
            if FLAGS.batch_norm:
                deep_inputs = batch_norm_layer(deep_inputs, train_phase=train_phase, scope_bn='bn_%d' %i)   #放在RELU之后 https://github.com/ducha-aiki/caffenet-benchmark/blob/master/batchnorm.md#bn----before-or-after-relu #关于BN的解释https://www.cnblogs.com/guoyaohua/p/8724433.html
            if mode == tf.estimator.ModeKeys.TRAIN:
                deep_inputs = tf.nn.dropout(deep_inputs, keep_prob=dropout[i])                              #Apply Dropout after all BN layers and set dropout=0.8(drop_ratio=0.2)
                #deep_inputs = tf.layers.dropout(inputs=deep_inputs, rate=dropout[i], training=mode == tf.estimator.ModeKeys.TRAIN)

        y_deep = tf.contrib.layers.fully_connected(inputs=deep_inputs, num_outputs=1, activation_fn=tf.identity, \
                weights_regularizer=tf.contrib.layers.l2_regularizer(l2_reg), scope='deep_out')     #tf.identity相当于恒等变化，是多少就输出多少
        y_d = tf.reshape(y_deep,shape=[-1])
        #sig_wgts = tf.get_variable(name='sigmoid_weights', shape=[layers[-1]], initializer=tf.glorot_normal_initializer())
        #sig_bias = tf.get_variable(name='sigmoid_bias', shape=[1], initializer=tf.constant_initializer(0.0))
        #deep_out = tf.nn.xw_plus_b(deep_inputs,sig_wgts,sig_bias,name='deep_out')

    with tf.compat.v1.variable_scope("DeepFM-out"):
        #y_bias = FM_B * tf.ones_like(labels, dtype=tf.float32)  # None * 1  warning;这里不能用label，否则调用predict/export函数会出错，train/evaluate正常；初步判断estimator做了优化，用不到label时不传
        y_bias = FM_B * tf.ones_like(y_d, dtype=tf.float32)      # None * 1
        y = y_bias + y_w + y_v + y_d
        pred = tf.sigmoid(y)
        #pred = tf.Session().run(y)

    predictions={"prob": pred}    #保存输出为字典
    export_outputs = {tf.saved_model.DEFAULT_SERVING_SIGNATURE_DEF_KEY: tf.estimator.export.PredictOutput(predictions)}  #输出
    # Provide an estimator spec for `ModeKeys.PREDICT`
    if mode == tf.estimator.ModeKeys.PREDICT:      #预测模式
        return tf.estimator.EstimatorSpec(
                mode=mode,
                predictions=predictions,
                export_outputs=export_outputs)

    #------bulid loss------
    #交叉熵损失函数
    if FLAGS.reg_type == 'l2':
        if FLAGS.loss_type == 'log_loss':
            loss = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(logits=y, labels=labels)) + \
                l2_reg * tf.nn.l2_loss(FM_W) + \
                l2_reg * tf.nn.l2_loss(FM_V)
        if FLAGS.loss_type == 'square_loss':
            loss = tf.reduce_mean(tf.square(pred - y)) + \
                l2_reg * tf.nn.l2_loss(FM_W) + \
                l2_reg * tf.nn.l2_loss(FM_V)
    if FLAGS.reg_type == 'l1':
        if FLAGS.loss_type == 'log_loss':
            loss = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(logits=y, labels=labels)) + \
                tf.contrib.layers.l1_regularizer(l2_reg)(FM_W) + \
                tf.contrib.layers.l1_regularizer(l2_reg)(FM_V)
        if FLAGS.loss_type == 'square_loss':
            loss = tf.reduce_mean(tf.square(pred - y)) + \
                l2_reg * tf.nn.l2_loss(FM_W) + \
                l2_reg * tf.nn.l2_loss(FM_V)

    # Provide an estimator spec for `ModeKeys.EVAL`
    eval_metric_ops = {
        "auc": tf.compat.v1.metrics.auc(labels, pred)
    }
    if mode == tf.estimator.ModeKeys.EVAL:      #评估模式
        return tf.estimator.EstimatorSpec(
                mode=mode,
                predictions=predictions,
                loss=loss,
                eval_metric_ops=eval_metric_ops)

    #------bulid optimizer------
    if FLAGS.optimizer == 'Adam':
        optimizer = tf.compat.v1.train.AdamOptimizer(learning_rate=learning_rate, beta1=0.9, beta2=0.999, epsilon=1e-8)
        #optimizer = tf.train.GradientDescentOptimizer(0.02)
    elif FLAGS.optimizer == 'Adagrad':
        optimizer = tf.train.AdagradOptimizer(learning_rate=learning_rate, initial_accumulator_value=1e-8)
    elif FLAGS.optimizer == 'Momentum':
        optimizer = tf.train.MomentumOptimizer(learning_rate=learning_rate, momentum=0.95)
    elif FLAGS.optimizer == 'ftrl':
        optimizer = tf.train.FtrlOptimizer(learning_rate)
    elif FLAGS.optimizer == 'GD':
        optimizer = tf.train.GradientDescentOptimizer(learning_rate)

    train_op = optimizer.minimize(loss, global_step=tf.compat.v1.train.get_global_step())

    # Provide an estimator spec for `ModeKeys.TRAIN` modes
    if mode == tf.estimator.ModeKeys.TRAIN:
        return tf.estimator.EstimatorSpec(
                mode=mode,
                predictions=predictions,
                loss=loss,
                train_op=train_op)

    # Provide an estimator spec for `ModeKeys.EVAL` and `ModeKeys.TRAIN` modes.
    #return tf.estimator.EstimatorSpec(
    #        mode=mode,
    #        loss=loss,
    #        train_op=train_op,
    #        predictions={"prob": pred},
    #        eval_metric_ops=eval_metric_ops)

def batch_norm_layer(x, train_phase, scope_bn):       #BN操作，返回操作后的BN数据
    bn_train = tf.contrib.layers.batch_norm(x, decay=FLAGS.batch_norm_decay, center=True, scale=True, updates_collections=None, is_training=True,  reuse=None, scope=scope_bn)  #对应训练模式的BN
    bn_infer = tf.contrib.layers.batch_norm(x, decay=FLAGS.batch_norm_decay, center=True, scale=True, updates_collections=None, is_training=False, reuse=True, scope=scope_bn)  #对应评估模式的BN
    z = tf.cond(tf.cast(train_phase, tf.bool), lambda: bn_train, lambda: bn_infer)     #cond类似if语句，cast是输出True、False的语句(图结构控制数据流向的函数)，即如果train_phase is True，执行bn_train，反之执行bn_infer
    return z

def set_dist_env():
    if FLAGS.dist_mode == 1:        # 本地分布式测试模式1 chief, 1 ps, 1 evaluator
        ps_hosts = FLAGS.ps_hosts.split(',')
        chief_hosts = FLAGS.chief_hosts.split(',')
        task_index = FLAGS.task_index
        job_name = FLAGS.job_name
        print('ps_host', ps_hosts)
        print('chief_hosts', chief_hosts)
        print('job_name', job_name)
        print('task_index', str(task_index))
        # 无worker参数
        tf_config = {
            'cluster': {'chief': chief_hosts, 'ps': ps_hosts},
            'task': {'type': job_name, 'index': task_index }
        }
        print(json.dumps(tf_config))
        os.environ['TF_CONFIG'] = json.dumps(tf_config)
    elif FLAGS.dist_mode == 2:      # 集群分布式模式
        ps_hosts = FLAGS.ps_hosts.split(',')
        worker_hosts = FLAGS.worker_hosts.split(',')
        chief_hosts = worker_hosts[0:1] # get first worker as chief
        worker_hosts = worker_hosts[2:] # the rest as worker
        task_index = FLAGS.task_index
        job_name = FLAGS.job_name
        print('ps_host', ps_hosts)
        print('worker_host', worker_hosts)
        print('chief_hosts', chief_hosts)
        print('job_name', job_name)
        print('task_index', str(task_index))
        # use #worker=0 as chief
        if job_name == "worker" and task_index == 0:
            job_name = "chief"
        # use #worker=1 as evaluator
        if job_name == "worker" and task_index == 1:
            job_name = 'evaluator'
            task_index = 0
        # the others as worker
        if job_name == "worker" and task_index > 1:
            task_index -= 2

        tf_config = {
            'cluster': {'chief': chief_hosts, 'worker': worker_hosts, 'ps': ps_hosts},
            'task': {'type': job_name, 'index': task_index }
        }
        print(json.dumps(tf_config))
        os.environ['TF_CONFIG'] = json.dumps(tf_config)

def main(_):    #主函数中的tf.app.run()会调用main，并传递参数，因此必须在main函数中设置一个参数位置。https://blog.csdn.net/KEE_HA/article/details/85047522
    #------check Arguments------
    if FLAGS.dt_dir == "":
        FLAGS.dt_dir = (date.today()).strftime('%Y%m%d')
        #FLAGS.dt_dir = (date.today() + timedelta(-1)).strftime('%Y%m%d')
    FLAGS.model_dir = FLAGS.model_dir + FLAGS.dt_dir
    #FLAGS.data_dir  = FLAGS.data_dir + FLAGS.dt_dir

    print('task_type ', FLAGS.task_type)
    print('model_dir ', FLAGS.model_dir)
    print('data_dir ', FLAGS.data_dir)
    print('dt_dir ', FLAGS.dt_dir)
    print('num_epochs ', FLAGS.num_epochs)
    print('feature_size ', FLAGS.feature_size)
    print('field_size ', FLAGS.field_size)
    print('embedding_size ', FLAGS.embedding_size)
    print('batch_size ', FLAGS.batch_size)
    print('deep_layers ', FLAGS.deep_layers)
    print('dropout ', FLAGS.dropout)
    print('loss_type ', FLAGS.loss_type)
    print('optimizer ', FLAGS.optimizer)
    print('learning_rate ', FLAGS.learning_rate)
    print('batch_norm_decay ', FLAGS.batch_norm_decay)
    print('batch_norm ', FLAGS.batch_norm)
    print('l2_reg ', FLAGS.l2_reg)

    #------init Envs------
    tr_files = glob.glob("%s/tr*libsvm" % FLAGS.data_dir)  #训练，glob.glob是正则搜索
    random.shuffle(tr_files)                               #打乱文件夹
    print("tr_files:", tr_files)
    va_files = glob.glob("%s/va*libsvm" % FLAGS.data_dir)  #评估
    print("va_files:", va_files)
    te_files = glob.glob("%s/te*libsvm" % FLAGS.data_dir)  #预测
    print("te_files:", te_files)

    if FLAGS.clear_existing_model:
        try:
            shutil.rmtree(FLAGS.model_dir)    #删除存放模型的文件夹
        except Exception as e:
            print(e, "at clear_existing_model")   #报错就弹出错误码并且定位到错误位置
        else:
            print("existing model cleaned at %s" % FLAGS.model_dir)  #清空完毕后输出完毕字样

    set_dist_env()    #选择分布式模式，默认模式码为0，单机

    #------bulid Tasks------
    model_params = {
        "field_size": FLAGS.field_size,
        "feature_size": FLAGS.feature_size,
        "embedding_size": FLAGS.embedding_size,
        "learning_rate": FLAGS.learning_rate,
        "batch_norm_decay": FLAGS.batch_norm_decay,
        "l2_reg": FLAGS.l2_reg,
        "deep_layers": FLAGS.deep_layers,
        "dropout": FLAGS.dropout
    }
    config = tf.estimator.RunConfig().replace(session_config = tf.compat.v1.ConfigProto(device_count={'GPU':0, 'CPU':FLAGS.num_threads}),
            log_step_count_steps=FLAGS.log_steps, save_summary_steps=FLAGS.log_steps)
    DeepFM = tf.estimator.Estimator(model_fn=model_fn, model_dir=FLAGS.model_dir, params=model_params, config=config)

    if FLAGS.task_type == 'train':    #训练模式
        train_spec = tf.estimator.TrainSpec(input_fn=lambda: input_fn(tr_files, num_epochs=FLAGS.num_epochs, batch_size=FLAGS.batch_size))
        eval_spec = tf.estimator.EvalSpec(input_fn=lambda: input_fn(va_files, num_epochs=1, batch_size=FLAGS.batch_size), steps=None, start_delay_secs=1000, throttle_secs=1200)
        tf.estimator.train_and_evaluate(DeepFM, train_spec, eval_spec)
    elif FLAGS.task_type == 'eval':   #评估模式
        DeepFM.evaluate(input_fn=lambda: input_fn(va_files, num_epochs=1, batch_size=FLAGS.batch_size))
    elif FLAGS.task_type == 'infer':  #预测模式
        preds = DeepFM.predict(input_fn=lambda: input_fn(te_files, num_epochs=1, batch_size=FLAGS.batch_size), predict_keys="prob")
        with open(FLAGS.data_dir+"/pred.txt", "w") as fo:
            for prob in preds:
                fo.write("%f\n" % (prob['prob']))
    elif FLAGS.task_type == 'export':   #保存模型
        #feature_spec = tf.feature_column.make_parse_example_spec(feature_columns)
        #feature_spec = {
        #    'feat_ids': tf.FixedLenFeature(dtype=tf.int64, shape=[None, FLAGS.field_size]),
        #    'feat_vals': tf.FixedLenFeature(dtype=tf.float32, shape=[None, FLAGS.field_size])
        #}
        #serving_input_receiver_fn = tf.estimator.export.build_parsing_serving_input_receiver_fn(feature_spec)
        feature_spec = {
            'feat_ids': tf.placeholder(dtype=tf.int64, shape=[None, FLAGS.field_size], name='feat_ids'),
            'feat_vals': tf.placeholder(dtype=tf.float32, shape=[None, FLAGS.field_size], name='feat_vals')
        }
        serving_input_receiver_fn = tf.estimator.export.build_raw_serving_input_receiver_fn(feature_spec)
        DeepFM.export_savedmodel(FLAGS.servable_model_dir, serving_input_receiver_fn)

if __name__ == "__main__":
    tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.INFO)   #将TensorFlow日志信息输出到屏幕
    tf.compat.v1.app.run()       #配合tf.app.flags使用，即通过处理flag解析，然后执行main函数。。正是因为这里的tf.app.run，前面的main函数才要写成main(_)
    #tf.compat.v1.app.run(options=tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE), run_metadata=tf.RunMetadata())
