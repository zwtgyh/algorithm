#tensorboard可视化
import tensorflow as tf
from tensorflow.python.platform import gfile
graph = tf.get_default_graph()
graphdef = graph.as_graph_def()
_ = tf.train.import_meta_graph("Desktop/ww/model.ckpt-7086.meta")
summary_write = tf.summary.FileWriter("Desktop/ww/log" , graph)
summary_write.close()

tensorboard --logdir=Desktop/ww/log/ --host=127.0.0.1 --port=6006



#读取ckpt系数
from tensorflow.python import pywrap_tensorflow
reader = pywrap_tensorflow.NewCheckpointReader("Desktop/ww/model.ckpt-7086")
var_to_shape_map = reader.get_variable_to_shape_map()
for key in var_to_shape_map:
    print("tensor_name: ", key)
    print(reader.get_tensor(key))


