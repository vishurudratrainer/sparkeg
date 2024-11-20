# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 19:55:50 2024

@author: VISHWA
"""
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
sc = SparkContext(appName="PythonStreamingQueueStream")
ssc = StreamingContext(spark.sparkContext, 1)
    
    
rddQueue = []
for i in range(5):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]
    
inputStream = ssc.queueStream(rddQueue)
mappedStream = inputStream.map(lambda x: (x % 10, 1))
reducedStream = mappedStream.reduceByKey(lambda a, b: a + b)
reducedStream.pprint()

ssc.start()
time.sleep(6)
ssc.stop(stopSparkContext=True, stopGraceFully=True)