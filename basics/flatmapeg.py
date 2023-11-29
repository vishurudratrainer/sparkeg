# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 23:44:13 2021

@author: ASUS
"""


def lower(data):
    return data.lower()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()
spark.sparkContext.setLogLevel("INFO")
rdd=spark.sparkContext.parallelize(["Hello","FELLO","below"])
lowerrdd= rdd.flatMap(lower)
print('here',lowerrdd.collect())