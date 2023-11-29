# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 23:30:01 2021

@author: ASUS
"""

def lower(data):
    return data.lower()


def upper(data):
    return data.upper()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()

rdd=spark.sparkContext.parallelize(["Hello","FELLO","below"])
lowerrdd= rdd.map(lower)
print(lowerrdd.take(3))
upperrdd= rdd.map(upper)
print(upperrdd.take(3))
#for data in lowerrdd.collect():
#    print(data)
