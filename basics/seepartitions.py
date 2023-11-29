# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 21:23:12 2021

@author: ASUS
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()

def f(idx, iterator):
  count = 0
  for _ in iterator:
    count += 1
  return idx, count
# res1.mapPartitionsWithIndex(f).collect()

def f2(iterator):
  yield sum(1 for _ in iterator)

rdd=spark.sparkContext.parallelize([1,2,3,4,56])
#number of elements in eac partition
rdd.mapPartitions(f2).collect()
print("initial partition count:"+str(rdd.getNumPartitions()))
