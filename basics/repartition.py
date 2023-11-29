# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 21:21:15 2021

@author: ASUS
"""


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()
rdd=spark.sparkContext.parallelize([1,2,3,4,56])

print("initial partition count:"+str(rdd.getNumPartitions()))

reparRdd = rdd.repartition(4)
print("re-partition count:"+str(reparRdd.getNumPartitions()))

