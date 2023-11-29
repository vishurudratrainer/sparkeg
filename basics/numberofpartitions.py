# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 21:15:44 2021

@author: ASUS
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()
rdd=spark.sparkContext.parallelize([1,2,3,4,56])

print("initial partition count:"+str(rdd.getNumPartitions()))


#Create empty RDD with partition
rdd2 = spark.sparkContext.parallelize([],10) #This creates 10 partitions

print("rdd2 partition count:"+str(rdd2.getNumPartitions()))

