# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 22:50:57 2021

@author: ASUS
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
rdd=spark.sparkContext.parallelize([1,2,3,4,56])
print("RDD count :"+str(rdd.count()))

rdd = spark.sparkContext.emptyRDD
print('Empty',rdd)
rdd2 = spark.sparkContext.parallelize([])
print('SECOND',rdd2.count())