# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 22:33:06 2021

@author: ASUS
"""


import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("df1").getOrCreate()
#list of columns
columns = ["language","users_count"]
#Below is list of tuples
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)
dfFromRDD1 = rdd.toDF()#This converts from RDD to dataframe
dfFromRDD1.printSchema()
res=dfFromRDD1.collect()
print('Result')
print(res)
#columns = ["language","users_count"]
#dfFromRDD1 = rdd.toDF(columns)
#dfFromRDD1.printSchema()
#res=dfFromRDD1.collect()
#print(res)