# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 23:06:23 2021

@author: ASUS
"""


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg,max

spark = SparkSession.builder.appName('RDD').getOrCreate()



df = spark.read.option("header",True).option("inferSchema", True) .csv("C:/sparkeg/Soyabean.csv")

df.show(truncate=False)





df.groupBy("Class") \
    .agg (
         avg("precip").alias("avg_precip"), \
         sum("leaves").alias("sum_leaves")
     ) \
    .show(truncate=False)
    
