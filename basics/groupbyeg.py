# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 23:53:43 2021

@author: ASUS
"""


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()
rdd = spark.sparkContext.parallelize([1, 1, 2, 3, 5, 8])
result = rdd.groupBy(lambda x: x % 2).collect()
sorted([(x, sorted(y)) for (x, y) in result])