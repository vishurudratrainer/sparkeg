# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 21:28:33 2021

@author: ASUS
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()



from operator import add
rdd = spark.sparkContext.parallelize([("a", 1), ("b", 1), ("a", 1)])
newrdd=rdd.reduceByKey(add)
print(newrdd.collect())