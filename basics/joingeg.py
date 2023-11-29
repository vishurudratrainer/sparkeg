# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 21:31:10 2021

@author: ASUS
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()

x = spark.sparkContext.parallelize([("a", 1), ("b", 4)])
y = spark.sparkContext.parallelize([("a", 2), ("a", 3)])
#Each pair of elements will be returned as 
#a (k, (v1, v2)) tuple, where (k, v1) is in self and (k, v2) is in other.

x.join(y).collect()