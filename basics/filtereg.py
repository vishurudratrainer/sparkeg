# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 23:49:02 2021

@author: ASUS
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()


rdd=spark.sparkContext.parallelize([1,2,3,4,56])
unwanted = [3,56]
postfilterrdd = rdd.filter(lambda x: x not in unwanted)
print('output:',postfilterrdd.take(3))