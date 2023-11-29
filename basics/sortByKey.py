# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 21:35:19 2021

@author: ASUS
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()

x = spark.sparkContext.parallelize([("b",4),("z", 1), ("a", 4)])
res=x.sortByKey(True).collect()
print(res)

tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
res=spark.sparkContext.parallelize(tmp2).sortByKey(True, keyfunc=lambda k: k.lower()).collect()
print(res)