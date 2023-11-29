# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 22:37:09 2021

@author: ASUS
"""

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('collect') \
                    .getOrCreate()
spark.sparkContext.setLogLevel("INFO")

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)

dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
res=dfFromRDD2.collect()
print("here",res)
