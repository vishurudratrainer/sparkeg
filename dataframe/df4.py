# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 22:40:58 2021

@author: ASUS
"""

import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("df1").getOrCreate()


df = spark.read.csv("C:/sparkeg/code/data/zipcodes.csv")
df.printSchema()
df.show()
df.coalesce(1).write.option("header",True) .csv("C:/sparkeg/code/data/zipexport22.csv")
df = spark.read.format("csv").load("C:/sparkeg/code/data/zipcodes.csv")

#df = spark.read.format("org.apache.spark.sql.csv").load("C:/sparkeg/code/data/zipcodes.csv")
#df.printSchema()


df2 = spark.read.option("header",True) .csv("C:/sparkeg/code/data/zipcodes.csv")
df2.printSchema()
df2.show()


df3 = spark.read.options(delimiter=',') .csv("C:/sparkeg/code/data/zipcodes.csv")


df4 = spark.read.options(inferSchema='True',delimiter=',') .csv("C:/sparkeg/code/data/zipcodes.csv")

df3 = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("C:/sparkeg/code/data/zipcodes.csv")


df.write.option("header",True) \
 .csv("C:/sparkeg/code/data/zipexport")
df2.write.options(header='True', delimiter=',') \
 .csv("C:/sparkeg/code/data/zipexport")
 