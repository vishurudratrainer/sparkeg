# -*- coding: utf-8 -*-
"""
Created on Mon Dec 13 11:04:03 2021

@author: ASUS
"""
#pip install requests
import requests

response= requests.get("https://jsonplaceholder.typicode.com/todos")
data=response.text

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, BooleanType,StringType, IntegerType,BooleanType,DoubleType
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("RDD") \
    .getOrCreate()
import json

rdd = spark.sparkContext.parallelize(json.loads(data))

schema = StructType([ \

    StructField("id", IntegerType(), True), \
    StructField("title", StringType(), True), \
    StructField("userId", IntegerType(), True), \
    StructField("completed", BooleanType(), True), \

  ])
 
df = spark.createDataFrame(data=rdd,schema=schema)
df.show(3)