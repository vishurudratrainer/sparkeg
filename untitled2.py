# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 12:05:11 2022

@author: ASUS
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Dec 13 11:04:03 2021

@author: ASUS
"""
#pip install requests
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, BooleanType,StringType, IntegerType,BooleanType,DoubleType
import json

def convertDFAfterFetch(url,schema):
    response= requests.get(url)
    data=response.text
    spark = SparkSession.builder \
    .master("local[1]") \
    .appName("RDD") \
    .getOrCreate()
    rdd = spark.sparkContext.parallelize(json.loads(data))
    df = spark.createDataFrame(data=rdd,schema=schema)
    return df

url = "https://jsonplaceholder.typicode.com/todos"
schema = StructType([ \

    StructField("id", IntegerType(), True), \
    StructField("title", StringType(), True), \
    StructField("userId", IntegerType(), True), \
    StructField("completed", BooleanType(), True), \

  ])
 
dataframe = convertDFAfterFetch(url,schema)
dataframe.show(3)
