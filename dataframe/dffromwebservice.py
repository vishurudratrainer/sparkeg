# -*- coding: utf-8 -*-
"""
Created on Mon Dec 13 11:04:03 2021

@author: ASUS
"""
#pip install requests
import requests

response= requests.get("https://jsonplaceholder.typicode.com/todos")
data=response.text
with open("C://files//todos.json","w") as op:
    op.write(str(data))
    
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("RDD") \
    .getOrCreate()

resdf= spark.read.option("multiline", True).json("C://files//todos.json")
resdf.show(3)