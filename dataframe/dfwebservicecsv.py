# -*- coding: utf-8 -*-
"""
Created on Mon Dec 13 11:23:31 2021

@author: ASUS
"""

import requests
data=requests.get("https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv")
resdata=data.text

with open("C://files//address.csv","w") as op:
    op.write(resdata)
    
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("RDD") \
    .getOrCreate()

resdf= spark.read.options(header='False', inferSchema='True', delimiter=',').csv("C://files//address.csv")
resdf.show(3)