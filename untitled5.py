# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 16:37:45 2022

@author: ASUS
"""

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  port=3306,
  user="root",
  passwd="root"
)

mydbproperties = {
    host="localhost",
  port=3306,
  user="root",
  passwd="root"

    }

print(mydb) 

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()

spark.sparkContext.setLogLevel('WARN')



df = spark.read.jdbc("jbc://", "sample.table1")

df = sample.read.jdbc("","select * from table where")







 