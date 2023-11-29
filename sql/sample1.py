# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 12:39:01 2021

@author: ASUS
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("RDD") \
    .getOrCreate()

# Read JSON file into dataframe    
df = spark.read.json("C:/sparkeg/code/data/zipcodes.json")
df.createOrReplaceTempView("zipcodes")
allDf = spark.sql("select * from zipcodes")
allDf.show(1)

cityDf=spark.sql("select City from zipcodes")
cityDf.show(5)

selectdf=spark.sql('select City,Country,Location from zipcodes')
selectdf.show(5)

selectadvdf=spark.sql('select City,Country,Location from zipcodes  where City like "%C%"')
selectadvdf.show(5)