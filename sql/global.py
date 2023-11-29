# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 13:07:58 2021

@author: ASUS
"""


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("RDD") \
    .getOrCreate()

# Read JSON file into dataframe    
df = spark.read.json("C:/sparkeg/code/data/zipcodes.json")
df.createOrReplaceGlobalTempView("zipcodes")


# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.zipcodes").show()

# Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.zipcodes").show()