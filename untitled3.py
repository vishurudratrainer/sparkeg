# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 11:48:34 2023

@author: ASUS
"""

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('RDD.com') \
                    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("C://sparkeg/Wages.csv")