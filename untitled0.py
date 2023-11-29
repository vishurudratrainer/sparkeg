# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 14:35:48 2023

@author: ASUS
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("spark://mtvlabeksa1.brainupgrade.in:80") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
print(spark.sparkContext)
print("Spark App Name : "+ spark.sparkContext.appName)