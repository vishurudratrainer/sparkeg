# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 12:09:36 2024

@author: VISHWA
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()
spark.sparkContext.setLogLevel("INFO")
url = "https://raw.githubusercontent.com/asma-hachaichi/Adult-Income-Prediction/refs/heads/main/adult.csv"
from pyspark import SparkFiles
spark.sparkContext.addFile(url)

df = spark.read.csv("file:////"+SparkFiles.get("adult.csv"), header=True, inferSchema= True)
print(df.show())