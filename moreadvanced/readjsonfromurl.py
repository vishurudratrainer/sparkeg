# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 12:16:38 2024

@author: VISHWA
"""

from pyspark import SparkFiles

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Project").getOrCreate()

zip_url = "https://raw.githubusercontent.com/spark-examples/spark-scala-examples/master/src/main/resources/zipcodes.json"

spark.sparkContext.addFile(zip_url)

zip_df = spark.read.json("file:///" +SparkFiles.get("zipcodes.json"))
print(zip_df.show())
