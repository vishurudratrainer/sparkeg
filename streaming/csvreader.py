# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 21:21:04 2025

@author: VISHWA
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# Define schema of the csv
userSchema = StructType().add("name", "string").add("salary", "integer")

# Read CSV files from set path
dfCSV = spark.readStream.option("sep", ";").option("header", "false").schema(userSchema).csv("C:/sparkexamples/data")

# We have defined the total salary per name. Note that this is a streaming DataFrame which represents the running sum of the stream.

dfCSV.createOrReplaceTempView("salary")
totalSalary = spark.sql("select name,sum(salary) from salary group by name")

# totalSalary = dfCSV.groupBy("name").sum("salary")

# All that is left is to actually start receiving data and computing the counts. To do this, we set it up to print the complete set of counts (specified by outputMode("complete")) to the console every time they are updated. And then start the streaming computation using start().

# Start running the query that prints the running counts to the console
query = totalSalary.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()