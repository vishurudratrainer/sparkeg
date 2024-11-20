# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:04:28 2024

@author: VISHWA
"""

#. How to import PySpark and check the version?



















import findspark
findspark.init()

# Creating a SparkSession: A SparkSession is the entry point for using the PySpark DataFrame and SQL API.
# To create a SparkSession, use the following code
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD").getOrCreate()

# Get version details
print(spark.version)
