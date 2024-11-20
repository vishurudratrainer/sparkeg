# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:21:09 2024

@author: VISHWA

How to calculate the number of characters in each word in a column?
Difficulty Level: L1

# Suppose you have the following DataFrame
data = [("john",), ("alice",), ("bob",)]
df = spark.createDataFrame(data, ["name"])

df.show()
"""











"""
from pyspark.sql import functions as F

df = df.withColumn('word_length', F.length(df.name))
df.show()
"""