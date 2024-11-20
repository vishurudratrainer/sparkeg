# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:19:39 2024

@author: VISHWA


 How to convert the first character of each element in a series to uppercase?
Difficulty Level: L1

# Suppose you have the following DataFrame
data = [("john",), ("alice",), ("bob",)]
df = spark.createDataFrame(data, ["name"])

df.show()
"""










"""
from pyspark.sql.functions import initcap

# Convert the first character to uppercase
df = df.withColumn("name", initcap(df["name"]))

df.show()

"""