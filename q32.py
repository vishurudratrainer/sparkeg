# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:33:49 2024

@author: VISHWA
How to format all the values in a dataframe as percentages?
Difficulty Level: L2

Input

# Sample data
data = [(0.1, .08), (0.2, .06), (0.33, .02)]
df = spark.createDataFrame(data, ["numbers_1", "numbers_2"])

df.show()
+---------+---------+
|numbers_1|numbers_2|
+---------+---------+
| 0.1| 0.08|
| 0.2| 0.06|
| 0.33| 0.02|
+---------+---------+
Show Solution
from pyspark.sql.functions import concat, col, lit

columns = ["numbers_1", "numbers_2"]

for col_name in columns:
df = df.withColumn(col_name, concat((col(col_name) * 100).cast('decimal(10, 2)'), lit("%")))

df.show()

+---------+---------+
|numbers_1|numbers_2|
+---------+---------+
| 10.00%| 8.00%|
| 20.00%| 6.00%|
| 33.00%| 2.00%|
+---------+---------+

"""

