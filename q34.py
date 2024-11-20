# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:35:36 2024

@author: VISHWA
How to get the row number of the nth largest value in a column?
Difficulty Level: L2

Input

from pyspark.sql import Row

# Sample Data
data = [
Row(id=1, column1=5),
Row(id=2, column1=8),
Row(id=3, column1=12),
Row(id=4, column1=1),
Row(id=5, column1=15),
Row(id=6, column1=7),
]

df = spark.createDataFrame(data)
df.show()
"""




"""
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number

window = Window.orderBy(desc("column1"))
df = df.withColumn("row_number", row_number().over(window))

n = 3 # We're interested in the 3rd largest value.
row = df.filter(df.row_number == n).first()

if row:
print("Row number:", row.row_number)
print("Column value:", row.column1)
"""