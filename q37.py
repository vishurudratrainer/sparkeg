# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:27:40 2024

@author: VISHWA
"""


"""
How to reverse the rows of a dataframe?
Difficulty Level: L2

Reverse all the rows of dataframe df.

Input

# Create a numeric DataFrame
data = [(1, 2, 3, 4),
(2, 3, 4, 5),
(3, 4, 5, 6),
(4, 5, 6, 7)]

df = spark.createDataFrame(data, ["col_1", "col_2", "col_3", "col_4"])

# Print DataFrame
df.show()

"""






"""
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

# Define window specification
w = Window.orderBy(monotonically_increasing_id())

# Add index
df = df.withColumn("id", row_number().over(w) - 1)

df_2 = df.orderBy("id", ascending=False).drop("id")

df_2.show()

"""