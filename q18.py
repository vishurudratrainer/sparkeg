# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:21:51 2024

@author: VISHWA


How to compute difference of differences between consecutive numbers of a column?
Difficulty Level: L2

# For the sake of example, we'll create a sample DataFrame
data = [('James', 34, 55000),
('Michael', 30, 70000),
('Robert', 37, 60000),
('Maria', 29, 80000),
('Jen', 32, 65000)]

df = spark.createDataFrame(data, ["name", "age" , "salary"])

df.show()
"""






"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification
df = df.withColumn("id", F.monotonically_increasing_id())
window = Window.orderBy("id")

# Generate the lag of the variable
df = df.withColumn("prev_value", F.lag(df.salary).over(window))

# Compute the difference with lag
df = df.withColumn("diff", F.when(F.isnull(df.salary - df.prev_value), 0)
.otherwise(df.salary - df.prev_value)).drop("id")

df.show()

"""