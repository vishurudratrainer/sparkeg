# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:26:14 2024

@author: VISHWA

How to get the mean of a variable grouped by another variable?
Difficulty Level: L3

# Sample data
data = [("1001", "Laptop", 1000),
("1002", "Mouse", 50),
("1003", "Laptop", 1200),
("1004", "Mouse", 30),
("1005", "Smartphone", 700)]

# Create DataFrame
columns = ["OrderID", "Product", "Price"]
df = spark.createDataFrame(data, columns)

df.show()
+-------+----------+-----+
|OrderID| Product|Price|
+-------+----------+-----+
| 1001| Laptop| 1000|
| 1002| Mouse| 50|
| 1003| Laptop| 1200|
| 1004| Mouse| 30|
| 1005|Smartphone| 700|
+-------+----------+-----+
Show Solution
from pyspark.sql.functions import mean

# GroupBy and aggregate
result = df.groupBy("Product").agg(mean("Price").alias("Total_Sales"))

# Show results
result.show()

+----------+-----------+
| Product|Total_Sales|
+----------+-----------+
| Laptop| 1100.0|
| Mouse| 40.0|
|Smartphone| 700.0|
+----------+-----------+
"""

