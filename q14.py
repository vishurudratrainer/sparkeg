# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:18:49 2024

@author: VISHWA

How to compute the mean squared error on a truth and predicted columns?
Difficulty Level: L2

Input

# Assume you have a DataFrame df with two columns "actual" and "predicted"
# For the sake of example, we'll create a sample DataFrame
data = [(1, 1), (2, 4), (3, 9), (4, 16), (5, 25)]
df = spark.createDataFrame(data, ["actual", "predicted"])

df.show()
"""






"""
# Calculate the squared differences
df = df.withColumn("squared_error", pow((col("actual") - col("predicted")), 2))

# Calculate the mean squared error
mse = df.agg({"squared_error": "avg"}).collect()[0][0]

print(f"Mean Squared Error (MSE) = {mse}")
"""