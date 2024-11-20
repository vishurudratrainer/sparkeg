# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:11:33 2024

@author: VISHWA


How to get the minimum, 25th percentile, median, 75th, and max of a numeric column?
Difficulty Level: L2

Compute the minimum, 25th percentile, median, 75th, and maximum of column Age

input

# Create a sample DataFrame
data = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50), ("F", 15), ("G", 28), ("H", 54), ("I", 41), ("J", 86)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()
"""





#ANS
"""
# Calculate percentiles
quantiles = df.approxQuantile("Age", [0.0, 0.25, 0.5, 0.75, 1.0], 0.01)

print("Min: ", quantiles[0])
print("25th percentile: ", quantiles[1])
print("Median: ", quantiles[2])
print("75th percentile: ", quantiles[3])
print("Max: ", quantiles[4])


"""