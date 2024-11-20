# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:25:31 2024

@author: VISHWA


How to Pivot PySpark DataFrame?
Convert region categories to Columns and sum the revenue

Difficulty Level: L3

# Sample data
data = [
(2021, 1, "US", 5000),
(2021, 1, "EU", 4000),
(2021, 2, "US", 5500),
(2021, 2, "EU", 4500),
(2021, 3, "US", 6000),
(2021, 3, "EU", 5000),
(2021, 4, "US", 7000),
(2021, 4, "EU", 6000),
]

# Create DataFrame
columns = ["year", "quarter", "region", "revenue"]
df = spark.createDataFrame(data, columns)
df.show()
"""






"""
# Execute the pivot operation
pivot_df = df.groupBy("year", "quarter").pivot("region").sum("revenue")

pivot_df.show()
"""