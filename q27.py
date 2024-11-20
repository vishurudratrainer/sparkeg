# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:29:41 2024

@author: VISHWA

How to check if a dataframe has any missing values and count of missing values in each column?
Difficulty Level: L2

Input

# Assuming df is your DataFrame
df = spark.createDataFrame([
("A", 1, None),
("B", None, "123" ),
("B", 3, "456"),
("D", None, None),
], ["Name", "Value", "id"])

df.show()
"""






"""from pyspark.sql.functions import col, sum

missing = df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns))
has_missing = any(row.asDict().values() for row in missing.collect())
print(has_missing)

missing_count = missing.collect()[0].asDict()
print(missing_count)

"""