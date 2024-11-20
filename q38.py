# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:29:07 2024

@author: VISHWA
"""


"""
How to calculate Mode of a PySpark DataFrame column?
Difficulty Level: L1

Input

# Create a sample DataFrame
data = [(1, 2, 3), (2, 2, 3), (2, 2, 4), (1, 2, 3), (1, 1, 3)]
columns = ["col1", "col2", "col3"]

df = spark.createDataFrame(data, columns)

df.show()

"""



"""
from pyspark.sql.functions import col

df_grouped = df.groupBy('col2').count()
mode_df = df_grouped.orderBy(col('count').desc()).limit(1)

mode_df.show()
"""