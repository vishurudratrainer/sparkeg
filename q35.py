# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:24:21 2024

@author: VISHWA
"""
"""
Create a new column ‘penultimate’ which has the second largest value of each row of df

Input

data = [(10, 20, 30),
(40, 60, 50),
(80, 70, 90)]

df = spark.createDataFrame(data, ["Column1", "Column2", "Column3"])

df.show()

"""







"""
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType

# Define UDF to sort array in descending order
sort_array_desc = F.udf(lambda arr: sorted(arr), ArrayType(IntegerType()))

# Create array from columns, sort in descending order and get the penultimate value
df = df.withColumn("row_as_array", sort_array_desc(F.array(df.columns)))
df = df.withColumn("Penultimate", df['row_as_array'].getItem(1))
df = df.drop('row_as_array')

df.show()
"""