# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:12:11 2024

@author: VISHWA

How to get frequency counts of unique items of a column?

Calculte the frequency counts of each unique value

Input

from pyspark.sql import Row

# Sample data
data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

# create DataFrame
df = spark.createDataFrame(data)

# show DataFrame
df.show()

"""




###ANS

"""
df.groupBy("job").count().show()


"""