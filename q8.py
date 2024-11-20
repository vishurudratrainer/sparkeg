# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:13:01 2024

@author: VISHWA

How to keep only top 2 most frequent values as it is and replace everything else as ‘Other’?
Difficulty Level: L3

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



#ANS
"""
from pyspark.sql.functions import col, when

# Get the top 2 most frequent jobs
top_2_jobs = df.groupBy('job').count().orderBy('count', ascending=False).limit(2).select('job').rdd.flatMap(lambda x: x).collect()

# Replace all but the top 2 most frequent jobs with 'Other'
df = df.withColumn('job', when(col('job').isin(top_2_jobs), col('job')).otherwise('Other'))

# show DataFrame
df.show()

"""