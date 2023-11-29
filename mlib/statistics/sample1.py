# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 13:27:27 2021

@author: ASUS
"""

from pyspark.mllib.stat import Statistics
from pyspark.sql import SparkSession
import numpy as np
spark = SparkSession.builder.appName('RDD').getOrCreate()


import numpy as np

from pyspark.mllib.stat import Statistics

mat = spark.sparkContext.parallelize(
    [np.array([1.0, 10.0, 100.0]), np.array([2.0, 20.0, 200.0]), np.array([3.0, 30.0, 300.0])]
)  # an RDD of Vectors

# Compute column summary statistics.
summary = Statistics.colStats(mat)
print(summary.mean())  # a dense vector containing the mean value for each column
print(summary.variance())  # column-wise variance
print(summary.numNonzeros())  # number of nonzeros in each column