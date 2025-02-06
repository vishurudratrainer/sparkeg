'''
Using cache() and persist()
methods, Spark provides an optimization mechanism to 
store the intermediate computation
of a Spark DataFrame so they can be reused in subsequent actions.
When you persist a dataset, each node stores its partitioned data 
in memory and reuses them in other actions on that dataset. 
And Spark’s persisted data on nodes are fault-tolerant meaning
if any partition of a Dataset is lost, it will automatically
be recomputed using the original transformations that created it.'''

import pyspark
import time
# Import SparkSession
from pyspark.sql import SparkSession
from pyspark import SparkContext
#https://raw.githubusercontent.com/24jmwangi/pyspark_optimization/refs/heads/master/data/Car%20details%20v3.csv

spark = SparkSession.builder\
.appName("cache and persist example")\
.getOrCreate()


 #read csv and create a data frame
df1 = spark.read.csv("C:/data_eng/pyspark_optimization/data/Car details v3.csv")
# print first n rows of the df
#df1.show(5)

start1= time.time()
# operations before caching the dataframe 

df1.createOrReplaceTempView("df1")
df_q1 = spark.sql("SELECT * FROM df1")

for column in df1.columns:
        print(column)

end1 = time.time()
#execution time using unpersisted df
print("Using unpersisted df",
      (end1-start1) * 10**3, "ms")


'''
Cache function can be used to cache data with a preset StorageLevel
Current default storage level is "MEMORY_AND_DISK" for cache function.
'''
df1.is_cached # will return true if rdd ic cached
df1.cache()

# Persist -Memory only
'''This storage level is used to store 
the RDD / Data frame into the JVM memory of the PySpark. 
If there is enough it will not save some partitions of 
the data and these will be recomputed when required for processing.
Persist function is similar to cache function but user can
specify the storage level using storageLevel Function as parameter. 
If no parameter is passed to this function then it will save with default
storage level
'''
 # persits df to storage level memory only
#df1.persist(pyspark.StorageLevel.MEMORY_ONLY)


# returns true or false depending on the storage level where df is persisted
#df1.storageLevel 


# operations on cached df
start2 = time.time()
df1.createOrReplaceTempView("df2")
df_q2= spark.sql("SELECT* FROM df2")

for column in df1.columns:
        print(column)
# execution time using cached df
end2 = time.time()
print("Using cached df:",
      (end2-start2) * 10**3, "ms")# -*- coding: utf-8 -*-

