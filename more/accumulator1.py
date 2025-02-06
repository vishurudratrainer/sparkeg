# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 22:29:35 2021

@author: ASUS
What is PySpark Accumulator?
Accumulators are write-only and initialize once variables 
where only tasks that are running on workers 
are allowed to update and 
updates from the workers get propagated
 automatically to the driver program. 
 But, only the driver program is allowed to access 
 the Accumulator variable using the value property.

How to create Accumulator variable in PySpark?
Using accumulator() from SparkContext class we can create an Accumulator in PySpark programming. Users can also create Accumulators for custom types using AccumulatorParam class of PySpark.

Some points to note..

sparkContext.accumulator() is used to define accumulator variables.
add() function is used to add/update a value in accumulator
value property on the accumulator variable is used to retrieve the value from the accumulator.
"""


import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("accumulator").getOrCreate()

accum=spark.sparkContext.accumulator(0)
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x:accum.add(x))
print(accum.value)

accuSum=spark.sparkContext.accumulator(0)
def countFun(x):
    global accuSum
    accuSum+=x
rdd.foreach(countFun)
print(accuSum.value)

accumCount=spark.sparkContext.accumulator(0)
rdd2=spark.sparkContext.parallelize([1,2,3,4,5])
rdd2.foreach(lambda x:accumCount.add(1))
print(accumCount.value)
