# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:08:59 2024

@author: VISHWA

 How to get the items of list A not present in list B?
Difficulty Level: L2

Get the items of list_A not present in list_B in PySpark, you can use the subtract operation on RDDs (Resilient Distributed Datasets).

Input:

list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]
Expected Output:

#> [1, 2, 3]
"""



#Ans
"""
sc = spark.sparkContext

# Convert lists to RDD
rdd_A = sc.parallelize(list_A)
rdd_B = sc.parallelize(list_B)

# Perform subtract operation
result_rdd = rdd_A.subtract(rdd_B)

# Collect result
result_list = result_rdd.collect()
print(result_list)
"""