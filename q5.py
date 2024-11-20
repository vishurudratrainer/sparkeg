# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:10:14 2024

@author: VISHWA

How to get the items not common to both list A and list B?
Difficulty Level: L2

Get all items of list_A and list_B not common to both.

Input:

list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]
"""




#Ans
"""
sc = spark.sparkContext

# Convert lists to RDD
rdd_A = sc.parallelize(list_A)
rdd_B = sc.parallelize(list_B)

# Perform subtract operation
result_rdd_A = rdd_A.subtract(rdd_B)
result_rdd_B = rdd_B.subtract(rdd_A)

# Union the two RDDs
result_rdd = result_rdd_A.union(result_rdd_B)


"""