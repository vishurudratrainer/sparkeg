# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:30:11 2024

@author: VISHWA
"""

"""
How to View PySpark Cluster Configuration Details?
Difficulty Level: L1

Show Solution
# Print all configurations
for k,v in spark.sparkContext.getConf().getAll():
print(f"{k} : {v}")

"""