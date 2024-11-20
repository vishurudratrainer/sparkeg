# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:32:02 2024

@author: VISHWA
"""


"""
 How to Divide a PySpark DataFrame randomly in a given ratio (0.8, 0.2)?
Difficulty Level: L1

Show Solution
# Randomly split data (0.8, 0.2)

train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

"""