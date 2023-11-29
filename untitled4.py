# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 12:30:39 2022

@author: ASUS
"""

import requests
import csv
response= requests.get("https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv")
data=response.text

reader=csv.DictReader(data,delimiter=",")
lst = list(reader)
 
print(lst[0])