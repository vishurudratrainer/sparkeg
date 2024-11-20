# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:28:49 2024

@author: VISHWA

How to get the nrows, ncolumns, datatype of a dataframe?
Difficiulty Level: L1

Get the number of rows, columns, datatype and summary statistics of each column of the Churn_Modelling dataset. Also get the numpy array and list equivalent of the dataframe

url = "https://raw.githubusercontent.com/selva86/datasets/master/Churn_Modelling.csv"

spark.sparkContext.addFile(url)

df = spark.read.csv(SparkFiles.get("Churn_Modelling.csv"), header=True, inferSchema=True)

#df = spark.read.csv("C:/Users/RajeshVaddi/Documents/MLPlus/DataSets/Churn_Modelling.csv", header=True, inferSchema=True)

df.show(5, truncate=False)
"""











"""
# For number of rows
nrows = df.count()
print("Number of Rows: ", nrows)

# For number of columns
ncols = len(df.columns)
print("Number of Columns: ", ncols)

# For data types of each column
datatypes = df.dtypes
print("Data types: ", datatypes)
"""