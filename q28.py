# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:31:11 2024

@author: VISHWA

How to replace missing values of multiple numeric columns with the mean?
Difficulty Level: L2

Input

df = spark.createDataFrame([
("A", 1, None),
("B", None, 123 ),
("B", 3, 456),
("D", 6, None),
], ["Name", "var1", "var2"])

df.show()
"""





"""
from pyspark.ml.feature import Imputer

column_names = ["var1", "var2"]

# Initialize the Imputer
imputer = Imputer(inputCols= column_names, outputCols= column_names, strategy="mean")

# Fit the Imputer
model = imputer.fit(df)

#Transform the dataset
imputed_df = model.transform(df)

imputed_df.show(5)

"""