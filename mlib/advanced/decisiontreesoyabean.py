# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 13:50:38 2021

@author: ASUS
"""

# import libraries
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from sklearn.metrics import confusion_matrix
from sklearn.datasets import load_iris
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit
# instantiate spark environment
sc = SparkContext("local[*]", "Decision Tree Classifier")
spark = SparkSession(sc)
# load the dataset
df = spark.read.csv("C://sparkeg//Soyabean.csv", inferSchema=True, header=True)

new_cols=(column.replace('.', '_') for column in df.columns)
df2 = df.toDF(*new_cols)
df2.printSchema()
print(df2.columns)

columns= ['date', 'plant_stand', 'precip', 'temp', 'hail', 'crop_hist', 'area_dam', 'sever', 'seed_tmt', 'germ', 'plant_growth', 'leaves', 'leaf_halo', 'leaf_marg', 'leaf_size', 'leaf_shread', 'leaf_malf', 'leaf_mild', 'stem', 'lodging', 'stem_cankers', 'canker_lesion', 'fruiting_bodies', 'ext_decay', 'mycelium', 'int_discolor', 'sclerotia', 'fruit_pods', 'fruit_spots', 'seed', 'mold_growth', 'seed_discolor', 'seed_size', 'shriveling', 'roots']
from pyspark.sql.functions import col
df3=df2.select(*(col(c).cast("float").alias(c) if c!='Class' else col(c).cast("string").alias(c) for c in df2.columns ))
df3=df3.fillna(0)

features = columns
from pyspark.ml.feature import StringIndexer
# estimator
l_indexer = StringIndexer(inputCol="Class", outputCol="label")
df3 = l_indexer.fit(df).transform(df3)
va = VectorAssembler(inputCols = features, outputCol='features')

va_df = va.transform(df3)
va_df = va_df.select(['features', 'label'])
va_df.show(3)

(train, test) = va_df.randomSplit([0.8, 0.2])
help(va_df.randomSplit)
dtc = DecisionTreeClassifier(featuresCol="features", labelCol="label")
dtc = dtc.fit(train)

pred = dtc.transform(test)
pred.show(3)

evaluator=MulticlassClassificationEvaluator(predictionCol="prediction")
acc = evaluator.evaluate(pred)
print("Prediction Accuracy: ", acc)

y_pred=pred.select("prediction").collect()
y_orig=pred.select("label").collect()

cm = confusion_matrix(y_orig, y_pred)
print("Confusion Matrix:")
print(cm)




