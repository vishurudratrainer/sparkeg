# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 13:50:38 2021

@author: ASUS
"""

# import libraries
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
# instantiate spark environment
sc = SparkContext("local[*]", "Decision Tree Classifier")
spark = SparkSession(sc)
# load the dataset
df = spark.read.csv("C://files//iris.csv", inferSchema=True, header=True).toDF("sep_len", "sep_wid", "pet_len", "pet_wid", "label")
df.show(5)


from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 13:43:14 2021

@author: ASUS
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from sklearn.metrics import confusion_matrix
from sklearn.datasets import load_iris
import pandas as pd
from pyspark.sql import SparkSession


iris = load_iris()
df_iris = pd.DataFrame(iris.data, columns=iris.feature_names)
df_iris['label'] = pd.Series(iris.target)
 
print(df_iris.head())
 

sc = SparkSession.builder.appName('RDD').getOrCreate()
context = sc.sparkContext

data = sqlContext.createDataFrame(df_iris)
print(data.printSchema())

features = iris.feature_names

va = VectorAssembler(inputCols = features, outputCol='features')

va_df = va.transform(data)
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

sc.stop() # transformer
vector_assembler = VectorAssembler(inputCols=["sep_len", "sep_wid", "pet_len", "pet_wid"],outputCol="features")
df_temp = vector_assembler.transform(df)
df_temp.show(3)
# drop the original data features column
df = df_temp.drop('sep_len', 'sep_wid', 'pet_len', 'pet_wid')
df.show(3)
from pyspark.ml.feature import StringIndexer
# estimator
l_indexer = StringIndexer(inputCol="label", outputCol="labelIndex")
df = l_indexer.fit(df).transform(df)
df.show(3)
# data splitting
(training,testing) = df.randomSplit([0.7,0.3])

from pyspark.ml.classification import DecisionTreeClassifier
# train our model using training data
dt = DecisionTreeClassifier(labelCol="labelIndex", featuresCol="features")
model = dt.fit(training)
# test our model and make predictions using testing data
predictions = model.transform(testing)
predictions.select("prediction", "labelIndex").show(5)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction",metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))
print("Accuracy = %g " % accuracy)


sc.stop() 





