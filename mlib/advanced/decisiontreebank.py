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
df = spark.read.csv("C://files//bank.csv", 
                    inferSchema=True, header=True,sep=';').toDF(
        "age","job","marital","education","default","balance","housing","loan","contact","day","month","duration","campaign","pdays","previous","poutcome", "y")
df.show(5)


string_columns = [column[0] for column in df.dtypes if column[1].startswith('string')]

 
from pyspark.ml.feature import StringIndexer
inputs=["job","contact","loan","marital","education","default","housing","month","poutcome","y"]
outputs=["jobi","contacti","loani","maritali","educationi","defaulti","housingi","monthi","poutcomei","labelIndex"]

indexer = StringIndexer(inputCols=inputs, outputCols=outputs)
indexed = indexer.fit(df).transform(df)
indexed.show() 
indexed=indexed.drop("job","loan","contact","marital","education","default","housing","month","poutcome","y")
indexed.columns
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
# transformer
vector_assembler = VectorAssembler(inputCols=[
    'age',
 'balance',
 'loani',
 'contacti',
 'day',
 'duration',
 'campaign',
 'pdays',
 'previous',
 'housingi',
 'jobi',
 'maritali',
 'educationi',
 'defaulti',
 'monthi',
 'loani',
 'poutcomei'
],outputCol="features")
df_temp = vector_assembler.transform(indexed)
df_temp.show(3)


df.show(3)
# data splitting
(training,testing) = df_temp.randomSplit([0.7,0.3])

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





