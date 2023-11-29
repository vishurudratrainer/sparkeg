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
df = spark.read.csv("C://files//audit_risk.csv", inferSchema=True, header=True).toDF()

df=df.na.drop()

df.show(5)
from pyspark.ml.feature import StringIndexer
# estimator
l_indexer = StringIndexer(inputCol="LOCATION_ID", outputCol="LOCATION_ID_INDEX")
df = l_indexer.fit(df).transform(df)
df=df.drop('LOCATION_ID')
df.printSchema()

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
# transformer
vector_assembler = VectorAssembler(inputCols=['Sector_score',
'LOCATION_ID_INDEX',
'PARA_A',
'Score_A',
'Risk_A',
'PARA_B',
'Score_B',
'Risk_B',
'TOTAL',
'numbers',
'ScoreB',
'Risk_C',
'Money_Value',
'Score_MV',
'Risk_D',
'District_Loss',
'PROB1',
'RiSk_E',
'History',
'Prob',
'Risk_F',
'Score',
'Inherent_Risk',
'CONTROL_RISK',
'Detection_Risk',
'Audit_Risk'
],outputCol="features")
df_temp = vector_assembler.transform(df)
df_temp.show(3)
# drop the original data features column
df = df_temp.drop('Sector_score',
'LOCATION_ID_INDEX',
'PARA_A',
'Score_A',
'Risk_A',
'PARA_B',
'ScoreB',
'Risk_B',
'TOTAL',
'numbers',
'Score_B',
'Risk_C',
'Money_Value',
'Score_MV',
'Risk_D',
'District_Loss',
'PROB1',
'RiSk_E',
'History',
'Prob',
'Risk_F',
'Score',
'Inherent_Risk',
'CONTROL_RISK',
'Detection_Risk',
'Audit_Risk'
)

df.show(3)
df=df.na.drop()
# data splitting
(training,testing) = df.randomSplit([0.7,0.3])

from pyspark.ml.classification import DecisionTreeClassifier
# train our model using training data
dt = DecisionTreeClassifier(labelCol="Label", featuresCol="features")
model = dt.fit(training)
# test our model and make predictions using testing data
predictions = model.transform(testing)
predictions.select("prediction", "Label").show(5)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))
print("Accuracy = %g " % accuracy)


sc.stop() 





