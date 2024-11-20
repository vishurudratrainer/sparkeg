# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 19:36:18 2024

@author: VISHWA
"""



from pyspark.sql.types import IntegerType, DateType, StringType, StructType
	
customSchema = StructType() \
		 .add("PID", IntegerType(), True) \
		 .add("Name", StringType(), True) \
		 .add("DID", IntegerType(), True) \
		 .add("DName", StringType(), True) \
		 .add("VisitDate", DateType(), True)
	
	#read the CSV file with headers and apply the schema
dfPatients =  spark \
		 .readStream \
		 .format("csv") \
		 .option("header",True) \
		 .option("path","C:/sparkeg/sparkeg-main/sparkeg-main/streaming/dept*.csv") \
		 .schema(customSchema) \
		 .load()
	
	
	#Apply filters to get only patients from the ortho department
orthoPatients = dfPatients.select("PID","Name").where("DID =86")
	
	
	#start the streaming of output data
orthoPatients \
		 .writeStream \
		 .format("console") \
		 .start()
         
from pyspark.sql.functions import col,expr,column
  
         
dfPatients.select("name", col("name"), column("name"), expr("PID +3"))
