# Databricks notebook source
print("Hello")


# COMMAND ----------

# dictionary 
# funcation 
# lambda duncation 
# tuples, list of tuples 

# COMMAND ----------

from pyspark import SparkContext, SparkConf

# COMMAND ----------


'''
conf=SparkConf().setMaster("*").SetAppName()
# sc: obj of the spark content class
# sc => acess (spark cluster , ram, application, rdd)
sc = SparkContext(conf=conf)
'''

# COMMAND ----------

# RDD creation with 3 method 
# paralized 
# using external dataset
#using exitsiting dataset



# COMMAND ----------

mylist= [10,3,43,20,29,23,23,43,44,3]
print(mylist)

# COMMAND ----------

#rdd are lazy in nature
rdd1 =sc.parallelize([10,20,29])

# COMMAND ----------

rdd1.collect()

# COMMAND ----------

rdd1.take(4)

# COMMAND ----------

rdd1.map(lambda x:x+5).collect() 


# COMMAND ----------

#[11,13,15,12,10,6,8]
#filter => all the even number 
# print the squre of each even number 

# COMMAND ----------

rdd2 =sc.parallelize([11,13,15,13,12,10,6,8])

rdd2.filter(lambda x: x%2 == 0).map(lambda x: x*x).collect()
