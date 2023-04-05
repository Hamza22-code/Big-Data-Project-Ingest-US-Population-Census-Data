#IMPORT LIBRARIES
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType, LongType, DoubleType, LongType, DoubleType, BooleanType 
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col,from_json,json_tuple
import pyspark.sql.functions as psf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import requests
from time import sleep
from confluent_kafka import Producer
from socket import gethostname
import pyspark
import json

#BELOW QUERY WILL READ THE FORMAT OF YOUR PROJECT API SCHEMA
df = spark.read.json("/user/root/project/project.json",multiLine=True)
df.schema

#BELOW QUERY WILL LOAD DATA FROM KAFKA TOPIC 
df = spark.readStream.format("kafka").option('kafka.bootstrap.servers', 'sandbox-hdp:6667').option('subscribe', 'Topic_USA') .option('startingOffsets', 'earliest').load()

#QUERY FOR CHECKING SCHEMA
df.schema
df.printSchema()

#BELOW QUERIES WILL SHOW THE SELECTED COLUMN FROM KAFKA TOPIC
ds = df.select(col('value').cast('string')).select(from_json('value', schema).alias('json_data')).select(col('json_data.ID Nation')).writeStream.format("console").trigger(processingTime = '30 second').start()

ds = df.select(col('value').cast('string')).select(from_json('value', schema).alias('json_data')).select(col('json_data.Nation')).writeStream.format("console").trigger(processingTime = '30 second').start()

ds = df.select(col('value').cast('string')).select(from_json('value', schema).alias('json_data')).select(col('json_data.ID Year')).writeStream.format("console").trigger(processingTime = '30 second').start()

##BELOW QUERY WILL SHOW ALL MULTIPLE SELECTED COLUMNS
ds = df.select(col('value').cast('string')).select(from_json('value', schema).alias('json_data')).select('json_data.ID Nation','json_data.ID Year','json_data.Nation','json_data.Year','json_data.Slug Nation','json_data.Population').writeStream.format("console").trigger(processingTime = '30 second').start()

#BELOW QUERY WILL STORE YOUR SELECTED COLUMNS FROM KAFKA TOPIC INTO HDFS USING ALIAS TO CHANGE COLUMN NAMES 
ds = df.select(col('value').cast('string')).select(from_json('value',schema).alias('json_data')).select(col('json_data.ID Nation').alias('ID_Nation'),col('json_data.ID Year').alias('ID_Year'),col('json_data.Nation'),col('json_data.Population')).writeStream.format("parquet").outputMode("append").option("checkpointLocation", "/user/root/C2").option("path", "/user/root/D2").start() #MAKE DIRECTORIES IN HDFS AND GIVE READ AND WRITE PERMISSION BEFORE EXECUTING THIS QUERY 

#THIS QUERY WILL LOAD YOUR DATA FROM HDFS INTO PYSPARK 
df2 = spark.read.parquet("/user/root/D2/part-00000-97340159-3bb6-43d5-a635-61f8b40128ff-c000.snappy.parquet")
#THIS QUERY WILL LOAD YOUR HISTORIC DATA FROM HDFS INTO PYSPARK
df3 = spark.read.load('/user/root/data/HD.csv', format='csv', sep='^', inferSchema='true', header='true')

#QUERY FOR FULL JOIN
df4 = df2.join(df3, df2.Nation == df3.Nation,'full')
df4.show()


#QUERY FOR LEFT JOIN
df4 = df2.join(df3, df2.ID_Year == df3.ID_Year1,'left')
df4.show()

#QUERY FOR INNER JOIN USING SELECTED COLUMNS
df5=df2.alias("USA").join(df3.alias("PK"),col("USA.ID_Year") == col("PK.ID_Year1"),"inner").select(col("USA.Nation"),col("USA.Population"),col("USA.ID_Year"), col("PK.Nation1"),col("PK.Population1"))

#QUERIES WILL SHOW THE POPULATION OF USA AND PK OF YEAR 2013 AND 2015
df5.where("ID_Year == 2013").show()

df5.where("ID_Year == 2019").show()


#QUERY FOR INNER JOIN USING SELECTED COLUMNS BUT WITH THE SELECTED YEAR
df5=df2.alias("USA").join(df3.alias("PK"),col("USA.ID_Year") == col("PK.ID_Year1"),"inner").select(col("USA.Nation"),col("USA.Population"), col("PK.Nation1"),col("PK.Population1")).where(col("USA.ID_Year") == 2015)

