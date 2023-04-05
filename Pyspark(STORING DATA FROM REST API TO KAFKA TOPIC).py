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

conf = {
       'bootstrap.servers': "sandbox-hdp:6667",
        'client.id': gethostname()
       }

schema=StructType([StructField('ID Nation',StringType(),True),StructField('ID Year',LongType(),True),StructField('Nation',StringType(),True),StructField('Population',LongType(),True),StructField('Slug Nation',StringType(),True),StructField('Year',StringType(),True)])

kafka_topic = 'Topic_USA'
USA = []
producer = Producer(conf)

response = requests.get("https://datausa.io/api/data?drilldowns=Nation&measures=Population")
json_data = json.loads(response.text)
for item in json_data['data']:
  producer.produce(kafka_topic, key="data", value=json.dumps(item))
  USA.append(response)

for item in USA:
 print( item.text )