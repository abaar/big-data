import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.3 pyspark-shell'
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import server
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import threading

sc = SparkContext(appName="Stream text")
spsc = SparkSession(sc)
sqlc = SQLContext(spsc)
ssc = StreamingContext(sc,60)

brokers = "localhost:9092"
topics = ['bigdata']

kafkaS = KafkaUtils.createDirectStream(ssc,topics,{"metadata.broker.list":brokers})

endpoint_server = server.FlaskServer(51629,sqlc)
endpoint_server.startServer()

def addToEndpoint(rdd):
    print('a new data has come')
    if not rdd.isEmpty():
        print("converting to dataframe")
        df = rdd.map(lambda line: line[1].split(",")).map(lambda tokens: (float(tokens[2]),float(tokens[3]),1 if(tokens[4]=='Positive') else tokens[4] )).map(lambda s: (s[0],s[1], -1 if(s[2]=='Negative') else 0 )).toDF(['Latitude','Longitude','Sentiment'])
        print("adding new data to endpoint")
        endpoint_server.addData(df)
        print("data added")
    return rdd

kafkaS.foreachRDD(lambda x : addToEndpoint(x))

ssc.start()

ssc.awaitTermination()