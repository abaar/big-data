import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__=="__main__":
    sc = SparkContext(appName="Stream text")
    ssc = StreamingContext(sc,2)

    brokers = "localhost:9092"
    topics = ['test']

    kafkaS = KafkaUtils.createDirectStream(ssc,topics,{"metadata.broker.list":brokers})

    lines = kafkaS.map(lambda x:x[1])

    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a, b:a+b)


    counts.pprint()

    ssc.start()

    ssc.awatTermination()