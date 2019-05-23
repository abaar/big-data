from time import sleep
from json import dumps
from kafka import KafkaProducer
import random
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

with open("sentiment-by-location.csv",encoding='utf-8') as file:
    datas = file.readlines()

    for data in datas:
        producer.send('bigdata',data)
        sleep(random.uniform(0.001,0.005))