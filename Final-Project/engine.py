import os
from pyspark.mllib.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml.feature import IndexToString, StringIndexer
from pyspark.sql import *

import pandas


from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClusteringEngine:
    def trainModel(self, id):
        data = self.model[id][0]
        kmeans = KMeans().setK(10).setSeed(1)
        model = kmeans.fit(data)

        self.model[id]=(self.model[id][0],model,self.model[id][2])

    def getPrediction(self, id,x,y,z):
        if(self.model[id][1] == None):
            self.trainModel(id)
        predict_this = self.sc.createDataFrame([(x,y,z)],['Latitude','Longitude','Sentiment'])
        predict_this = self.assembler.transform(predict_this)
        predict_result = self.model[id][1].transform(predict_this).select("features", "prediction").toJSON().collect()

        return predict_result

    def showAll(self, id):
        if(self.model[id][1] == None):
            self.trainModel(id)
        
        predict_this = self.model[id][0]

        predict_result = self.model[id][1].transform(predict_this).select("features", "prediction").toJSON().collect()

        return predict_result
    
    def getModelsInfo(self):
        lens = len(self.model)
        x = {}
        x['total-model']=lens
        for i in range(0,lens):
            x['panjang-data-ke-'+str(i)]=self.model[i][0].count()
        return x


    def addData(self,data):
        
        data = self.assembler.transform(data)
        if(self.latest_model==None):
            self.latest_model=data
            self.model.append((data,None,None))
        else:
            self.latest_model = self.latest_model.union(data)
            self.model.append((self.latest_model,None,None))


    def __init__(self, sc):
        self.sc = sc
        self.assembler = VectorAssembler(inputCols=["Latitude","Longitude",'Sentiment'],outputCol='features')
        self.model = []
        self.latest_model = None
