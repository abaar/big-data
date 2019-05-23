from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import ClusteringEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
@main.route("/model-cluster/<id>", methods = ["GET"])
def getPrediction(id):
    x = float(request.args.get('latitude'))
    y = float(request.args.get('longitude'))
    getPredictions = Clustering_engine.getPrediction(int(id),x,y)
    return json.dumps(getPredictions)
 
 
@main.route("/show-cluster/<int:id>", methods = ["GET"])
def showAll(id):
    showAlls = Clustering_engine.showAll(id)
    return json.dumps(showAlls)
  
def create_app(spark_context):
    global Clustering_engine 

    Clustering_engine = ClusteringEngine(spark_context)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app , Clustering_engine
