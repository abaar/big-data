import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import threading

def start_listening(app,wport):
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': wport,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()

class FlaskServer:
    def addData(self,datatrain):
        self.engine.addData(datatrain)

    def startServer(self):
        app, self.engine = create_app(self.sc)
        self.t = threading.Thread(target=start_listening, args=(app,self.port))
        self.t.start()
    
    def __init__(self,port,parent_sc):
        self.sc = parent_sc
        self.port = port
        self.engine = None
        self.t = None

