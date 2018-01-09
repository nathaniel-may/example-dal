from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import logging.config

class MongoDal:
    '''example functions for accessing mongodb'''

    def __init__(self, connString, logLevel):

        #logging
        self.logger = logging.getLogger('DAL')
        self.logger.setLevel(logLevel)
        handler = logging.StreamHandler()
        handler.setLevel(logLevel)
        formatter = logging.Formatter('%(asctime)s\t%(name)s\t\t%(levelname)s::%(message)s', datefmt='%m/%d/%Y %H:%M:%S')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        #set variables for connecting to mongo
        self.connString = connString
        self.client = None
        self.logger.debug('completed __init__')

    def init(self):
        self.logger.debug('started init()')
        if(self.client is None):
            self.client = MongoClient(self.connString)
        db = self.client.dalExample
        #create all collections
        self.dalExample = db.example
        self.logger.debug('completed init()')

    def insertDoc(self, doc):
        self.logger.debug('started insertDoc()')
        id = ObjectId()
        doc.update({'_id': id})
        #TODO RETRY and ERROR HANDLE
        self.dalExample.insert_one(doc)
        self.logger.debug('completed insertDoc()')
        return id;

