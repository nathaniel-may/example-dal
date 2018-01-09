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
        #connect to mongodb
        self.logger.debug('started init()')
        if(self.client is None):
            self.client = MongoClient(self.connString)
        db = self.client.pydal

        #create all collections
        self.dalExample = db.example
        self.logger.debug('completed init()')

    def close(self):
        self.client.close()

    def insert_doc(self, doc):
        self.logger.debug('started insert_doc()')
        id = ObjectId()
        doc.update({'_id': id})
        #TODO RETRY and ERROR HANDLE
        self.dalExample.insert_one(doc)
        self.logger.debug('completed insertDoc()')
        return id;

    def get_by_id(self, id):
        self.logger.debug('started get_by_id()')
        #TODO retry and error handle
        doc = self.dalExample.find_one({'_id': id})
        self.logger.debug('completed get_by_id()')
        return doc


    def delete_all_docs(self):
        self.logger.debug('started delete_all_docs()')
        #TODO error handle
        self.dalExample.delete_many({})
        self.logger.debug('completed delete_all_docs()')


