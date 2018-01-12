from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.errors import DuplicateKeyError
from pymongo.errors import AutoReconnect

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
        self.logger.debug('completed __init__')

    def connect(self):
        #connect to mongodb
        self.logger.debug('started init()')
        if not hasattr(self, 'client'):
            self.logger.info('setting up the connection')
            self.client = MongoClient(self.connString, 
                serverSelectionTimeoutMS=10) #TODO seconds or milliseconds?
            db = self.client.pydal
            #test connection and error handle results
            try:
                self.logger.debug('testing connection to mongodb')
                self.client.server_info()
            except ConnectionFailure as e:
                self.logger.fatal('Connection refused to %s. with conn string ', self.connString)
                del client
            except ServerSelectionTimeoutError as e:
                self.logger.fatal('Server selection timeout to %s. with conn string ', self.connString)
                del client
            else:
                #create all collections
                self.dalExample = db.example
                self.logger.debug('completed init()')
        else:
            self.logger.debug('client is already connected')

    def close(self):
        if not hasattr(self, 'client'):
            self.client.close()

    def insert_doc(self, doc):
        self.logger.debug('started insert_doc')
        if '_id' not in doc:
            self.logger.debug('doc does not have _id: %s', doc)
            doc['_id'] = ObjectId()
        try:
            self.logger.debug('attempting insert')
            self.retry_on_error(
                self.dalExample.insert_one, doc
            )
        #TODO remote blanket catch
        except Exception as e:
            self.logger.error(e)
            self.logger.error('error inserting doc: %s', doc)
            raise #TODO raise new dal-type errors or raise pymongo error?
        else:
            self.logger.debug('completed insertDoc')
            return doc['_id'];

    def get_by_id(self, id):
        self.logger.debug('started get_by_id')
        try:
            doc = self.retry_on_error(
                self.dalExample.find_one, {'_id': id}
            )
        except Exception as e: #TODO make exact errors?
            self.logger.error('error while getting by id: %s', e)
        else:
            self.logger.debug('completed get_by_id')
            return doc #TODO does doc always exist here?


    def delete_all_docs(self):
        self.logger.debug('started delete_all_docs')
        try:  
            self.retry_on_error(
                self.dalExample.delete_many, {}
            )
        except: #TODO make exact errors?
            self.logger.error('error while getting by id')
        else:
            self.logger.debug('completed delete_all_docs')

    def retry_on_error(self, fn, *args):
        try:
            val = fn(*args) #TODO put return statement here?
        except AutoReconnect as e1: #NetworkError
            self.logger.debug('experienced network error- retrying')
            try:
                val = fn(*args) #TODO put return statement here?
            except DuplicateKeyError as e2:
                self.logger.debug('retry resolved network error')
            except: #TODO make exact errors?
                self.logger.error('could not resolve with retry')
                raise
        #catching all exceptions to log. Raises them to be handled appropriately.
        except Exception as e: 
            self.logger.error('error is not retryable: %s', e)
            raise
        else:
            return val
