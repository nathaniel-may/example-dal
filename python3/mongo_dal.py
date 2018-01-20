from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.errors import DuplicateKeyError
from pymongo.errors import AutoReconnect
from pymongo import ReturnDocument
from pymongo.write_concern import WriteConcern

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
                                      w='majority')
            db = self.client.pydal
            #test connection and error handle results. won't catch bad write concern option until a write is attempted.
            try:
                self.logger.debug('testing connection to mongodb')
                self.client.server_info()
            except ConnectionFailure as e:
                self.logger.fatal('Connection refused to %s. with conn string ', self.connString)
                del client
            except ServerSelectionTimeoutError as e:
                self.logger.fatal('Server selection timeout. Are these servers available: %s', self.connString)
                del client
            else:
                #create all collections
                self.dalExample = db.get_collection('example', write_concern=WriteConcern(w='majority', wtimeout=10000, j=True))
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
        #TODO remove blanket catch
        except DuplicateKeyError:
            raise DuplicateIdError(doc['_id'])
        except Exception as e:
            self.logger.error('error while inserting doc: %s, err: %s', doc, e)
            raise #TODO raise new dal-type errors or raise pymongo error?
        else:
            self.logger.debug('completed insert_doc')
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
            return doc

    def inc_counter(self, id):
        self.logger.debug('started incCounter')
        #create a unique id to represent this particular increment operation
        opid = ObjectId()
        try:
            newCount = self.retry_on_error(
                self.dalExample.find_one_and_update,
                #query by id and that this operation hasn't been completed already
                {'_id': id, 'opids': {'$ne': opid}},
                #increment the counter and add the opid for this operation into the opids array
                #slice the oldest elements out of the array if it is too large
                {'$inc': {'counter': 1}, '$push': {'opids': {'$each': [opid], '$slice': -10}}},
                #don't bring back the whole document which includes the list of opids
                #only return the new counter value for logging purposes
                projection={'counter': True, '_id':False},
                return_document=ReturnDocument.AFTER
            )
        except Exception as e: 
            self.logger.error('failed to increment the counter: ', e)
        else:
            #newCount might be None if the operation was successful on the first try
            #but resulted in a network error. The retry will not match the document and will not return the new count
            #query the document to get an accurate count
            self.logger.debug('completed incCounter. Current value=%s',newCount)
            return


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

    def retry_on_error(self, fn, *args, **kwargs):
        try:
            val = fn(*args, **kwargs) #TODO put return statement here?
        except AutoReconnect as e1: #NetworkError
            self.logger.debug('experienced network error- retrying')
            try:
                val = fn(*args) #TODO put return statement here?
            except DuplicateKeyError as e2:
                self.logger.debug('retry resolved network error')
            except Exception: #TODO make exact errors?
                self.logger.error('could not resolve with retry')
                raise
        #catching all exceptions to log. Raises them to be handled appropriately.
        except Exception as e: 
            self.logger.error('error is not retryable: %s', e)
            raise
        else:
            return val

class Error(Exception):
    '''Base class for exceptions'''
    pass

class DuplicateIdError(Error):
    '''Exception raised when attempting to insert a document which contains
       an _id which is already present in the collection

    Attributes:
        id -- the id which caused the error
        message -- generated explanation of the error
    '''

    def __init__(self, id):
        self.id = id
        self.message = 'id {0} already present in collection'.format(self.id)
