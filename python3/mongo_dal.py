from pymongo import MongoClient, ReturnDocument
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, DuplicateKeyError, AutoReconnect
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
            client = MongoClient(self.connString, 
                                      w='majority')
            db = client.pydal
            #test connection and error handle results. won't catch bad write concern option until a write is attempted.
            try:
                self.logger.debug('testing connection to mongodb')
                client.server_info()
            except ConnectionFailure as e:
                self.logger.fatal('Connection refused to %s. with conn string ', self.connString)
            except ServerSelectionTimeoutError as e:
                self.logger.fatal('Server selection timeout. Are these servers available: %s', self.connString)
            else:
                #connection succeeded
                self.client = client
                #create all collections
                self.dalExample = db.get_collection('example', write_concern=WriteConcern(w='majority', wtimeout=10000, j=True))
                self.logger.debug('completed init()')
        else:
            self.logger.debug('client is already connected')

    def close(self):
        if hasattr(self, 'client'):
            self.client.close()

    def insert_doc(self, doc):
        self.logger.debug('started insert_doc')
        if '_id' not in doc:
            self.logger.debug('doc does not have _id: %s', doc)
            #don't mutate their object
            doc = {**doc, '_id': ObjectId()}

        try:
            self.logger.debug('attempting insert')
            self.retry_on_error(
                self.dalExample.insert_one, doc
            )
        except DuplicateKeyError:
            raise DuplicateIdError(doc['_id'])
        #TODO
        #catch all mongo exceptions and return internal DAL error
        #raise all other errors that aren't mongo specific
        except Exception as e:
            #log any other errors
            self.logger.error('error while inserting doc: %s, err: %s', doc, e)
            raise
        else:
            self.logger.debug('completed insert_doc')
            return doc['_id'];

    def get_by_id(self, id):
        self.logger.debug('started get_by_id')
        try:
            doc = self.retry_on_error(
                self.dalExample.find_one, {'_id': id}
            )
        #TODO ^^^
        #log unexpected errors
        except Exception as e:
            self.logger.error('error while getting by id: %s', e)
            raise
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
        #TODO ^^^
        except Exception as e: 
            self.logger.error('failed to increment the counter: ', e)
            raise
        else:
            #newCount might be None if the operation was successful on the first try
            #but resulted in a network error. The retry will not match the document and will not return the new count
            #query the document to get an accurate count
            self.logger.debug('completed incCounter. Current value=%s',newCount)


    def delete_all_docs(self):
        self.logger.debug('started delete_all_docs')
        try:  
            self.retry_on_error(
                self.dalExample.delete_many, {}
            )
        #TODO ^^^
        #log unexpected errors
        except:
            self.logger.error('error while getting by id')
            raise
        else:
            self.logger.debug('completed delete_all_docs')

    def retry_on_error(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except AutoReconnect as e1: #NetworkError base class
            self.logger.debug('experienced network error- retrying')
            try:
                return fn(*args, **kwargs)
            except DuplicateKeyError as e2:
                self.logger.debug('retry resolved network error')
            #log unexpected errors
            except Exception:
                self.logger.error('could not resolve with retry')
                raise
        #catching all exceptions to log. Raises them to be handled appropriately.
        except Exception as e: 
            self.logger.error('error is not retryable: %s', e)
            raise

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
