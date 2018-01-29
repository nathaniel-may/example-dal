from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError, DuplicateKeyError, AutoReconnect
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
        formatter = logging.Formatter('%(asctime)s\t%(name)s\t\t%(levelname)s::%(message)s', datefmt='%m/%d/%Y %H:%M:{!s}')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        #set variables for connecting to mongo
        self.connString = connString
        self.logger.debug('completed __init__')

    def connect(self):
        #connect to mongodb
        self.logger.debug('started init()')
        if hasattr(self, 'client'):
            self.logger.debug('client is already connected')
        else:
            self.logger.info('setting up the connection')
            self.client = MongoClient(self.connString, 
                                      w='majority')
            db = self.client.pydal
            #test connection and error handle results. won't catch bad write concern option until a write is attempted.
            try:
                self.logger.debug('testing connection to mongodb')
                self.client.server_info()
                #create all collections
                self.dalExample = db.get_collection('example', write_concern=WriteConcern(w='majority', wtimeout=10000, j=True))
                self.logger.debug('completed init()')
            except ConnectionFailure as e:
                self.logger.fatal('Connection refused to {!s}.'.format(self.connString))
                del client
            except ServerSelectionTimeoutError as e:
                self.logger.fatal('Server selection timeout. Are these servers reachable? {!s}'.format(self.connString))
                del client
            except PyMongoError as e:
                self.logger.fatal('error connecting: ', e)
                raise WrappedError(e)
            except Exception as e:
                self.logger.fatal('error connecting: ', e)
                raise

    def close(self):
        if not hasattr(self, 'client'):
            try:
                self.client.close()
                self.logger.debug('client closed')
            except PyMongoError as e:
                self.logger.error('error closing client: ', e)
                raise WrappedError(e)
            except Exception:
                self.logger.error('error closing client: ', e)
                raise

    def insert_doc(self, doc):
        self.logger.debug('started insert_doc')
        if '_id' not in doc:
            self.logger.debug('doc does not have _id: ', doc)
            doc['_id'] = ObjectId()
        try:
            self.logger.debug('attempting insert')
            self.retry_on_error(
                self.dalExample.insert_one, doc
            )
            self.logger.debug('completed insert_doc')
            return doc['_id'];
        except DuplicateKeyError:
            self.logger.error('DuplicateKeyError while inserting doc. id: {id!s}'.format(id=doc['_id']))
            raise DuplicateIdError(doc['_id'])
        except PyMongoError as e:
            self.logger.error('error while inserting doc: ', e)
            raise WrappedError(e)
        except Exception as e:
            self.logger.error('error while inserting doc: {doc!s}, err: {e!s}'.format(doc=doc, e=e))
            raise

    def get_by_id(self, id):
        self.logger.debug('started get_by_id')
        try:
            doc = self.retry_on_error(
                self.dalExample.find_one, {'_id': id}
            )
            self.logger.debug('completed get_by_id')
            return doc
        except PyMongoError as e:
            self.logger.error('error while getting by id: {!s}'.format(e))
            raise WrappedError(e)
        except Exception as e:
            self.logger.error('error while getting by id: {!s}'.format(e))
            raise

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
            #newCount might be None if the operation was successful on the first try
            #but resulted in a network error. The retry will not match the document and will not return the new count
            #query the document to get an accurate count
            self.logger.debug('completed incCounter. Current value={count!s}'.format(count=newCount))
        except PyMongoError as e:
            self.logger.error('failed to increment the counter: ', e)
            raise WrappedError(e)
        except Exception as e: 
            self.logger.error('failed to increment the counter: ', e)


    def delete_all_docs(self):
        self.logger.debug('started delete_all_docs')
        try:  
            self.retry_on_error(
                self.dalExample.delete_many, {}
            )
            self.logger.debug('completed delete_all_docs')
        except PyMongoError as e:
            self.logger.error('error deleting_all_docs: ', e)
            raise WrappedError(e)
        except Exception as e:
            self.logger.error('error deleting_all_docs: ', e)
            raise

    def retry_on_error(self, fn, *args, **kwargs):
        try:
            val = fn(*args, **kwargs)
        except AutoReconnect as netErr: #NetworkError base class
            self.logger.debug('experienced network error- retrying')
            try:
                val = fn(*args)
            #the only way for a duplicate key error to happen here is if the first insert succeeded
            except DuplicateKeyError as e:
                self.logger.debug('retry resolved network error')
            except Exception:
                self.logger.error('could not resolve with retry')
                raise
        #catching all non-network errors to raise and log.
        except Exception as e: 
            self.logger.error('error is not retryable: {!s}'.format(e))
            raise
        else:
            return val

#Custom mongo_dal errors so higher functions don't need to catch pymongo-specific errors          
class DatabaseError(Exception):
    '''Base class for exceptions'''
    pass

class DuplicateIdError(DatabaseError):
    '''Exception raised when attempting to insert a document which contains
       an _id which is already present in the collection

    Attributes:
        id -- the id which caused the error
        message -- generated explanation of the error
    '''

    def __init__(self, id):
        self.id = id
        self.message = 'id {0} already present in collection'.format(self.id)

class WrappedError(DatabaseError):
    '''Exception wraps any unexpected pymongo errors before raising'''
    def __init__(self, err):
        self.message = 'pymongo error raised: {0}'.format(err)

