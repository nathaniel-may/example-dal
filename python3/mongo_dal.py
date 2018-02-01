from pymongo import MongoClient, ReturnDocument
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError, DuplicateKeyError, AutoReconnect
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from bson.objectid import ObjectId
from datetime import datetime
import logging.config

''' example functions for accessing mongodb
    all operations are idempotent so they can be safely retried in the face of network errors
'''


class MongoDal:

    def __init__(self, connString, logLevel):

        # logging
        self.logger = logging.getLogger('DAL')
        self.logger.setLevel(logLevel)
        handler = logging.StreamHandler()
        handler.setLevel(logLevel)
        formatter = logging.Formatter(
            '%(asctime)s\t%(name)s\t\t%(levelname)s::%(message)s',
            datefmt='%m/%d/%Y %H:%M:%S')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # set variables for connecting to mongo
        self.connString = connString
        self.logger.debug('completed __init__')

    def connect(self):
        # connect to mongodb
        self.logger.debug('started init()')
        if hasattr(self, 'client'):
            self.logger.debug('client is already connected')
        else:
            self.logger.info('setting up the connection')
            '''here write concern is set on the whole client 
               this can also be set on the following levels:
               - connection string
               - client connection
               - database
               - collection
               - operation
            '''
            client = MongoClient(self.connString,
                                 w='majority')
            db = client.pydal
            # test connection and error handle results. won't catch bad write
            # concern option until a write is attempted.
            try:
                self.logger.debug('testing connection to mongodb')
                # calling server_info() will catch a bad connection string before
                # waiting for the first read operation to be called
                client.server_info()
                # create all collections
                # write concern here is redundant since it is also set on the
                # client
                self.dalExample = db.get_collection(
                    'example', write_concern=WriteConcern(
                        w='majority', wtimeout=10000, j=True))
                self.dalExampleReadMaj = db.get_collection(
                    'example', write_concern=WriteConcern(
                        w='majority', wtimeout=10000, j=True), read_concern=ReadConcern(
                        level='majority'))
                # client is connected. assign to complete singleton
                self.client = client
                self.logger.debug('completed init()')
            except ConnectionFailure as e:
                self.logger.fatal(
                    'Connection refused to {!s}.'.format(
                        self.connString))
                raise DbConnectionRefusedError(e)
            except ServerSelectionTimeoutError as e:
                self.logger.fatal(
                    'Server selection timeout. Are these servers reachable? {!s}'.format(
                        self.connString))
                raise DatabaseConnectionError(e)
            except PyMongoError as e:
                self.logger.fatal('error connecting: {!s}'.format(e))
                raise DbWrappedError(e)
            except Exception as e:
                self.logger.fatal('error connecting: {!s}'.format(e))
                raise

    def close(self):
        if not hasattr(self, 'client'):
            try:
                self.client.close()
                self.logger.debug('client closed')
            except PyMongoError as e:
                self.logger.error('error closing client: {!s}'.format(e))
                raise DbWrappedError(e)
            except Exception:
                self.logger.error('error closing client: {!s}'.format(e))
                raise

    def insert_doc(self, doc):
        self.logger.debug('started insert_doc')
        # assign an _id app-side so the operation can be safely retried
        if '_id' not in doc:
            self.logger.debug('doc does not have _id: {!s}'.format(doc))
            # don't mutate their object
            doc = {**doc, '_id': ObjectId()}

        try:
            self.logger.debug('attempting insert')
            self.__retry_on_error(
                self.dalExample.insert_one, doc
            )
            self.logger.debug('completed insert_doc')
            return doc['_id']
        except DuplicateKeyError:
            self.logger.error(
                'DuplicateKeyError while inserting doc. id: {id!s}'.format(
                    id=doc['_id']))
            raise DbDuplicateIdError(doc['_id'])
        except PyMongoError as e:
            self.logger.error('error while inserting doc: {!s}'.format(e))
            raise DbWrappedError(e)
        except Exception as e:
            self.logger.error(
                'error while inserting doc: {doc!s}, err: {e!s}'.format(
                    doc=doc, e=e))
            raise

    def get_by_id(self, id):
        self.logger.debug('started get_by_id')
        try:
            doc = self.__retry_on_error(
                # uses the collection with read concern majority
                self.dalExampleReadMaj.find_one, {'_id': id}
            )
            self.logger.debug('completed get_by_id')
            return doc
        except PyMongoError as e:
            self.logger.error('error while getting by id: {!s}'.format(e))
            raise DbWrappedError(e)
        except Exception as e:
            self.logger.error('error while getting by id: {!s}'.format(e))
            raise

    # this uses the algorithm outlined here to achieve idempotency:
    # https://explore.mongodb.com/developer/nathaniel-may
    def inc_counter(self, id):
        self.logger.debug('started incCounter')
        # create a unique id to represent this particular increment operation
        opid = ObjectId()
        try:
            new_count = self.__retry_on_error(
                self.dalExample.find_one_and_update,
                # query by id and that this operation hasn't been completed
                # already
                {'_id': id, 'opids': {'$ne': opid}},
                # increment the counter and add the opid for this operation into the opids array to note that
                # it has been completed.
                # slice the oldest elements out of the array if it is too large. should be no smaller than
                # the expected number of concurrent updates on this document
                {'$inc': {'counter': 1}, '$push': {'opids': {'$each': [opid], '$slice': -10}}},
                # don't bring back the whole document which includes the list of opids
                # only return the new counter value for logging purposes
                projection={'counter': True, '_id': False},
                return_document=ReturnDocument.AFTER
            )
            # new_count might be None if the operation was successful on the first try
            # but resulted in a network error. The retry will not match the document and will not return the new count
            # query the document to get an accurate count
            self.logger.debug(
                'completed incCounter. Current count is {count!s}'.format(
                    count=new_count['counter']))
        except PyMongoError as e:
            self.logger.error(
                'failed to increment the counter: {!s}'.format(e))
            raise DbWrappedError(e)
        except Exception as e:
            self.logger.error(
                'failed to increment the counter: {!s}'.format(e))
            raise

    def delete_all_docs(self):
        self.logger.debug('started delete_all_docs')
        try:
            self.__retry_on_error(
                self.dalExample.delete_many, {}
            )
            self.logger.debug('completed delete_all_docs')
        except PyMongoError as e:
            self.logger.error('error deleting_all_docs: {!s}'.format(e))
            raise DbWrappedError(e)
        except Exception as e:
            self.logger.error('error deleting_all_docs: {!s}'.format(e))
            raise

    '''This function exists to compensate for when network errors and primary
       fail overs happen during our operation. A network error can occur on
       the way to the database so that our operation never arrived, or on the
       way back so that we do not know the operation took place. Making all
       operations safe to retry and retrying them exactly once in the face of
       these network errors prevents raising unnecessary errors to the user.

       In v3.6 of the driver this functionality is built in for operations on
       single documents with retryable writes.
    '''
    def __retry_on_error(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except AutoReconnect as netErr:  # NetworkError base class
            self.logger.debug('experienced network error- retrying')
            try:
                return fn(*args, **kwargs)
            # the only way for a duplicate key error to happen here is if the
            # first insert succeeded
            except DuplicateKeyError as e:
                self.logger.debug('retry resolved network error')
            except Exception:
                self.logger.error('could not resolve with retry')
                raise
        # catching all non-network errors to raise and log.
        except Exception as e:
            self.logger.error('error is not retryable: {!s}'.format(e))
            raise


'''Base class for custom exceptions
   this prevents higher functions from needing to catch pymongo-specific errors
'''


class DatabaseError(Exception):
    pass


class DbConnectionRefusedError(DatabaseError):
    def __init__(self):
        self.message = 'connection refused to '.format(self.connString)


'''Exception raised when attempting to insert a document which contains
   an _id which is already present in the collection

Attributes:
    id -- the id which caused the error
    message -- generated explanation of the error
'''


class DbDuplicateIdError(DatabaseError):

    def __init__(self, id):
        self.id = id
        self.message = 'id {!s} already present in collection'.format(self.id)


'''Exception wraps any unexpected pymongo errors before raising

Attributes:
    err -- the original error being wrapped
    message -- generated explanation of the error
'''


class DbWrappedError(DatabaseError):

    def __init__(self, err):
        self.message = 'pymongo error raised: {!s}'.format(err)
