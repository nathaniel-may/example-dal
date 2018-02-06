from pymongo import MongoClient, ReturnDocument
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError, DuplicateKeyError, AutoReconnect
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from bson.objectid import ObjectId
import logging.config

''' An example class for accessing mongodb

    All operations are written so they can be safely retried 
    in the event of network errors and primary failovers
'''


class MongoDal:

    def __init__(self, conn_string, log_level, logger_name='DAL'):

        # logging
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)
        handler = logging.StreamHandler()
        handler.setLevel(log_level)
        formatter = logging.Formatter(
            '%(asctime)s\t%(name)s\t\t%(levelname)s::%(message)s',
            datefmt='%m/%d/%Y %H:%M:%S')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # set variables for connecting to mongo
        self.conn_string = conn_string
        # setting flag for connection assert in later functions
        # self.connect must be called after init for error handling
        self.__connected = False

        # creating the MongoClient and and collection definitions here
        # because they don't reach out to the database until called
        # and there should only ever be one instance of MongoClient
        # Connect must be called after creating this to error handle
        # initial connection issues
        self.client = MongoClient(self.conn_string, w='majority')

        # write concern is redundant here because it is  set on the 
        # client connection above. colleciton defined write concern
        # overrides client definitions. 
        write_concern = WriteConcern(w='majority', wtimeout=10000, j=True)
        self.dalExample = self.client.pydal.get_collection(
            'example',
            write_concern=write_concern)
        self.dalExampleReadMaj = self.client.pydal.get_collection(
            'example',
            write_concern=write_concern,
            read_concern=ReadConcern(level='majority'))

        self.logger.debug('completed __init__')

    def connect(self):
        """ Connects to the database. Raises errors caused by reads with an
            incorrectly configured connection string. Errors relating to write options
            such as write concern are only raised when a write is attempted
            
            Must be called once before calling any other function
            
            Calling twice raises an error
        """

        # connect to mongodb
        self.logger.debug('started connect')

        # if this has already been called raise an error
        if self.__connected:
            raise DbAlreadyConnectedError()

        # test connection and error handle results. won't catch bad write
        # concern option until a write is attempted.
        try:
            self.logger.debug('testing connection to mongodb')

            # calling server_info() will catch a bad connection string and
            # an unavailable cluster
            self.client.server_info()
            
            # setting flag for connection assert in later functions
            self.__connected = True
            self.logger.debug('completed connect')
        except ConnectionFailure as e:
            self.logger.fatal(
                'Connection refused to {!s}.'.format(
                    self.conn_string))
            raise DbConnectionRefusedError(e)
        except ServerSelectionTimeoutError as e:
            self.logger.fatal(
                'Server selection timeout. Are these servers reachable? {!s}'.format(
                    self.conn_string))
            raise DatabaseConnectionError(e)
        except PyMongoError as e:
            self.logger.fatal('error connecting: {!s}'.format(e))
            raise DbWrappedError(e)
        except Exception as e:
            self.logger.fatal('error connecting: {!s}'.format(e))
            raise

    def __assert_connected(self):
        """ Raises DbNotConnectedError if connect has not been called """

        if not self.__connected:
            raise DbNotConnectedError()

    def close(self):
        """ Closes the connection to MongoDB.

            Connection can be reinitialized by calling self.connect.
        """

        self.__assert_connected()
        try:
            self.client.close()
            self.__connected = False
            self.logger.debug('client closed')
        except PyMongoError as e:
            self.logger.error('error closing client: {!s}'.format(e))
            raise DbWrappedError(e)
        except Exception:
            self.logger.error('error closing client: {!s}'.format(e))
            raise

    def insert_doc(self, doc):
        """ function for inserting a document.

            Assigns an id to the document if it does not already exist to make it safe to retry
        """

        self.__assert_connected()
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
        """ Gets a document by id """

        self.__assert_connected()
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
        """ Increments the counter field of the document.

            Appends ObjectIds to the document in the opids array to achieve idempotency.
        """

        self.__assert_connected()
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
        """ Deletes all documents in the collection. Does not drop the collection """

        self.__assert_connected()
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

    def __retry_on_error(self, fn, *args, **kwargs):
        """ This function exists to compensate for when network errors and primary
            fail overs happen during our operation. A network error can occur on
            the way to the database so that our operation never arrived, or on the
            way back so that we do not know the operation took place. Making all
            operations safe to retry and retrying them exactly once in the face of
            these network errors prevents raising unnecessary errors to the user.
            
            In v3.6 of the driver this functionality is built in for operations on
            single documents with retryable writes.
        """

        try:
            return fn(*args, **kwargs)
        except ConnectionFailure:
            self.logger.debug('experienced network error- retrying')
            try:
                return fn(*args, **kwargs)
            # the only way for a duplicate key error to happen here is if the
            # first insert succeeded
            except DuplicateKeyError:
                self.logger.debug('retry resolved network error')
            except Exception:
                self.logger.error('could not resolve with retry')
                raise
        # catching all non-network errors to raise and log.
        except Exception as e:
            self.logger.error('error is not retryable: {!s}'.format(e))
            raise


class DatabaseError(Exception):
    """ Base class for custom exceptions
        this prevents higher functions from needing to catch pymongo-specific errors
    """

    pass


class DbNotConnectedError(DatabaseError):
    """ Exception raised when a database action has been called but
        self.connect has not been called which has important error handling

    Attributes:
        message -- generated explanation of the error
    """

    def __init__(self):
        self.message = 'must call connect after init'


class DbAlreadyConnectedError(DatabaseError):
    """ Exception raised when self.connect has been called twice

    Attributes:
        message -- generated explanation of the error
    """

    def __init__(self):
        self.message = 'must call connect only once'


class DbConnectionRefusedError(DatabaseError):
    """ Exception raise when connection is refused to mongodb

    Attributes:
        message -- generated explanation of the error
    """

    def __init__(self):
        self.message = 'connection refused to '.format(self.conn_string)


class DbDuplicateIdError(DatabaseError):
    """ Exception raised when attempting to insert a document which contains
       an _id which is already present in the collection

    Attributes:
        id -- the id which caused the error
        message -- generated explanation of the error
    """

    def __init__(self, id):
        self.id = id
        self.message = 'id {!s} already present in collection'.format(self.id)


class DbWrappedError(DatabaseError):
    """ Exception wraps any unexpected pymongo errors before raising

    Attributes:
        err -- the original error being wrapped
        message -- generated explanation of the error
    """

    def __init__(self, err):
        self.message = 'pymongo error raised: {!s}'.format(err)
