//fully idempotent example database access layer for MongoDB 3.4

//load dependencies
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectId;
var ReadConcern = require('mongodb').ReadConcern;
var winston = require('winston');

class MongoDal{

  constructor(connString, logLevel){
    this.logModule = 'DAL';
    this.connString = connString;
    this._database = null;

    //std out logging
    this.logger = new (winston.Logger)({
      transports: [
        new (winston.transports.Console)({colorize: true})
      ]
    });
    this.logger.level = logLevel;

    this.logger.silly(new Date() + ' ' + this.logModule + ' MongoDal constructor completed');
  }

  init(){
    return new Promise((resolve, reject) => {
      this.logger.silly(new Date() + ' ' + this.logModule + ' init function called');
      //attempt to connect to the database only once
      this._connect('dal').then((db) => {
        //define database object so that reconnecting is not required
        this._database = db;

        //define all necessary collections
        //every document read from this collection will be present on at least a majority of servers
        //but may not be the latest version of the document
        this.dalExample = db.collection('example', {readConcern: {level: 'majority'}});
        //single documents queried from this collection will be present on at least a majority of servers
        //and be the latest version of that document. this cannot be used for querying multiple documents.
        this.dalExampleLin = db.collection('example', {readConcern: {level: 'linearizable'}});

        this.logger.debug(new Date() + ' ' + this.logModule + ' init function completed');
        resolve();
      })
      .catch((err) => {
        this.logger.error(new Date() + ' ' + this.logModule + ' could not establish a connection to mongod.\' Check that the database is actually up. Error: ' + err);
        reject(err);
      });
    });
  };

  _connect(dbName){
    return new Promise((resolve, reject) => {
      this.logger.silly(new Date() + ' ' + this.logModule + ' _connect function called for database ' + dbName);
      this.logger.debug(new Date() + ' ' + this.logModule + ' attempting mongodb connection with conn string ' + this.connString);
      //attempt to connect to the replica set
      MongoClient.connect(this.connString).then((db) => {
        this.logger.debug(new Date() + ' ' + this.logModule + ' MongoClient success');
        //return the requested database. the database does not need to exist for this to work.
        resolve(db.db(dbName));
      })
      .catch((err) => {
        this.logger.error(new Date() + ' ' + this.logModule + ' could not establish a connection to mongod.\' Check that the database is actually up. Error: ' + err);
        reject(err);
      });
    });
  }

  insertDoc(docIn){
    return new Promise((resolve, reject) => {
      this.logger.debug(new Date() + ' ' + this.logModule + ' insertDoc function called with doc ' + JSON.stringify(docIn));

      //creating a deep copy to avoid modifying in the previous scope
      //note that Date() objects become type ISODate which is OK for MongoDB
      let doc = JSON.parse(JSON.stringify(docIn));

      //assigning an id allows for safe retries of inserts
      //the duplicate key exception prevents multiple inserts
      if(undefined == typeof doc._id){
        doc._id = new ObjectId();
      }

      //define the function inserting the doc with a write concern of majority
      //because the document has an _id it is safe to retry
      let fn = () => {
        return this.dalExample.insertOne(doc, {w:'majority'});
      };

      //call the function with retry logic
      this._retryOnErr(fn).then((res) => {
        this.logger.debug(new Date() + ' ' + this.logModule + ' document successfully inserted');
        resolve(doc._id);
      })
      .catch((err) => {
        this.logger.error(new Date() + ' ' + this.logModule + ' error inserting doc: ' + err);
        reject(err);
      })
    });
  };

  getById(id){
    return new Promise((resolve, reject) => {
      this.logger.silly(new Date() + ' ' + this.logModule + ' getById function called with id ' + id);

      //define the funciton to query a document by id and return all fields except the array of opids.
      //this query uses dalExampleLin which was created with the linearizable read concern. This means
      //the resulting document will be present on a majority of servers and also be the latest version
      //of the document. all linearized operations need maxTimeMS set to guard against the loss of
      //a majority of servers resulting in an infinitely hanging operation
      let fn = () => {
        return this.dalExampleLin.find({_id: id})
          .comment('getById from MongoDal.js with readConcern linearizable and 10s maxTimeMS')
          .maxTimeMS(10000)
          .next();
      };

      //call the function with retry logic
      this._retryOnErr(fn).then((doc) => {
        this.logger.debug(new Date() + ' ' + this.logModule + ' found doc with id ' + id);
        resolve(doc);
      })
      .catch((err) => {
        this.logger.error(new Date() + ' ' + this.logModule + ' error getting by id');
        reject(err);
      });
    });
  };

  countCol(){
    return new Promise((resolve, reject) => {
      this.logger.silly(new Date() + ' ' + this.logModule + ' countCol function called');

      //define the function return the collection count. the query comment helps with debugging
      //if this operation appears in slow query logs. this operation can be safely retried.
      let fn = () => {
        return this.dalExample.find({}).comment('countCol from MongoDal.js').count();
      };

      //call the function with retry logic
      this._retryOnErr(fn).then((count) => {
        this.logger.debug(new Date() + ' ' + this.logModule + ' collection count is ' + count);
        resolve(count);
      })
      .catch((err) => {
        this.logger.error(new Date() + ' ' + this.logModule + ' error counting collection: ' + err);
        reject(err);
      });
    })
  }

  //uses pattern for idempotency explained here: explore.mongodb.com/developer/nathaniel-may
  incCounter(id){
    return new Promise((resolve, reject) => {
      this.logger.silly(new Date() + ' ' + this.logModule + ' incCounter called for doc ' + id);

      //create a unique id to represent this particular increment operation
      let opid = new ObjectId();
      //define the function which increments a counter on a single document queried by id. 
      //successful increments will contain the unique opid in the opids array.
      //since all documents are queried by id -and- the opid not existing in the array
      //this function is safe to retry. slice removes the oldest elements in the array beyond
      //the specified size to prevent infinite growth of the opids array.
      let fn = () => {
        return new Promise((resolve, reject) => {
          this.dalExample.findOneAndUpdate(
            //query by id and that this operation hasn't been completed already
            {'_id': id, 'opids': {'$ne': opid}},
            //increment the counter and add the opid for this operation into the opids array
            //slice the oldest elements out of the array if it is too large
            {'$inc': {'counter': 1}, '$push': {'opids': {'$each': [opid], '$slice': -10000}}},
            //don't bring back the whole document which includes the list of opids
            //only return the new counter value for logging purposes
            {'projection': {'counter': 1, '_id':0}, 'returnOriginal': false, 'w':'majority'})
          .then((updatedDoc) => {
            this.logger.silly(new Date() + ' ' + this.logModule + ' incCounter updated doc');
            //value will be null when it matches no documents. this will happen when a network error 
            //occurred on the way back from the db before the retry, and the retry doesn't match any documents
            if(null != updatedDoc.value){
              resolve(updatedDoc.value.counter);
            }
            else{
              resolve();
            }
          })
          .catch((err) => {
            reject(err);
          });
        })
      };

      //call the function with retry logic
      this._retryOnErr(fn).then((count) => {
        if(undefined != count){
          this.logger.debug(new Date() + ' ' + this.logModule + ' counter incremented to ' + count);
        }
        else{
          this.logger.debug(new Date() + ' ' + this.logModule + ' counter is undefined because the query matched no documents or a retry resolved a network error');
        }
        //doesn't return the new value because while retrying after a network error which interrupted
        //the ok response, the query will match -no documents- since the operation succeeded and the 
        //opid will be present in the opids array. the response of the query will be undefined
        //in this instance. to get an accurate value, query the doc byId afterward.
        resolve();
      })
      .catch((err) => {
        this.logger.error(new Date() + ' ' + this.logModule + ' error incrementing counter: ' + err);
        reject(err);
      });
    });
  }

  deleteAllDocs(){
    return new Promise((resolve, reject) => {
      this.logger.silly(new Date() + ' ' + this.logModule + ' deleteAllDocs called');

      //define the function to delete all documents with write concern majority
      //this operation can be safely retried
      let fn = () => {
        return this.dalExample.deleteMany({}, {w:'majority'});
      };

      //call the function with retry logic
      this._retryOnErr(fn).then(() => {
        this.logger.debug(new Date() + ' ' + this.logModule + ' deleted all docs');
        resolve();
      })
      .catch((err) => {
        this.logger.error(new Date() + ' ' + this.logModule + ' error getting by id');
        reject(err);
      });
    });
  }

  _retryOnErr(fn){
    return new Promise((resolve, reject) => {
      //call the function for the first time
      fn().then((res) => {
        //if nothing went wrong, pass return the response
        resolve(res);
      })
      //the function returned an error on the first call
      .catch((err) => {
        //if the error is a network error or an interrupt error it may be able to be resolved by retrying
        if(MongoDal.networkErrors[err.code] != undefined || MongoDal.interruptErrors[err.code] != undefined){
          this.logger.warn(new Date() + ' ' + this.logModule + ' experienced network error- retrying');
          //call the function for the second time. The MongoDB driver automatically waits for a 
          //visible primary for no more than 30 seconds.
          fn().then((res) => {
            this.logger.debug(new Date() + ' ' + this.logModule + ' retry resolved network error');
            resolve(res);
          })
          .catch((err) => {
            //the only way for a duplicate key error to happen here is if it occured
            //after a retry, and not on the initial call. this means the initial call inserted
            //the document but the response was interrupted, so this error can be ignored
            if(err.code == MongoDal.errors['duplicate key exception']){
              this.logger.debug(new Date() + ' ' + this.logModule + ' retry resolved network error');
              resolve();
            }
            //If it's another kind of error, the retry did not resolve the issue and may mean the 
            //a majority of the replica set nodes are unreachable from this server
            else{
              this.logger.error(new Date() + ' ' + this.logModule + ' could not resolve with retry: ' + err);
              reject(new Error('Database Unavailable'));
            }
          });
        }
        //the error is not a network error or an interrupt error and is therefore not retryable
        else{
          this.logger.error(new Date() + ' ' + this.logModule + ' error is not retryable: ' + err);
          reject(err);
        }
      });
    });
  }

}

//define static class vars
MongoDal.networkErrors = {
  6:'host unreachable',
  7:'host not found',
  89:'network timeout',
  9001:'socket exception',
  'host unreachable':6,
  'host not found':7,
  'network timeout':89,
  'socket exception':9001
};

MongoDal.interruptErrors = {
  11601:'interrupted',
  11600:'interrupted at shutdown',
  11602:'interrupted due to repl state change',
  50:'exceeded time limit',
  'interrupted':11601,
  'interrupted at shutdown':11600,
  'interrupted due to repl state change':11602,
  'exceeded time limit':50
};

MongoDal.errors = {
  11000:'duplicate key exception',
  'duplicate key exception':11000
};

module.exports = MongoDal;
