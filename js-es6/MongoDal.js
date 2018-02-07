//fully idempotent example database access layer for MongoDB 3.4

//load dependencies
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectId;
var ReadConcern = require('mongodb').ReadConcern;
var Winston = require('winston');

class MongoDal{

  constructor(connString, logLevel){
    this.logModule = 'DAL';
    this.connString = connString;
    this._database = null;

    // std out logging
    this.logger = new (Winston.Logger)({
      transports: [
        new (Winston.transports.Console)({colorize: true})
      ]
    });
    this.logger.level = logLevel;

    //wrap the log functions with timestamp and module tags
    const levels = ['silly', 'debug', 'info', 'warn', 'error'];
    for(let level = 0; level<levels.length; level++){
      const fn = this.logger[levels[level]];
      this.logger[levels[level]] = str => fn(`${new Date()} ${this.logModule} ${str}`);
    }


    this.logger.silly(`MongoDal constructor completed`);
  }

  init(){
    this.logger.silly(`init function called`);
    //attempt to connect to the database only once
    return this._connect('dal').then(db => {
      //define database object so that reconnecting is not required
      this._database = db;

      //define all necessary collections
      //every document read from this collection will be present on at least a majority of servers
      //but may not be the latest version of the document
      this.dalData = db.collection('data', {readConcern: {level: 'majority'}});
      //single documents queried from this collection will be present on at least a majority of servers
      //and be the latest version of that document. this cannot be used for querying multiple documents.
      this.dalDataLin = db.collection('data', {readConcern: {level: 'linearizable'}});

      this.logger.debug(`init function completed`);
    })
    .catch(err => {
      this.logger.error(`could not establish a connection to mongod. Check that the database is actually up. Error: ${err}`);
      throw err;
    });
  };

  _connect(dbName){
    this.logger.silly(`_connect function called for database ${dbName}`);
    this.logger.debug(`attempting mongodb connection with conn string ${this.connString}`);
    //attempt to connect to the replica set
    return MongoClient.connect(this.connString).then(db => {
      this.logger.debug(`MongoClient success`);
      //return the requested database. the database does not need to exist for this to work.
      return db.db(dbName);
    })
    .catch(err => {
      this.logger.error(`_connect failed`);
      throw err;
    });
  }

  insertDoc(doc){
    this.logger.debug(`insertDoc function called with doc ${JSON.stringify(doc)}`);

    //assigning an id allows for safe retries of inserts
    //the duplicate key exception prevents multiple inserts
    //using a spread only creates a shallow copy of doc, however this prevents the new id from modifying the previous scope
    if(!('_id' in doc)){
      this.logger.debug(`no _id found. creating one.`);
      doc = {...doc, _id: new ObjectId()}
    }

    //define the function inserting the doc with a write concern of majority
    //because the document has an _id it is safe to retry
    const fn = (doc, writeConcern) => this.dalData.insertOne(doc, writeConcern);

    //call the function with retry logic
    return this._retryOnErr(fn, doc, {w:'majority'}).then((res) => {
      this.logger.debug(`document successfully inserted`);
      return doc._id;
    })
    .catch(err => {
      this.logger.error(`error inserting doc: ${err}`);
      throw err;
    })
  };

  getById(id){
    this.logger.silly(`getById function called with id ${id}`);

    //define the funciton to query a document by id and return all fields except the array of opids.
    //this query uses dalDataLin which was created with the linearizable read concern. This means
    //the resulting document will be present on a majority of servers and also be the latest version
    //of the document. all linearized operations need maxTimeMS set to guard against the loss of
    //a majority of servers resulting in an infinitely hanging operation
    const fn = () => this.dalDataLin.find({_id: id})
                         .comment('getById from MongoDal.js with readConcern linearizable and 10s maxTimeMS')
                         .maxTimeMS(10000)
                         .next();

    //call the function with retry logic
    return this._retryOnErr(fn).then(doc => {
      this.logger.debug(`found doc with id ${id}`);
      return doc;
    })
    .catch(err => {
      this.logger.error(`error getting by id`);
      throw err;
    });
  };

  countCol(){
    this.logger.silly(`countCol function called`);

    //define the function return the collection count. the query comment helps with debugging
    //if this operation appears in slow query logs. this operation can be safely retried.
    const fn = () => this.dalData.find({}).comment('countCol from MongoDal.js').count();

    //call the function with retry logic
    return this._retryOnErr(fn).then(count => {
      this.logger.debug(`collection count is ${count}`);
      return count;
    })
    .catch(err => {
      this.logger.error(`error counting collection: ${err}`);
      throw err;
    });
  }

  //uses pattern for idempotency explained here: explore.mongodb.com/developer/nathaniel-may
  incCounter(id){
    this.logger.silly(`incCounter called for doc ${id}`);

    //create a unique id to represent this particular increment operation
    const opid = new ObjectId();
    //define the function which increments a counter on a single document queried by id. 
    //successful increments will contain the unique opid in the opids array.
    //since all documents are queried by id -and- the opid not existing in the array
    //this function is safe to retry. slice removes the oldest elements in the array beyond
    //the specified size to prevent infinite growth of the opids array.
    const fn = () => this.dalData.findOneAndUpdate(
                                 //query by id and that this operation hasn't been completed already
                                 {'_id': id, 'opids': {'$ne': opid}},
                                 //increment the counter and add the opid for this operation into the opids array
                                 //slice the oldest elements out of the array if it is too large
                                 {'$inc': {'counter': 1}, '$push': {'opids': {'$each': [opid], '$slice': -10000}}},
                                 //don't bring back the whole document which includes the list of opids
                                 //only return the new counter value for logging purposes
                                 {'projection': {'counter': 1, '_id':0}, 'returnOriginal': false, 'w':'majority'})
    .then(updatedDoc => {
      this.logger.silly(`incCounter updated doc`);
      //value will be null when it matches no documents. this will happen when a network error 
      //occurred on the way back from the db before the retry, and the retry doesn't match any documents
      if(null != updatedDoc.value){
        return updatedDoc.value.counter;
      }
    })
    .catch(err => {
      throw err;
    });

    //call the function with retry logic
    return this._retryOnErr(fn).then(count => {
      if(undefined != count){
        this.logger.debug(`counter incremented to ${count}`);
      }
      else{
        this.logger.debug(`counter is undefined because the query matched no documents or a retry resolved a network error`);
      }
      //doesn't return the new value because while retrying after a network error which interrupted
      //the ok response, the query will match -no documents- since the operation succeeded and the 
      //opid will be present in the opids array. the response of the query will be undefined
      //in this instance. to get an accurate value, query the doc byId afterward.
    })
    .catch(err => {
      this.logger.error(`error incrementing counter: ${err}`);
      throw err;
    });
  }

  deleteAllDocs(){
    this.logger.silly(`deleteAllDocs called`);

    //define the function to delete all documents with write concern majority
    //this operation can be safely retried
    const fn = () => this.dalData.deleteMany({}, {w:'majority'});

    //call the function with retry logic
    return this._retryOnErr(fn).then(() => this.logger.debug(`deleted all docs`))
    .catch(err => {
      this.logger.error(`error getting by id`);
      throw err;
    });
  }

  //arguments: first parameter is the function to call which returns a promise. All following arguments are passed to the function when called.
  _retryOnErr(...args){
    this.logger.silly(`_retryOnErr called`);
    //remove the first arg and store it as fn
    //args now only contains the args to pass to fn
    const fn = args.shift()
    //call the function for the first time
    return fn.apply(this, args).then(res => {
      this.logger.silly(`success on first attempt`);
      //if nothing went wrong, return the response
      return res;
    })
    //the function returned an error on the first call
    .catch(err => {
      //if the error is a network error or an interrupt error it may be able to be resolved by retrying
      if(MongoDal.networkErrors[err.code] != undefined || MongoDal.interruptErrors[err.code] != undefined){
        this.logger.warn(`experienced network error- retrying`);
        //call the function for the second time. The MongoDB driver automatically waits for a 
        //visible primary for no more than 30 seconds.
        return fn.apply(this, args).then(res => {
          this.logger.debug(`retry resolved network error`);
          return res;
        })
        .catch(err => {
          //the only way for a duplicate key error to happen here is if it occured
          //after a retry, and not on the initial call. this means the initial call inserted
          //the document but the response was interrupted, so this error can be ignored
          if(err.code == MongoDal.errors['duplicate key exception']){
            this.logger.debug(`retry resolved network error`);
            return;
          }
          //If it's another kind of error, the retry did not resolve the issue and may mean the 
          //a majority of the replica set nodes are unreachable from this server
          else{
            this.logger.error(`could not resolve with retry: ${err}`);
            throw new Error('Database Unavailable');
          }
        })
      }
      //the error is not a network error or an interrupt error and is therefore not retryable
      else{
        this.logger.error(`experienced error that is not retryable: ${err}`);
        throw err;
      }
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
