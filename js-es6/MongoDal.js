//fully idempotent example database access layer for MongoDB 3.4

//load dependencies
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectId;
var winston = require('winston');

//define vars
var logger;

class MongoDal{

  constructor(connString, logLevel){
    this.logModule = 'DAL';
    this.connString = connString;
    this._database = null;

    //std out logging
    logger = new (winston.Logger)({
      transports: [
        new (winston.transports.Console)({colorize: true})
      ]
    });
    logger.level = logLevel;

    logger.silly(new Date() + ' ' + this.logModule + ' MongoDal constructor completed');
  }

  init(){
    return new Promise((resolve, reject) => {
      logger.debug(new Date() + ' ' + this.logModule + ' init function called');
      this._connect('dal').then((db) => {
        //define database object
        this._database = db;

        //define all necessary collections
        this.dalExample = db.collection('example');

        resolve();
        logger.debug(new Date() + ' ' + this.logModule + ' init function completed');
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' could not establish a connection to mongod.\' Check that the database is actually up. Error: ' + err);
        reject(err);
      });
    });
  };

  _connect(dbName){
    logger.debug(new Date() + ' ' + this.logModule + ' _connect function called for database ' + dbName + '.');
    return new Promise((resolve, reject) => {
      logger.debug(new Date() + ' ' + this.logModule + ' attempting mongodb connection with conn string ' + this.connString);
      MongoClient.connect(this.connString).then((db) => {
        logger.debug(new Date() + ' ' + this.logModule + ' MongoClient success');
        resolve(db.db(dbName));
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' could not establish a connection to mongod.\' Check that the database is actually up. Error: ' + err);
        reject(err);
      });
    });
  }

  insertDoc(docIn){
    return new Promise((resolve, reject) => {
      logger.debug(new Date() + ' ' + this.logModule + ' insertDoc function called with doc ' + JSON.stringify(docIn));

      //creating a deep copy to avoid modifying in the previous scope
      //note that Date() objects become type ISODate which is OK for MongoDB
      let doc = JSON.parse(JSON.stringify(docIn));

      //assigning an id allows for safe retries of inserts
      //the duplicate key exception prevents multiple inserts
      if(undefined == typeof doc._id){
        doc._id = new ObjectId();
      }

      let fn = () => {
        return this.dalExample.insertOne(doc, {w:'majority'});
      };

      this._retryOnErr(fn).then((res) => {
        logger.debug(new Date() + ' ' + this.logModule + ' document successfully inserted');
        resolve(doc._id);
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' error inserting doc: ' + err);
        reject(err);
      })
    });
  };

  getById(id){
    return new Promise((resolve, reject) => {
      logger.debug(new Date() + ' ' + this.logModule + ' getById function called with id ' + id);

      let fn = () => {
        return this.dalExample.find({_id: id}).comment('getById from MongoDal.js').next();
      };

      this._retryOnErr(fn).then((doc) => {
        logger.debug(new Date() + ' ' + this.logModule + ' found doc with id ' + id);
        resolve(doc);
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' error getting by id');
        reject(err);
      });
    });
  };

  countCol(){
    return new Promise((resolve, reject) => {
      let fn = () => {
        return this.dalExample.find({}).comment('countCol from MongoDal.js').count();
      };

      this._retryOnErr(fn).then((count) => {
        logger.debug(new Date() + ' ' + this.logModule + ' collection count is ' + count);
        resolve(count);
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' error counting collection: ' + err);
        reject(err);
      });
    })
  }

  //uses pattern for idempotency explained here: explore.mongodb.com/developer/nathaniel-may
  incCounter(id){
    logger.debug(new Date() + ' ' + this.logModule + ' incCounter called for doc ' + id);
    return new Promise((resolve, reject) => {
      let opid = new ObjectId();
      let fn = () => {
        return new Promise((resolve, reject) => {
          this.dalExample.findOneAndUpdate(
            {'_id': id, 'opids': {'$ne': opid}},
            {'$inc': {'counter': 1}, '$push': {'opids': {'$each': [opid], '$slice': -10000}}},
            {'projection': {'counter': 1, '_id':0}, 'returnOriginal': false, 'w':'majority'})
          .then((updatedDoc) => {
            logger.debug(new Date() + ' ' + this.logModule + ' incCounter updated doc ' + id + ' to ' + updatedDoc.value.counter);
            //doesn't return the new value because after a retry where the value had already updated, 
            //this would be undefined. To get an accurate value, query the doc byId afterward.
            resolve();
          })
          .catch((err) => {
            reject(err);
          });
        })
      };

      this._retryOnErr(fn).then((count) => {
        logger.debug(new Date() + ' ' + this.logModule + ' counter is now set to ' + count);
        resolve(count);
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' error incrementing counter: ' + err);
        reject(err);
      });
    });
  }

  deleteAllDocs(){
    return new Promise((resolve, reject) => {
      let fn = () => {
        return this.dalExample.deleteMany({}, {w:'majority'});
      };

      this._retryOnErr(fn).then(() => {
        logger.debug(new Date() + ' ' + this.logModule + ' deleted all docs');
        resolve();
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' error getting by id');
        reject(err);
      });
    });
  }

  _retryOnErr(fn){
    return new Promise((resolve, reject) => {
      fn().then((res) => {
        resolve(res);
      })
      .catch((err) => {
        if(MongoDal.networkErrors[err.code] != undefined || MongoDal.interruptErrors[err.code] != undefined){
          logger.warn(new Date() + ' ' + this.logModule + ' experienced network error- retrying');
          fn().then((res) => {
            logger.debug(new Date() + ' ' + this.logModule + ' retry resolved network error');
            resolve(res);
          })
          .catch((err) => {
            //eats duplicate key during retry
            if(err.code == 11000){
              logger.debug(new Date() + ' ' + this.logModule + ' retry resolved network error');
              resolve();
            }
            else{
              logger.error(new Date() + ' ' + this.logModule + ' could not resolve with retry: ' + err);
              reject(new Error('Database Unavailable'));
            }
          });
        }
        //the error is not retryable
        else{
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
  9001:'socket exception'
};

MongoDal.interruptErrors = {
  11601:'interrupted',
  11600:'interrupted at shutdown',
  11602:'interrupted due to repl state change',
  50:'exceeded time limit'
};

module.exports = MongoDal;
