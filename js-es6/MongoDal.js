//example data access layer for MongoDB
//with full idempotency for safe retry strategy

//load dependencies
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectId;
var logger = require('winston');

//TODO LIST
// - timeouts
// - 

class MongoDal{

  constructor(connString){
    this.logModule = 'DAL';

    logger.silly(new Date() + ' ' + this.logModule + ' MongoDal constructor called');
    this.connString = connString;
    this._database = null;

    //logging
    logger.remove(logger.transports.Console);
    logger.add(logger.transports.Console, {colorize: true});
    logger.level = 'silly'; //TODO pass from app config

    logger.silly(new Date() + ' ' + this.logModule + ' MongoDal constructor completed');

  }

  init(){
    let self = this;
    return new Promise(function(resolve, reject){
      logger.debug(new Date() + ' ' + self.logModule + ' init() function called');

      //if MongoClient is already connected, just return a db from the current connection
      if(null != self._database && typeof self._database == 'object'){
          logger.debug(new Date() + ' ' + self.logModule + ' mongoClient is already connected');
          resolve();
      }
      else{
        logger.debug(new Date() + ' ' + self.logModule + ' attempting mongodb connection with conn string ' + self.conn);
        mongoClient = MongoClient.connect(self.connString)
        .then(function(db){
          logger.debug(new Date() + ' ' + self.logModule + ' MongoClient success');
          self._database = db;
          resolve();
        })
        .catch(function(err){
          logger.error(new Date() + ' ' + self.logModule + ' could not establish a connection to mongod.\' Check that the database is actually up. Error: ' + err);
          reject(err);
        });
      }
    });
  };

  _getCol(dbName, colName){
    let self = this;
    logger.debug(new Date() + ' ' + self.logModule + ' getCol function called for namespace ' + dbName + '.' + colName);
    return new Promise(function(resolve, reject){
      self._getMongo(self.env)
      .then(function(database){
        let col = database.db(dbName).collection(colName);
        logger.debug(new Date() + ' ' + self.logModule + ' collection successfully obtained');
        resolve(col);
      })
      .catch(function(err){
        logger.error(new Date() + ' ' + self.logModule + ' failed to get collection from mongo client: ' + err);
        reject(err);
      }); 
    });
  };

  insertDoc(doc){
    let self = this;
    return new Promise(function(resolve, reject){
      logger.debug(new Date() + ' ' + self.logModule + ' insertDoc function called');
      self._getCol('dal','example')
      .then(function(col){
        //assigning an id allows for safe retries of inserts.
        //the duplicate key exception prevents multiple inserts
        if(undefined == typeof doc._id){
          doc._id = new ObjectId();
        }
        let fn = function(){
          return col.insert(doc).comment('insertingDoc from ' + self.logModule);
        }
        return self._retryOnErr(fn);
      })
      .then(function(res){
        logger.debug(new Date() + ' ' + self.logModule + ' document successfully inserted');
      })
      .catch(function(err){
        logger.error(new Date() + ' ' + self.logModule + ' error inserting doc: ' + err);
      })
    });
  };

  getById(id){
    let self = this;
    return new Promise(function(resolve, reject){
      logger.debug(new Date() + ' ' + self.logModule + ' getAllCurrentDebts function called');
      logger.debug(new Date() + ' ' + self.logModule + ' about to call find on debts');
      self._getCol('dal','example')
      .then(function(debtsCol){
        let fn = function(){
          return debtsCol.find({_id: id})
          .comment('getById from ' + self.logModule).toArray();
        };
        return self._retryOnErr(fn);
      })
      .then(function(array){
        logger.debug(new Date() + ' ' + self.logModule + ' found ' + array.length + ' docs');
        resolve(array[0]);
      })
      .catch(function(err){
        logger.error(new Date() + ' ' + self.logModule + ' error getting all current debts');
        reject(err);
      });
    });
  };

  getAllRecentDocuments(sinceDate){
    let self = this;
    return new Promise(function(resolve, reject){
      logger.debug(new Date() + ' ' + self.logModule + ' getAllRecentDebts function called');
      logger.debug(new Date() + ' ' + self.logModule + ' about to call find on debts');
      self._getCol('dal','example')
      .then(function(sinceDate){
        let fn = function(){
          return debtsCol.find({date: {$gte: sinceDate}})
          .comment('getAllrecentDocs from ' + self.logModule).toArray(); //TODO array pulls them all into memory. Add cursor functions
        };
        return self._retryOnErr(fn);
      })
      .then(function(array){
        logger.debug(new Date() + ' ' + self.logModule + ' found ' + array.length + ' docs');
        resolve(array);
      })
      .catch(function(err){
        logger.error(new Date() + ' ' + self.logModule + ' error getting all current debts');
        reject(err);
      });
    });
  };

  newFunction(email){
    let self = this;
    return new Promise(function(resolve, reject){
      reject(new Error('function not defined yet'));
    });
  };

  _retryOnErr(fn){
    let self = this;
    return new Promise(function(resolve, reject){
      fn()
      .then(function(res){
        resolve(res);
      })
      .catch(function(err){ //TODO catch correctly
        if(err.message.includes('duplicate key error')){
          resolve(new message('ate the duplicate key error'));
        }
        else{
          logger.warn(new Date() + ' ' + self.logModule + ' experienced error- retrying');
          return fn();
        }
      })
      .then(function(res){
        resolve(res);
      })
      .catch(function(err){
        logger.error(new Date() + ' ' + self.logModule + ' could not resolve with retry: ' + err);
        reject(err);
      }); 
    });
  };

}

module.exports = MongoDal;

