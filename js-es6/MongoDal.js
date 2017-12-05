//example data access layer for MongoDB 3.4
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
    this.connString = connString;
    this._database = null;

    //logging
    logger.remove(logger.transports.Console);
    logger.add(logger.transports.Console, {colorize: true});
    logger.level = 'silly';

    logger.silly(new Date() + ' ' + this.logModule + ' MongoDal constructor completed');

  }

  init(){
    return new Promise((resolve, reject) => {
      logger.debug(new Date() + ' ' + this.logModule + ' init() function called');

      logger.debug(new Date() + ' ' + this.logModule + ' attempting mongodb connection with conn string ' + this.connString);
      this._connect('dal')
      .then((db) => {
        //define database object
        this._database = db;

        //define all necessary collections
        this.dalExample = db.collection('example');

        resolve();
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
      MongoClient.connect(this.connString)
      .then((db) => {
        logger.debug(new Date() + ' ' + this.logModule + ' MongoClient success');
        resolve(db);
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' could not establish a connection to mongod.\' Check that the database is actually up. Error: ' + err);
        reject(err);
      });
    });
  }

  insertDoc(doc){
    return new Promise((resolve, reject) => {
      logger.debug(new Date() + ' ' + this.logModule + ' insertDoc function called');

      //assigning an id allows for safe retries of inserts
      //the duplicate key exception prevents multiple inserts
      if(undefined == typeof doc._id){
        doc._id = new ObjectId();
      }

      let fn = () => {
        return this.dalExample.insert(doc).comment('insertDoc from ' + this.logModule);
      }
      this._retryOnErr(fn).then((res) => {
        logger.debug(new Date() + ' ' + this.logModule + ' document successfully inserted');
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' error inserting doc: ' + err);
      })
    });
  };

  getById(id){
    return new Promise((resolve, reject) => {
      logger.debug(new Date() + ' ' + this.logModule + ' getAllCurrentDebts function called');
      logger.debug(new Date() + ' ' + this.logModule + ' about to call find on debts');

      let fn = () => {
        return this.dalExample.find({_id: id}).comment('getById from ' + this.logModule).toArray();
      };
      return this._retryOnErr(fn).then((array) => {
        logger.debug(new Date() + ' ' + this.logModule + ' found ' + array.length + ' docs');
        resolve(array[0]);
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' error getting all current debts');
        reject(err);
      });
    });
  };

  getAllRecentDocuments(sinceDate){
    return new Promise((resolve, reject) => {
      logger.debug(new Date() + ' ' + this.logModule + ' getAllRecentDebts function called');
      logger.debug(new Date() + ' ' + this.logModule + ' about to call find on debts');

      let fn = () => {
        return debtsCol.find({date: {$gte: sinceDate}})
        .comment('getAllrecentDocs from ' + this.logModule).toArray(); //TODO array pulls them all into memory. Add cursor functions
      };
      this._retryOnErr(fn)
      .then((array) => {
        logger.debug(new Date() + ' ' + this.logModule + ' found ' + array.length + ' docs');
        resolve(array);
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' error getting all current debts');
        reject(err);
      });
    });
  };

  newFunction(param){
    return new Promise((resolve, reject) => {
      reject(new Error('function not defined yet'));
    });
  };

  _retryOnErr(fn){
    return new Promise((resolve, reject) => {
      fn()
      .then((res) => {
        resolve(res);
      })
      .catch((err) => { //TODO catch correctly
        if(err.message.includes('duplicate key error')){
          resolve(new message('ate the duplicate key error'));
        }
        else{
          logger.warn(new Date() + ' ' + this.logModule + ' experienced error- retrying');
          return fn();
        }
      })
      .then((res) => {
        resolve(res);
      })
      .catch((err) => {
        logger.error(new Date() + ' ' + this.logModule + ' could not resolve with retry: ' + err);
        reject(err);
      }); 
    });
  };

}

module.exports = MongoDal;

