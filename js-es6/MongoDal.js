//fully idempotent example database access layer for MongoDB 3.4

//load dependencies
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectId;
var logger = require('winston');

//TODO LIST
// - timeouts
// - query comments

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
        return new Promise(() => {
          this.dalExample.insertOne(doc).then(() => { //TODO add $comment
            resolve(doc._id);
          })
          .catch((err) => {
            reject(err);
          })
        })
      };
      this._retryOnErr(fn).then((res) => {
        //TODO do something with res like return id
        logger.debug(new Date() + ' ' + this.logModule + ' document successfully inserted');
        resolve(res);
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
        return new Promise(() => {
          this.dalExample.findOne({_id: id}).then((doc) => { //TODO add $comment
            resolve(doc);
          }) 
          .catch((err) => {
            reject(err);
          })
        })
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
        return this.dalExample.count({}); //TODO add $comment
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

  deleteAllDocs(){
    return new Promise((resolve, reject) => {
      let fn = () => {
        return this.dalExample.deleteMany({}); //TODO add $comment
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

  genId(){
    return new Promise((resolve, reject) => {
      resolve(new ObjectId());
    })
  }

  _retryOnErr(fn){
    return new Promise((resolve, reject) => {
      fn().then((res) => {
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
      }).then((res) => {
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
