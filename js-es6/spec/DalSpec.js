//load dependencies
var winston = require('winston');
var ObjectId = require('mongodb').ObjectId;
var MongoDal = require('../MongoDal');

//define variables
var logger;
var dal;

//std out logging settings
winston.remove(winston.transports.Console);
winston.add(winston.transports.Console, {colorize: true});
winston.level = 'silly';

describe('TraceLogger', function() {

  let logModule = 'JAS DAL';

  beforeAll(function(done){
    winston.debug(new Date() + ' ' + logModule + ' ---beforeAll started---');

    //create TraceLogger
    try{
      this.dal = new MongoDal('localhost:27017');
    }
    catch(err){
      winston.error(new Date() + ' ' + logModule + ' error creating MongoDal instance: ' + err);
      fail(err);
      done(); return;
    }

    //setup the logger
    dal.init()
    .then(function(){
      winston.debug(new Date() + ' ' + logModule + ' dal init completed');
      winston.debug(new Date() + ' ' + logModule + ' ---beforeAll completed---\n');
      done();
    })
    .catch(function(err){
      winston.error(new Date() + ' ' + logModule + ' error attempting to init trace logger: ' + err);
      done();
    });

  });

  it('gets document by id', function(done) {
    winston.debug(new Date() + ' ' + logModule + ' ---generates a trace id---');
    dal.getById()
    .then(function(id){
      winston.debug(new Date() + ' ' + logModule + ' got id: ' + id);
      if(id instanceof ObjectId){
        return id;
      }
      else{
        throw new Error(id + 'is not an instanceof ObjectId');
      }
    })
    .catch(function(err){
      winston.error(new Date() + ' ' + logModule + ' error getting trace id: ' + err);
      fail(err); 
    })
    .then(function(){
      winston.debug(new Date() + ' ' + logModule + ' ---generates a trace id---\n');
      done();
    });
  });

  afterEach(function(done){
    winston.silly(new Date() + ' ' + logModule + ' ---after each---');
    logger.deleteAllLogs()
    .then(function(count){
      winston.silly(new Date() + ' ' + logModule + ' deleted ' + count  + ' existing logs.');
      winston.silly(new Date() + ' ' + logModule + ' ---after each---\n');
      done();
    })
    .catch(function(err){
      winston.error(new Date() + ' ' + logModule + ' error deleting all logs in afterEach: ' + err + '\n');
      fail(err);
      done();
    });
    
  });

});
