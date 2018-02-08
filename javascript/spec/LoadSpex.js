//load dependencies
var winston = require('winston');
var ObjectId = require('mongodb').ObjectId;
var MongoError = require('mongodb').MongoError;
var MongoDal = require('../MongoDal');

//define variables
var logger;
var dal;

//std out logging settings
logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({colorize: true})
  ]
});
logger.level = 'silly';

describe('MongoDal Load Test', () => {
  let logModule = 'JAS LOAD';

  beforeAll((done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---beforeAll started---');

    //create MongoDal
    try{
      dal = new MongoDal('mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=repl0&w=majority', 'info');
    }
    catch(err){
      logger.error(new Date() + ' ' + logModule + ' error creating MongoDal instance: ' + err);
      fail(err);
      done(); return;
    }

    //setup the logger
    dal.init().then(() => {
      logger.debug(new Date() + ' ' + logModule + ' dal init completed');
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' error attempting to init MongoDal: ' + err);
      fail();
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---beforeAll completed---\n');
      done();
    });

  });

  it('makes 1000 updates on a single document without interruption', (done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---makes 1000 updates on a single document without interruption---');

    let docId;
    let goal = 1000;
    dal.insertDoc({counter: 0}).then((id) => {
      logger.debug(new Date() + ' ' + logModule + ' inserted single counter doc');
      docId = id;
      let promises = Array(goal).fill().map(() => dal.incCounter(id));
      logger.debug(new Date() + ' ' + logModule + ' about to fire all promises');
      //TODO timer here
      return Promise.all(promises);
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' all promises completed');
      return dal.getById(docId);
    })
    .then((doc) => {
      logger.debug(new Date() + ' ' + logModule + ' fetched doc');
      expect(doc.counter).toBe(goal);
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' error testing 1000 updates: ' + err);
      fail();
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---makes 1000 updates on a single document without interruption---\n');
      done();
    });


  }, 10000);

  afterEach((done) => {
    logger.silly(new Date() + ' ' + logModule + ' ---after each---');

    dal.deleteAllDocs()
    .then((count) => {
      logger.silly(new Date() + ' ' + logModule + ' deleted ' + count  + ' existing docs.');
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' error deleting all docs in afterEach: ' + err);
      fail(err);
      done();
    })
    .then(() => {
      logger.silly(new Date() + ' ' + logModule + ' ---after each---\n');
      done();
    });
    
  });

});
