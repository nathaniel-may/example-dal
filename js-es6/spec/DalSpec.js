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

class CallTracker{

  constructor(){
    this.called = 0;
  }

  netErr(){
    this.called++;
    return new Promise((resolve, reject) => {
      let err = new MongoError();
      err.code = 9001; //socket exception

      //network error on first call
      if(this.called == 1){
        reject(err);
      }
      //no error on subsequent calls
      else{
        resolve('resolved');
      }
    });
  }

  netErrThenDupKey(){
    this.called++;
    return new Promise((resolve, reject) => {
      let netErr = new MongoError();
      netErr.code = 9001; //socket exception

      let dupKeyErr = new MongoError();
      dupKeyErr.code = 11000;

      //network error on first call
      if(this.called == 1){
        reject(netErr);
      }
      //duplicate key err on subsequent calls
      else{
        reject(dupKeyErr);
      }
    });
  }

  dupKeyErr(){
    return new Promise((resolve, reject) => {
      let err = new MongoError();
      err.code = 11000;
      reject(err);
    })
  }
  
};

describe('MongoDal', () => {
  let logModule = 'JAS DAL';

  //test documents
  let testDoc = {
      string: 'string value',
      num: 99,
      array: [1, 2, 3],
      subDoc: {string1: 'str1', str2: 'str2'}
    };

  beforeAll((done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---beforeAll started---');

    //create MongoDal
    try{
      dal = new MongoDal('mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=myReplSet', 'silly');
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

  it('inserts one doc', (done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---inserts one doc---');
    dal.insertDoc(testDoc).then((id) => {
      logger.debug(new Date() + ' ' + logModule + ' got id: ' + id);
      expect(id instanceof ObjectId).toBe(true);
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' error inserting document: ' + err);
      fail(err); 
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---inserts one doc---\n');
      done();
    });
  });

  it('gets document by id', (done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---gets document by id---');
    let _id;
    dal.insertDoc(testDoc).then((id) => {
      logger.debug(new Date() + ' ' + logModule + ' inserted doc.');
      _id = id;
      return dal.getById(id);
    })
    .then((doc) => {
      expect(doc._id).toEqual(_id);
      expect(doc.num).toEqual(99);
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' error inserting document: ' + err);
      fail(err); 
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---gets document by id---\n');
      done();
    })
  });

  it('counts the collection', (done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---counts the collection---');
    dal.countCol().then((count) => {
      expect(count).toBe(0);
      return Promise.all([dal.insertDoc({test:0}),
                          dal.insertDoc({test:1}),
                          dal.insertDoc({test:2})]);
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' inserted all 3 docs');
      return dal.countCol();
    })
    .then((count) => {
      logger.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs');
      expect(count).toBe(3);
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' error counting the collection: ' + err);
      fail(err); 
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---counts the collection---\n');
      done();
    });
  });

  it('deletes all docs', (done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---deletes all docs---');
    Promise.all([dal.insertDoc({test:0}),
                 dal.insertDoc({test:1}),
                 dal.insertDoc({test:2})])
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' inserted all 3 docs');
      return dal.countCol();
    })
    .then((count) => {
      logger.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs');
      expect(count).toBe(3);
      return dal.deleteAllDocs();
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' all docs successfully deleted');
      return dal.countCol();
    })
    .then((count) => {
      logger.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs after delete');
      expect(count).toBe(0);
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' error deleting all docs and counting them: ' + err);
      fail(err); 
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---deletes all docs---\n');
      done();
    });
  });

  it('should retry on network error', (done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---should retry on error---');

    let callTracker = new CallTracker();
    logger.silly(new Date() + ' ' + logModule + ' created callTracker obj');
    spyOn(callTracker, 'netErr').and.callThrough();

    dal._retryOnErr(() => {return callTracker.netErr();}).then((res) => {
      expect(callTracker.netErr).toHaveBeenCalledTimes(2);
      expect(res).toBe('resolved');
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' problem with _retryOnErr. callTracker called ' + callTracker.called + ' times. Err: ' + err);
      fail(err);
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---should retry on network error---\n');
      done();
    });
  });

  it('does not retry on first duplicate key error', (done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---does not retry on first duplicate key error---');

    let callTracker = new CallTracker();
    spyOn(callTracker, 'dupKeyErr').and.callThrough();
    logger.debug(new Date() + ' ' + logModule + ' created call tracker');

    dal._retryOnErr(() => {return callTracker.dupKeyErr();})
    .catch((err) => {
      logger.debug(new Date() + ' ' + logModule + ' caught error' + err);
      expect(err.code).toBe(11000); //expect the duplicate key error
      expect(callTracker.dupKeyErr).toHaveBeenCalledTimes(1);
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---does not retry on first duplicate key error---\n');
      done();
    });
  });

  it('retries and eats duplicate key error on insert retry', (done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---retries and eats duplicate key error on insert retry---');

    let callTracker = new CallTracker();
    spyOn(callTracker, 'netErrThenDupKey').and.callThrough();
    logger.debug(new Date() + ' ' + logModule + ' created call tracker');

    dal._retryOnErr(() => {return callTracker.netErrThenDupKey();})
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' no error');
      expect(callTracker.netErrThenDupKey).toHaveBeenCalledTimes(2);
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' error retrying: ' + err);
      fail();
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---does not retry on first duplicate key error---\n');
      done();
    });
  });

  it('increments a counter', (done) => {
    logger.debug(new Date() + ' ' + logModule + ' ---increments a counter---');
    let doc = {};
    doc.counter = 0;

    dal.insertDoc(doc).then((id) => {
      logger.debug(new Date() + ' ' + logModule + ' inserted doc with counter');
      doc._id = id;
      return dal.incCounter(id);
    })
    .then((count) => {
      logger.debug(new Date() + ' ' + logModule + ' incCounter reports a new count of ' + count);
      expect(count).toBe(1);
      return dal.getById(doc._id);
    })
    .then((doc) => {
      logger.debug(new Date() + ' ' + logModule + ' fetched doc by id');
      expect(doc.counter).toBe(1);
    })
    .catch((err) => {
      logger.error(new Date() + ' ' + logModule + ' error testing incCounter: ' + err);
      fail();
    })
    .then(() => {
      logger.debug(new Date() + ' ' + logModule + ' ---increments a counter---\n');
      done();
    });
  });

  xit('doesnt double count after network error', (done) => {

  });

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
