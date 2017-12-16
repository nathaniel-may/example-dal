//load dependencies
var winston = require('winston');
var ObjectId = require('mongodb').ObjectId;
var MongoError = require('mongodb').MongoError;
var MongoDal = require('../MongoDal');

//define variables
var logger;
var dal;

//std out logging settings
winston.remove(winston.transports.Console);
winston.add(winston.transports.Console, {colorize: true});
winston.level = 'silly';

class CallTracker{

  constructor(){
    this.called = 0;
  }

  errOnFirstCall(){
    this.called++;
    return new Promise((resolve, reject) => {
      if(this.called == 1){
        reject(new Error('I always reject the first call'));
      }
      else{
        resolve('I resolve after the first call');
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
    winston.debug(new Date() + ' ' + logModule + ' ---beforeAll started---');

    //create MongoDal
    try{
      dal = new MongoDal('mongodb://localhost:27017');
    }
    catch(err){
      winston.error(new Date() + ' ' + logModule + ' error creating MongoDal instance: ' + err);
      fail(err);
      done(); return;
    }

    //setup the logger
    dal.init().then(() => {
      winston.debug(new Date() + ' ' + logModule + ' dal init completed');
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error attempting to init MongoDal: ' + err);
      fail();
    })
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' ---beforeAll completed---\n');
      done();
    });

  });

  it('inserts one doc', (done) => {
    winston.debug(new Date() + ' ' + logModule + ' ---inserts one doc---');
    dal.insertDoc(testDoc).then((id) => {
      winston.debug(new Date() + ' ' + logModule + ' got id: ' + id);
      expect(id instanceof ObjectId).toBe(true);
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error inserting document: ' + err);
      fail(err); 
    })
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' ---inserts one doc---\n');
      done();
    });
  });

  it('gets document by id', (done) => {
    winston.debug(new Date() + ' ' + logModule + ' ---gets document by id---');
    let _id;
    dal.insertDoc(testDoc).then((id) => {
      winston.debug(new Date() + ' ' + logModule + ' inserted doc.');
      _id = id;
      return dal.getById(id);
    })
    .then((doc) => {
      expect(doc._id).toEqual(_id);
      expect(doc.num).toEqual(99);
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error inserting document: ' + err);
      fail(err); 
    })
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' ---gets document by id---\n');
      done();
    })
  });

  it('counts the collection', (done) => {
    winston.debug(new Date() + ' ' + logModule + ' ---counts the collection---');
    dal.countCol().then((count) => {
      expect(count).toBe(0);
      return Promise.all([dal.insertDoc({test:0}),
                          dal.insertDoc({test:1}),
                          dal.insertDoc({test:2})]);
    })
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' inserted all 3 docs');
      return dal.countCol();
    })
    .then((count) => {
      winston.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs');
      expect(count).toBe(3);
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error counting the collection: ' + err);
      fail(err); 
    })
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' ---counts the collection---\n');
      done();
    });
  });

  it('deletes all docs', (done) => {
    winston.debug(new Date() + ' ' + logModule + ' ---deletes all docs---');
    Promise.all([dal.insertDoc({test:0}),
                 dal.insertDoc({test:1}),
                 dal.insertDoc({test:2})])
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' inserted all 3 docs');
      return dal.countCol();
    })
    .then((count) => {
      winston.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs');
      expect(count).toBe(3);
      return dal.deleteAllDocs();
    })
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' all docs successfully deleted');
      return dal.countCol();
    })
    .then((count) => {
      winston.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs after delete');
      expect(count).toBe(0);
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error deleting all docs and counting them: ' + err);
      fail(err); 
    })
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' ---deletes all docs---\n');
      done();
    });
  });

  it('should retry on error', (done) => {
    winston.debug(new Date() + ' ' + logModule + ' ---should retry on error---');

    let callTracker = new CallTracker();
    winston.silly(new Date() + ' ' + logModule + ' created callTracker obj');
    spyOn(callTracker, 'errOnFirstCall').and.callThrough();

    dal._retryOnErr(callTracker.errOnFirstCall()).then((res) => {
      expect(callTracker.errOnFirstCall).toHaveBeenCalledTimes(2);
      expect(res).toBe('I resolve after the first call');
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' problem with _retryOnErr. callTracker called ' + callTracker.called + ' times. Err: ' + err);
      fail(err);
    })
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' ---should retry on error---\n');
      done();
    });
  });

  it('does not retry on first duplicate key error', (done) => {
    winston.debug(new Date() + ' ' + logModule + ' ---does not retry on first duplicate key error---');

    let callTracker = new CallTracker();
    spyOn(callTracker, 'dupKeyErr').and.callThrough();
    winston.debug(new Date() + ' ' + logModule + ' created call tracker');

    dal._retryOnErr(callTracker.dupKeyErr())
    .catch((err) => {
      winston.debug(new Date() + ' ' + logModule + ' caught error' + err);
      expect(err.code).toBe(11000); //expect the duplicate key error
      expect(callTracker.dupKeyErr).toHaveBeenCalledTimes(1);
    })
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' ---does not retry on first duplicate key error---\n');
      done();
    });
  });

  xit('retries and eats duplicate key error on insert retry', (done) => {
    fail(new Error('test not completed'));
    done();
  });

  xit('fails without retrying on cant $divide by zero error', (done) => {
    fail(new Error('test not completed'));
    done();
  });

  afterEach((done) => {
    winston.silly(new Date() + ' ' + logModule + ' ---after each---');
    dal.deleteAllDocs()
    .then((count) => {
      winston.silly(new Date() + ' ' + logModule + ' deleted ' + count  + ' existing docs.');
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error deleting all docs in afterEach: ' + err);
      fail(err);
      done();
    })
    .then(() => {
      winston.silly(new Date() + ' ' + logModule + ' ---after each---\n');
      done();
    });
    
  });

});
