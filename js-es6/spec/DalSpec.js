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
    dal.init()
    .then(() => {
      winston.debug(new Date() + ' ' + logModule + ' dal init completed');
      winston.debug(new Date() + ' ' + logModule + ' ---beforeAll completed---\n');
      done();
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error attempting to init MongoDal: ' + err);
      done();
    });

  });

  it('inserts one doc', (done) => {
    winston.debug(new Date() + ' ' + logModule + ' ---inserts one doc---');
    dal.insertDoc(testDoc).then((id) => {
      winston.debug(new Date() + ' ' + logModule + ' got id: ' + id);
      expect(id instanceof ObjectId).toBe(true);
      winston.debug(new Date() + ' ' + logModule + ' ---inserts one doc---\n');
      done();
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error inserting document: ' + err);
      fail(err); 
      done();
    })
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
      winston.debug(new Date() + ' ' + logModule + ' ---inserts one doc---\n');
      done();
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error inserting document: ' + err);
      fail(err); 
      done();
    })
  });

  xit('retries and eats duplicate key error on insert retry', (done) => {
    fail(new Error('test not completed'));
    done();
  });

  xit('fails without retrying on cant $divide by zero error', (done) => {
    fail(new Error('test not completed'));
    done();
  });

  it('counts the collection', (done) => {
    winston.debug(new Date() + ' ' + logModule + ' ---counts the collection---');
    dal.countCol().then((count) => {
      expect(count).toBe(0);
      return Promise.all([dal.insertDoc({test:0}),
                          dal.insertDoc({test:1}),
                          dal.insertDoc({test:2})]);
    }).then(() => {
      winston.debug(new Date() + ' ' + logModule + ' inserted all 3 docs');
      return dal.countCol();
    }).then((count) => {
      winston.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs');
      expect(count).toBe(3);
      winston.debug(new Date() + ' ' + logModule + ' ---counts the collection---\n');
      done();
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error counting the collection: ' + err);
      fail(err); 
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
      winston.debug(new Date() + ' ' + logModule + ' ---deletes all docs---\n');
      done();
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error deleting all docs and counting them: ' + err);
      fail(err); 
      done();
    });
  });

  it('should retry on error', function(done) {
    winston.debug(new Date() + ' ' + logModule + ' ---should retry on error---');

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
            resolve('success');
          }
        });
      }
      
    };

    let callTracker = new CallTracker();
    winston.silly(new Date() + ' ' + logModule + ' created callTracker obj');
    spyOn(callTracker, 'errOnFirstCall').and.callThrough();
    let fn = function(){ return callTracker.errOnFirstCall(); };

    dal._retryOnErr(fn)
    .then(function(res){
      expect(callTracker.errOnFirstCall).toHaveBeenCalledTimes(2);
      expect(res).toBe('success');
    })
    .catch(function(err){
      winston.error(new Date() + ' ' + logModule + ' problem with _retryOnErr. callTracker called ' + callTracker.called + ' times. Err: ' + err + '\n');
      fail(err);
    })
    .then(function(){
      winston.debug(new Date() + ' ' + logModule + ' ---should retry on error---\n');
      done();
    });
  });

  afterEach((done) => {
    winston.silly(new Date() + ' ' + logModule + ' ---after each---');
    dal.deleteAllDocs()
    .then((count) => {
      winston.silly(new Date() + ' ' + logModule + ' deleted ' + count  + ' existing docs.');
      winston.silly(new Date() + ' ' + logModule + ' ---after each---\n');
      done();
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error deleting all docs in afterEach: ' + err);
      fail(err);
      done();
    });
    
  });

});
