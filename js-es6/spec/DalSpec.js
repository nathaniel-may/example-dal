//load dependencies
var winston = require('winston');
var ObjectId = require('mongodb').ObjectId;
var MongoError = require('mongodb').MongoError;
var MongoDal = require('../MongoDal');
//TODO REMOVE THIS***
var CircularJSON = require('circular-json');

//define variables
var dal;

//std out logging settings
this.logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({colorize: true})
  ]
});
this.logger.level = 'silly';

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

  sockExceptCallThrough(fn){
    this.called++;
    return new Promise((resolve, reject) => {
      let sockErr = new MongoError();
      sockErr.code = 9001;

      if(this.called == 1){
        fn().then(() => {
          reject(sockErr);
        })
        .catch((err) => {
          reject(err);
        });
      }
      else{
        //TODO make this one return line
        fn().then(() => resolve())
        .catch(() => reject());
      }
    });
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
    this.logger.debug(new Date() + ' ' + logModule + ' ---beforeAll started---');

    //create MongoDal
    try{
      dal = new MongoDal('mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=repl0&w=majority', 'silly');
    }
    catch(err){
      this.logger.error(new Date() + ' ' + logModule + ' error creating MongoDal instance: ' + err);
      fail(err);
      done(); return;
    }

    //setup the this.logger
    dal.init().then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' dal init completed');
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' error attempting to init MongoDal: ' + err);
      fail();
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' ---beforeAll completed---\n');
      done();
    });

  });

  it('inserts one doc', (done) => {
    this.logger.debug(new Date() + ' ' + logModule + ' ---inserts one doc---');
    dal.insertDoc(testDoc).then((id) => {
      this.logger.debug(new Date() + ' ' + logModule + ' got id: ' + id);
      expect(id instanceof ObjectId).toBe(true);
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' error inserting document: ' + err);
      fail(err); 
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' ---inserts one doc---\n');
      done();
    });
  });

  it('gets document by id', (done) => {
    this.logger.debug(new Date() + ' ' + logModule + ' ---gets document by id---');
    let _id;
    dal.insertDoc(testDoc).then((id) => {
      this.logger.debug(new Date() + ' ' + logModule + ' inserted doc.');
      _id = id;
      return dal.getById(id);
    })
    .then((doc) => {
      expect(doc._id).toEqual(_id);
      expect(doc.num).toEqual(99);
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' error inserting document: ' + err);
      fail(err); 
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' ---gets document by id---\n');
      done();
    })
  });

  it('counts the collection', (done) => {
    this.logger.debug(new Date() + ' ' + logModule + ' ---counts the collection---');
    dal.countCol().then((count) => {
      expect(count).toBe(0);
      return Promise.all([dal.insertDoc({test:0}),
                          dal.insertDoc({test:1}),
                          dal.insertDoc({test:2})]);
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' inserted all 3 docs');
      return dal.countCol();
    })
    .then((count) => {
      this.logger.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs');
      expect(count).toBe(3);
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' error counting the collection: ' + err);
      fail(err); 
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' ---counts the collection---\n');
      done();
    });
  });

  it('deletes all docs', (done) => {
    this.logger.debug(new Date() + ' ' + logModule + ' ---deletes all docs---');
    Promise.all([dal.insertDoc({test:0}),
                 dal.insertDoc({test:1}),
                 dal.insertDoc({test:2})])
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' inserted all 3 docs');
      return dal.countCol();
    })
    .then((count) => {
      this.logger.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs');
      expect(count).toBe(3);
      return dal.deleteAllDocs();
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' all docs successfully deleted');
      return dal.countCol();
    })
    .then((count) => {
      this.logger.debug(new Date() + ' ' + logModule + ' counted ' + count + ' docs after delete');
      expect(count).toBe(0);
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' error deleting all docs and counting them: ' + err);
      fail(err); 
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' ---deletes all docs---\n');
      done();
    });
  });

  it('should retry on network error', (done) => {
    this.logger.debug(new Date() + ' ' + logModule + ' ---should retry on error---');

    let callTracker = new CallTracker();
    this.logger.silly(new Date() + ' ' + logModule + ' created callTracker obj');
    spyOn(callTracker, 'netErr').and.callThrough();

    dal._retryOnErr(() => {return callTracker.netErr();}).then((res) => {
      expect(callTracker.netErr).toHaveBeenCalledTimes(2);
      expect(res).toBe('resolved');
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' problem with _retryOnErr. callTracker called ' + callTracker.called + ' times. Err: ' + err);
      fail(err);
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' ---should retry on network error---\n');
      done();
    });
  });

  it('does not retry on first duplicate key error', (done) => {
    this.logger.debug(new Date() + ' ' + logModule + ' ---does not retry on first duplicate key error---');

    let callTracker = new CallTracker();
    spyOn(callTracker, 'dupKeyErr').and.callThrough();
    this.logger.debug(new Date() + ' ' + logModule + ' created call tracker');

    dal._retryOnErr(() => callTracker.dupKeyErr())
    .catch((err) => {
      this.logger.debug(new Date() + ' ' + logModule + ' caught error' + err);
      expect(err.code).toBe(11000); //expect the duplicate key error
      expect(callTracker.dupKeyErr).toHaveBeenCalledTimes(1);
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' ---does not retry on first duplicate key error---\n');
      done();
    });
  });

  it('retries and eats duplicate key error on insert retry', (done) => {
    this.logger.debug(new Date() + ' ' + logModule + ' ---retries and eats duplicate key error on insert retry---');

    let callTracker = new CallTracker();
    spyOn(callTracker, 'netErrThenDupKey').and.callThrough();
    this.logger.debug(new Date() + ' ' + logModule + ' created call tracker');

    dal._retryOnErr(() => callTracker.netErrThenDupKey())
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' no error');
      expect(callTracker.netErrThenDupKey).toHaveBeenCalledTimes(2);
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' error retrying: ' + err);
      fail();
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' ---does not retry on first duplicate key error---\n');
      done();
    });
  });

  it('increments a counter', (done) => {
    this.logger.debug(new Date() + ' ' + logModule + ' ---increments a counter---');
    let doc = {};
    doc.counter = 0;

    dal.insertDoc(doc).then((id) => {
      this.logger.debug(new Date() + ' ' + logModule + ' inserted doc with counter');
      doc._id = id;
      return dal.incCounter(id);
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' incCounter success');
      return dal.getById(doc._id);
    })
    .then((doc) => {
      this.logger.debug(new Date() + ' ' + logModule + ' fetched doc by id');
      expect(doc.counter).toBe(1);
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' error testing incCounter: ' + err);
      fail();
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' ---increments a counter---\n');
      done();
    });
  });

  xit('doesnt double count after network error', (done) => {
    this.logger.silly(new Date() + ' ' + logModule + ' ---doesnt double count after network error---');
    let counterDoc = {};
    counterDoc.counter = 0;

    let callTracker = new CallTracker();
    let updateFn;

    spyOn(dal, '_retryOnErr').and.callThrough();

    dal.insertDoc(counterDoc).then((id) => {
      this.logger.debug(new Date() + ' ' + logModule + ' inserted doc with counter');
      counterDoc._id = id;
      let opid = new ObjectId();
      //TODO this step is unnecessary
      return dal._incCounterTail(id, opid);
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' incCounter success');
      updateFn = dal.incCounter;
      return dal.getById(counterDoc._id);
    })
    .then((doc) => {
      this.logger.debug(new Date() + ' ' + logModule + ' fetched doc by id');
      expect(doc.counter).toBe(1);
      //TODO this nonsense
      updateFn.bind(dal);
      let updateCall = () => updateFn();
      let toRetry = () => callTracker.sockExceptCallThrough(updateCall);
      return dal._retryOnErr(toRetry);
    })
    .then(() => {
      this.logger.debug(new Date() + ' ' + logModule + ' incCounter success with network error');
      return dal.getById(counterDoc._id);
    })
    .then((doc) => {
      this.logger.debug(new Date() + ' ' + logModule + ' fetched doc by id');
      expect(doc.counter).toBe(2);
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' error avoiding double count: ' + err);
      fail();
    })
    .then(() => {
      this.logger.silly(new Date() + ' ' + logModule + ' ---doesnt double count after network error---\n');
      done();
    });
    
  });

  fit('2 doesnt double count after network error 2 ', (done) => {
    this.logger.silly(new Date() + ' ' + logModule + ' ---doesnt double count after network error---');
    let counterDoc = {};
    counterDoc.counter = 0;

    let callTracker = new CallTracker();

    class FakeCol{

      constructor(realCol){      
        this.called = 0;
        this.realCol = realCol;

        //std out logging settings
        this.logger = new (winston.Logger)({
          transports: [
            new (winston.transports.Console)({colorize: true})
          ]
        });
        this.logger.level = 'silly';

        this.logger.silly(new Date() + ' ' + logModule + ' FakeCol instance created');
      }

      findOneAndUpdate(query, update, options){
        this.logger.silly(new Date() + ' ' + logModule + ' findOneAndUpdate called');
        return this.callThroughWithNetErr(query, update, options);
      }

      callThroughWithNetErr(query, update, options){
        return new Promise((resolve, reject) => {
          this.called++;

          let sockErr = new MongoError();
          sockErr.code = 9001;
          
          if(this.called == 1){
            this.logger.silly(new Date() + ' ' + logModule + ' callThroughWithNetErr called for first time');
            realCol.findOneAndUpdate(query, update, options).then(() => {
              this.logger.silly(new Date() + ' ' + logModule + ' rejecting with socket err');
              reject(sockErr);
            })
            .catch(err => {
              this.logger.err(new Date() + ' ' + logModule + ' unexpected error' + err);
              reject(err);
            });
          }
          else if(this.called == 2){
            this.logger.silly(new Date() + ' ' + logModule + ' callThroughWithNetErr called for second time, calling again');
            let res = realCol.findOneAndUpdate(query, update, options)
            console.log('******' + JSON.stringify(res));
            resolve(res);
          }
          else{
            this.logger.error(new Date() + ' ' + logModule + ' called more than twice');
            reject(new Error('called more than twice'));
          }
        });

      }
    }

    let realCol = dal._database.collection('example');;
    let fakeCol = new FakeCol(realCol);

    dal.insertDoc(counterDoc).then((id) => {
      this.logger.debug(new Date() + ' ' + logModule + ' inserted doc with counter');
      counterDoc._id = id;
      
      //Does this *actually* replace the dalExample variable???
      dal.dalExample = fakeCol;

      return dal.incCounter(id);
    })
    .then(() => {
      expect(fakeCol.called).toBe(2);
      return dal.getById(counterDoc._id);
    })
    .then((res) => {
      expect(res.counter).toBe(1);
      dal = new MongoDal('mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=repl0&w=majority', 'silly');
    });

  });

  afterEach((done) => {
    this.logger.silly(new Date() + ' ' + logModule + ' ---after each---');

    dal.deleteAllDocs()
    .then((count) => {
      this.logger.silly(new Date() + ' ' + logModule + ' deleted ' + count  + ' existing docs.');
    })
    .catch((err) => {
      this.logger.error(new Date() + ' ' + logModule + ' error deleting all docs in afterEach: ' + err);
      fail(err);
      done();
    })
    .then(() => {
      this.logger.silly(new Date() + ' ' + logModule + ' ---after each---\n');
      done();
    });
    
  });

});
