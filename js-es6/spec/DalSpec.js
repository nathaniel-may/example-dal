//load dependencies
var winston = require('winston');
var ObjectId = require('mongodb').ObjectId;
var MongoError = require('mongodb').MongoError;
var MongoDal = require('../MongoDal');

//define variables
var dal;
var connString = 'mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=repl0&w=majority';
this.logModule = 'JAS RT '

//std out logging settings
this.logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({colorize: true})
  ]
});
this.logger.level = 'silly';

//wrap the log functions with timestamp and module tags
const levels = ['silly', 'debug', 'info', 'warn', 'error'];
for(let level = 0; level<levels.length; level++){
  const fn = this.logger[levels[level]];
  this.logger[levels[level]] = str => fn(`${new Date()} ${this.logModule} ${str}`);
}

class CallTracker{

  constructor(){
    this.called = 0;
  }

  netErr(){
    this.called++;
    return new Promise((resolve, reject) => {
      let err = new MongoError('socketException');
      err.code = 9001;

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
      let netErr = new MongoError('socketException');
      netErr.code = 9001; //socket exception

      let dupKeyErr = new MongoError('duplicateKeyError');
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
      let dupKeyErr = new MongoError('duplicateKeyError');
      dupKeyErr.code = 11000;
      reject(dupKeyErr);
    })
  }

  sockExceptCallThrough(fn){
    this.called++;
    return new Promise((resolve, reject) => {
      let err = new MongoError();
      err.code = 9001;

      if(this.called == 1){
        fn().then(() => {
          reject(err);
        })
        .catch(err => {
          reject();
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
  this.logModule = 'JAS DAL';

  //test documents
  let testDoc = {
      string: 'string value',
      num: 99,
      array: [1, 2, 3],
      subDoc: {string1: 'str1', str2: 'str2'}
    };

  beforeAll(done => {
    this.logger.debug(`---beforeAll started---`);

    //create MongoDal
    try{
      dal = new MongoDal(connString, 'silly');
    }
    catch(err){
      this.logger.error(`error creating MongoDal instance: ${err}`);
      fail(err);
      done(); return;
    }

    //setup the this.logger
    dal.init().then(() => this.logger.debug(`dal init completed`))
    .catch(err => {
      this.logger.error(`error attempting to init MongoDal: ${err}`);
      fail();
    })
    .then(() => {
      this.logger.debug(`---beforeAll completed---
                        `);
      done();
    });

  });

  it('inserts one doc', done => {
    this.logger.debug(`---inserts one doc---`);
    dal.insertDoc(testDoc).then(id => {
      this.logger.debug(`got id: ${id}`);
      expect(id instanceof ObjectId).toBe(true);
    })
    .catch(err => {
      this.logger.error(`error inserting document: ${err}`);
      fail(err); 
    })
    .then(() => {
      this.logger.debug(`---inserts one doc---
                        `);
      done();
    });
  });

  it('gets document by id', done => {
    this.logger.debug(`---gets document by id---`);
    let _id;
    dal.insertDoc(testDoc).then(id => {
      this.logger.debug(`inserted doc.`);
      _id = id;
      return dal.getById(id);
    })
    .then(doc => {
      expect(doc._id).toEqual(_id);
      expect(doc.num).toEqual(99);
    })
    .catch(err => {
      this.logger.error(`error inserting document: ${err}`);
      fail(err); 
    })
    .then(() => {
      this.logger.debug(`---gets document by id---
                        `);
      done();
    })
  });

  it('counts the collection', done => {
    this.logger.debug(`---counts the collection---`);
    dal.countCol().then(count => {
      expect(count).toBe(0);
      return Promise.all([dal.insertDoc({test:0}),
                          dal.insertDoc({test:1}),
                          dal.insertDoc({test:2})]);
    })
    .then(() => {
      this.logger.debug(`inserted all 3 docs`);
      return dal.countCol();
    })
    .then(count => {
      this.logger.debug(`counted ${count} docs`);
      expect(count).toBe(3);
    })
    .catch(err => {
      this.logger.error(` error counting the collection: ${err}`);
      fail(err); 
    })
    .then(() => {
      this.logger.debug(`---counts the collection---
                        `);
      done();
    });
  });

  it('deletes all docs', done => {
    this.logger.debug(`---deletes all docs---`);
    Promise.all([dal.insertDoc({test:0}),
                 dal.insertDoc({test:1}),
                 dal.insertDoc({test:2})])
    .then(() => {
      this.logger.debug(`inserted all 3 docs`);
      return dal.countCol();
    })
    .then(count => {
      this.logger.debug(`counted ${count} docs`);
      expect(count).toBe(3);
      return dal.deleteAllDocs();
    })
    .then(() => {
      this.logger.debug(`all docs successfully deleted`);
      return dal.countCol();
    })
    .then(count => {
      this.logger.debug(`counted ${count} docs after delete`);
      expect(count).toBe(0);
    })
    .catch(err => {
      this.logger.error(`error deleting all docs and counting them: ${err}`);
      fail(err); 
    })
    .then(() => {
      this.logger.debug(`---deletes all docs---
                        `);
      done();
    });
  });

  it('should retry on network error', done => {
    this.logger.debug(`---should retry on error---`);

    let callTracker = new CallTracker();
    this.logger.silly(`created callTracker obj`);
    spyOn(callTracker, 'netErr').and.callThrough();

    dal._retryOnErr(() => callTracker.netErr()).then(res => {
      expect(callTracker.netErr).toHaveBeenCalledTimes(2);
      expect(res).toBe('resolved');
    })
    .catch(err => {
      this.logger.error(`problem with _retryOnErr. callTracker called ${callTracker.called} times. Err: ${err}`);
      fail(err);
    })
    .then(() => {
      this.logger.debug(` ---should retry on network error---
                        `);
      done();
    });
  });

  it('does not retry on first duplicate key error', done => {
    this.logger.debug(`---does not retry on first duplicate key error---`);

    let callTracker = new CallTracker();
    spyOn(callTracker, 'dupKeyErr').and.callThrough();
    this.logger.debug(`created call tracker`);

    dal._retryOnErr(() => callTracker.dupKeyErr())
    .catch(err => {
      this.logger.debug(`caught error ${err}`);
      expect(err.code).toBe(11000); //expect the duplicate key error
      expect(callTracker.dupKeyErr).toHaveBeenCalledTimes(1);
    })
    .then(() => {
      this.logger.debug(`---does not retry on first duplicate key error---
                        `);
      done();
    });
  });

  it('retries and eats duplicate key error on insert retry', done => {
    this.logger.debug(`---retries and eats duplicate key error on insert retry---`);

    let callTracker = new CallTracker();
    spyOn(callTracker, 'netErrThenDupKey').and.callThrough();
    this.logger.debug(`created call tracker`);

    dal._retryOnErr(() => callTracker.netErrThenDupKey())
    .then(() => {
      this.logger.debug(`no error`);
      expect(callTracker.netErrThenDupKey).toHaveBeenCalledTimes(2);
    })
    .catch(err => {
      this.logger.error(`error retrying: ${err}`);
      fail();
    })
    .then(() => {
      this.logger.debug(`---retries and eats duplicate key error on insert retry---
                        `);
      done();
    });
  });

  it('increments a counter', done => {
    this.logger.debug(`---increments a counter---`);
    let doc = {};
    doc.counter = 0;

    dal.insertDoc(doc).then(id => {
      this.logger.debug(`inserted doc with counter`);
      doc._id = id;
      return dal.incCounter(id);
    })
    .then(() => {
      this.logger.debug(`incCounter success`);
      return dal.getById(doc._id);
    })
    .then(doc => {
      this.logger.debug(`fetched doc by id`);
      expect(doc.counter).toBe(1);
    })
    .catch(err => {
      this.logger.error(`error testing incCounter: ${err}`);
      fail();
    })
    .then(() => {
      this.logger.debug(`---increments a counter---
                        `);
      done();
    });
  });

  it('doesnt double count after network error', done => {
    this.logger.debug(`---doesnt double count after network error---`);
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

        this.logger.silly(`FakeCol instance created`);
      }

      findOneAndUpdate(query, update, options){
        this.logger.silly(`findOneAndUpdate called`);
        return this.callThroughWithNetErr(query, update, options);
      }

      callThroughWithNetErr(query, update, options){
        return new Promise((resolve, reject) => {
          this.called++;

          let sockErr = new MongoError('socketError');
          sockErr.code = 9001;
          
          if(this.called == 1){
            this.logger.silly(`callThroughWithNetErr called for first time`);
            realCol.findOneAndUpdate(query, update, options).then(() => {
              this.logger.silly(`rejecting with socket err`);
              reject(sockErr);
            })
            .catch(err => {
              this.logger.err(`unexpected error' ${err}`);
              reject(err);
            });
          }
          else if(this.called == 2){
            this.logger.silly(`callThroughWithNetErr called for second time, calling again`);
            let res = realCol.findOneAndUpdate(query, update, options);
            resolve(res);
          }
          else{
            this.logger.error(`called more than twice`);
            reject(new Error('called more than twice'));
          }
        });
      }
    }

    let realCol = dal._database.collection('data');;
    let fakeCol = new FakeCol(realCol);

    dal.insertDoc(counterDoc).then(id => {
      this.logger.debug(`inserted doc with counter`);
      counterDoc._id = id;
      
      //replace the collection definition with a mockup
      dal.dalData = fakeCol;

      return dal.incCounter(id);
    })
    .then(() => {
      expect(fakeCol.called).toBe(2);
      this.logger.debug(`this test wrecked the dal instance. Making a new one`);
      dal = new MongoDal(connString, 'silly');
      return dal.init();
    })
    .then(() => {
      this.logger.silly(`new instance created. Finding document to compare count`);
      return dal.getById(counterDoc._id);
    })
    .then(res => expect(res.counter).toBe(1))
    .catch(err => this.logger.error(`err: ${err}`))
    .then(() => {
      this.logger.debug(`---doesnt double count after network error---
                        `);
      done();
    });

  });

  afterEach(done => {
    this.logger.silly(`---after each---`);

    dal.deleteAllDocs()
    .then(count => this.logger.silly(`deleted ${count} existing docs.`))
    .catch(err => {
      this.logger.error(`error deleting all docs in afterEach: ${err}`);
      fail(err);
      done();
    })
    .then(() => {
      this.logger.silly(`---after each---
                        `);
      done();
    });
    
  });

});
