//load dependencies
var winston = require('winston');
var ObjectId = require('mongodb').ObjectId;
var MongoError = require('mongodb').MongoError;
var MongoDal = require('../MongoDal').MongoDal;
var MongoDalErrors = require('../MongoDal').Errors;

//define variables
var dal;
var connString = 'mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=repl0&w=majority';

//function to create a logger and wrap the functions with timestamp and module tags
const newLogger = (module, level) => {
  const logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({level: level, colorize: true})
    ]
  });

  const levels = ['silly', 'debug', 'info', 'warn', 'error'];
  for(let level = 0; level<levels.length; level++){
    const fn = logger[levels[level]];
    logger[levels[level]] = str => fn(`${new Date()} ${module} ${str}`);
  }

  return logger;

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
      reject(new MongoDalErrors.DbDuplicateIdError('exampleId'));
    });
  }

  sockExceptCallThrough(fn){
    this.called++;
    return new Promise(async (resolve, reject) => {
      let sockErr = new MongoError();
      sockErr.code = 9001;

      if(this.called == 1){
        await fn();
        reject(sockErr);
      }
      else{
        try{
          await fn();
          resolve();
        }
        catch(err){
          reject(err)
        };
      }
    });
  }
  
};

describe('MongoDal', () => {

  this.logger = newLogger('JAS DAL', 'silly');

  //test documents
  let testDoc = {
      string: 'string value',
      num: 99,
      array: [1, 2, 3],
      subDoc: {string1: 'str1', str2: 'str2'}
    };

  recreateMongoDal = () => {
    try{
      this.dal = new MongoDal(connString, 'silly');
    }
    catch(err){
      this.logger.error(`error creating MongoDal instance: ${err}`);
      throw err;
    }
  }

  connectMongoDal = async () => {
    try{
      await this.dal.connect();
      this.logger.debug(`dal connected`);
    }
    catch(err) {
      this.logger.error(`error attempting to connect MongoDal: ${err}`);
      throw err;
    }
  }


  beforeAll( async done => {
    this.logger.debug(`---beforeAll started---`);

    //create MongoDal
    recreateMongoDal();
    await connectMongoDal();

    this.logger.debug(`---beforeAll completed---
                      `);
    done();
  });

  it('inserts one doc', async done => {
    this.logger.debug(`---inserts one doc---`);
    try{
      const id = await this.dal.insertDoc(testDoc);
      this.logger.debug(`got id: ${id}`);
      expect(id instanceof ObjectId).toBe(true);
    }
    catch(err){
      this.logger.error(`error inserting document: ${err}`);
      fail(err); 
    }

    this.logger.debug(`---inserts one doc---
                      `);
    done();
  });

  it('gets document by id', async done => {
    this.logger.debug(`---gets document by id---`);
    let _id;
    try{
      const id = await this.dal.insertDoc(testDoc);
      this.logger.debug(`inserted doc.`);
      _id = id;
      const doc = await this.dal.getById(id);
      expect(doc._id).toEqual(_id);
      expect(doc.num).toEqual(99);
    }
    catch(err){
      this.logger.error(`error inserting document: ${err}`);
      fail(err); 
    }

    this.logger.debug(`---gets document by id---
                      `);
    done();
  });

  it('counts the collection', async done => {
    this.logger.debug(`---counts the collection---`);
    try{
      const count1 = await this.dal.countCol();
      expect(count1).toBe(0);
      await Promise.all([this.dal.insertDoc({test:0}),
                         this.dal.insertDoc({test:1}),
                         this.dal.insertDoc({test:2})]);
      this.logger.debug(`inserted all 3 docs`);
      const count2 = await this.dal.countCol();
      this.logger.debug(`counted ${count2} docs`);
      expect(count2).toBe(3);
    }
    catch(err){
      this.logger.error(`error counting the collection: ${err}`);
      fail(err); 
    }

    this.logger.debug(`---counts the collection---
                      `);
    done();
  });

  it('deletes all docs', async done => {
    this.logger.debug(`---deletes all docs---`);
    try{
      //insert and expect 3 documents
      await Promise.all([this.dal.insertDoc({test:0}),
                   this.dal.insertDoc({test:1}),
                   this.dal.insertDoc({test:2})]);
      this.logger.debug(`inserted all 3 docs`);
      const count1 = await this.dal.countCol();
      this.logger.debug(`counted ${count1} docs`);
      expect(count1).toBe(3);

      //delete all docs and expect 0
      await this.dal.deleteAllDocs();
      this.logger.debug(`all docs successfully deleted`);
      const count2 = await this.dal.countCol();
      this.logger.debug(`counted ${count2} docs after delete`);
      expect(count2).toBe(0);
    }
    catch(err){
      this.logger.error(`error deleting all docs and counting them: ${err}`);
      fail(err); 
    }

    this.logger.debug(`---deletes all docs---
                      `);
    done();
  });

  it('should retry on network error', async done => {
    this.logger.debug(`---should retry on error---`);

    const callTracker = new CallTracker();
    this.logger.silly(`created callTracker obj`);
    spyOn(callTracker, 'netErr').and.callThrough();

    try{
      const res = await this.dal._retryOnErr(() => callTracker.netErr());
      expect(callTracker.netErr).toHaveBeenCalledTimes(2);
      expect(res).toBe('resolved');
    }
    catch(err){
      this.logger.error(`problem with _retryOnErr. callTracker called ${callTracker.called} times. Err: ${err}`);
      fail(err);
    }

    this.logger.debug(` ---should retry on network error---
                      `);
    done();
  });

  it('does not retry on first duplicate key error', async done => {
    this.logger.debug(`---does not retry on first duplicate key error---`);

    let callTracker = new CallTracker();
    spyOn(callTracker, 'dupKeyErr').and.callThrough();
    this.logger.debug(`created call tracker`);

    try{
      await this.dal._retryOnErr(callTracker.dupKeyErr);
    }
    catch(err){
      this.logger.debug(`caught error ${err}`);
      //since _retryOnErr is being called directly with a mock, 
      //correct error propagation isn't really being enforced here.
      expect(err instanceof MongoDalErrors.DbDuplicateIdError).toBe(true);
      expect(callTracker.dupKeyErr).toHaveBeenCalledTimes(1);
    }

    this.logger.debug(`---does not retry on first duplicate key error---
                      `);
    done();
  });

  it('retries and eats duplicate key error on insert retry', async done => {
    this.logger.debug(`---retries and eats duplicate key error on insert retry---`);

    let callTracker = new CallTracker();
    spyOn(callTracker, 'netErrThenDupKey').and.callThrough();
    this.logger.debug(`created call tracker`);

    try{
      await this.dal._retryOnErr(() => callTracker.netErrThenDupKey());
      this.logger.debug(`no error`);
      expect(callTracker.netErrThenDupKey).toHaveBeenCalledTimes(2);
    }
    catch(err){
      this.logger.error(`error retrying: ${err}`);
      fail();
    }

      this.logger.debug(`---retries and eats duplicate key error on insert retry---
                        `);
      done();
  });

  it('increments a counter', async done => {
    this.logger.debug(`---increments a counter---`);
    let doc = {};
    doc.counter = 0;

    try{
      let id = await this.dal.insertDoc(doc);
      this.logger.debug(`inserted doc with counter`);
      doc._id = id;
      await this.dal.incCounter(id);
      this.logger.debug(`incCounter success`);
      const updatedDoc = await this.dal.getById(doc._id);
      this.logger.debug(`fetched doc by id`);
      expect(updatedDoc.counter).toBe(1);
    }
    catch(err){
      this.logger.error(`error testing incCounter: ${err}`);
      fail();
    }
    
    this.logger.debug(`---increments a counter---
                        `);
    done();
  });

  it('doesnt double count after network error', async done => {
    this.logger.debug(`---doesnt double count after network error---`);
    let counterDoc = {};
    counterDoc.counter = 0;

    let callTracker = new CallTracker();

    class FakeCol{

      constructor(realCol){      
        this.called = 0;
        this.realCol = realCol;
        this.logger = newLogger('JASMOCK', 'silly');

        this.logger.silly(`FakeCol instance created`);
      }

      findOneAndUpdate(query, update, options){
        this.logger.silly(`findOneAndUpdate called`);
        return this.callThroughWithNetErr(query, update, options);
      }

      callThroughWithNetErr(query, update, options){
        return new Promise(async (resolve, reject) => {
          this.called++;

          let sockErr = new MongoError('socketError');
          sockErr.code = 9001;
          
          if(this.called == 1){
            this.logger.silly(`callThroughWithNetErr called for first time`);
            try{
              await realCol.findOneAndUpdate(query, update, options);
              this.logger.silly(`rejecting with socket err`);
              reject(sockErr);
            }
            catch(err){
              this.logger.err(`unexpected error' ${err}`);
              reject(err);
            };
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

    let realCol = this.dal._database.collection('data');;
    let fakeCol = new FakeCol(realCol);

    try{
      const id = await this.dal.insertDoc(counterDoc);
      this.logger.debug(`inserted doc with counter`);
      counterDoc._id = id;
      
      //replace the collection definition with a mockup
      this.dal.dalData = fakeCol;

      await this.dal.incCounter(id);
      expect(fakeCol.called).toBe(2);
      this.logger.debug(`this test wrecked the dal instance. Making a new one`);
      try{
        recreateMongoDal();
        await connectMongoDal();
      }
      catch(err){
        fail(`failed to recreate dal instance: ${err}`);
      }
      this.logger.silly(`new instance created. Finding document to compare count`);
      const doc = await this.dal.getById(counterDoc._id);
      expect(doc.counter).toBe(1)
    }
    catch(err){
      this.logger.error(`err: ${err}`);
      fail();
    }

    this.logger.debug(`---doesnt double count after network error---
                      `);
    done();
  });

  it('throws DbNotConnectedError when not not connected', async done => {
    this.logger.debug(`---throws DbNotConnectedError when not not connected---`);
    try{
      recreateMongoDal();
      const id = await this.dal.insertDoc(testDoc);
      fail('expected DbNotConnectedError');
    }
    catch(err){
      expect(err instanceof MongoDalErrors.DbNotConnectedError).toBe(true);
    }

    //init for cleanup
    try{
      await connectMongoDal();
    }
    catch(err){
      fail(`failed to reconnect dal instance: ${err}`);
    }

    this.logger.debug(`---throws DbNotConnectedError when not not connected---
                      `);
    done();
  });

  it('throws DbAlreadyConnectedError connect called twice', async done => {
    this.logger.debug(`---throws DbAlreadyConnectedError connect called twiced---`);
    try{
      await connectMongoDal();
      const id = await this.dal.insertDoc(testDoc);
      fail('expected DbNotConnectedError');
    }
    catch(err){
      expect(err instanceof MongoDalErrors.DbAlreadyConnectedError).toBe(true);
    }

    this.logger.debug(`---throws DbAlreadyConnectedError connect called twice---
                      `);
    done();
  });

  afterEach( async done => {
    this.logger.silly(`---after each---`);

    try{
      const count = await this.dal.deleteAllDocs();
      this.logger.silly(`deleted ${count} existing docs.`);
    }
    catch(err){
      this.logger.error(`error deleting all docs in afterEach: ${err}`);
      fail(err);
    }

    this.logger.silly(`---after each---
                      `);
    done();
  });

});
