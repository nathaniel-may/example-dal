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

  xit('deletes all docs', (done) => {



    fail(new Error('test not completed'));
    done();
  });

  // afterEach((done) => {
  //   winston.silly(new Date() + ' ' + logModule + ' ---after each---');
  //   mongoDal.deleteAllDocs()
  //   .then((count) => {
  //     winston.silly(new Date() + ' ' + logModule + ' deleted ' + count  + ' existing docs.');
  //     winston.silly(new Date() + ' ' + logModule + ' ---after each---\n');
  //     done();
  //   })
  //   .catch((err) => {
  //     winston.error(new Date() + ' ' + logModule + ' error deleting all logs in afterEach: ' + err + '\n');
  //     fail(err);
  //     done();
  //   });
    
  // });

});
