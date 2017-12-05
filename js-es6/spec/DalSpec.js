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
    let doc = {
      string: 'string value',
      num: 10,
      array: [1, 2, 3],
      subDoc: {string1: 'str1', str2: 'str2'}
    };
    dal.insertDoc(doc).then((id) => {
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
    dal.getById().then((id) => {
      winston.debug(new Date() + ' ' + logModule + ' got id: ' + id);
      expect(id instanceof ObjectId).toBe(true);
      winston.debug(new Date() + ' ' + logModule + ' ---gets document by id---\n');
      done();
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error getting document: ' + err);
      fail(err); 
      done();
    })
  });

  afterEach((done) => {
    winston.silly(new Date() + ' ' + logModule + ' ---after each---');
    mongoDal.deleteAllDocs()
    .then((count) => {
      winston.silly(new Date() + ' ' + logModule + ' deleted ' + count  + ' existing docs.');
      winston.silly(new Date() + ' ' + logModule + ' ---after each---\n');
      done();
    })
    .catch((err) => {
      winston.error(new Date() + ' ' + logModule + ' error deleting all logs in afterEach: ' + err + '\n');
      fail(err);
      done();
    });
    
  });

});
