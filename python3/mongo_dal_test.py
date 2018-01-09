from mongo_dal import MongoDal
from bson.objectid import ObjectId
from datetime import datetime
import unittest
import logging

class MongoDalTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        #logging
        cls.logger = logging.getLogger('DAL TEST')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s\t%(name)s\t%(levelname)s::%(message)s', datefmt='%m/%d/%Y %H:%M:%S')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

        #connect to MongoDB
        cls.logger.debug('----------setUpClass----------')
        cls.dal = MongoDal('mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=repl0&w=majority', logging.DEBUG)
        cls.dal.init()
        cls.logger.debug('----------setUpClass----------\n')

    @classmethod
    def tearDownClass(cls):
        cls.dal.close()

    def tearDown(self):
        self.logger.debug('----------tearDown----------')
        self.dal.delete_all_docs()
        self.logger.debug('----------tearDown----------\n')


    def testInsertingOneDoc(self):
        self.logger.debug('----------testInsertingOneDoc----------')
        testDoc = {
          'string': 'string value',
          'num': 99,
          'array': [1, 2, 3],
          'subDoc': {'string1': 'str1', 'str2': 'str2'}
        }
        #TODO: Error Handle
        id = self.dal.insert_doc(testDoc)
        self.assertTrue(isinstance(id, ObjectId))
        self.logger.debug('----------testInsertingOneDoc----------\n')

    def testGetById(self):
        #self.skipTest('not implemented yet')
        self.logger.debug('----------testGetById----------')
        testDoc = {'test': True}
        id = self.dal.insert_doc(testDoc)
        doc = self.dal.get_by_id(id)
        self.assertEqual(id, testDoc['_id'])
        self.assertEqual(doc['test'], testDoc['test'])
        self.logger.debug('----------testGetById----------\n')

    def testIncCounter(self):
        self.skipTest('not implemented yet')
        self.logger.debug('----------testIncCounter----------')
        testDoc = {counter: 0}
        id = self.dal.insert_doc(testDoc)
        self.dal.inc_counter()
        doc = self.dal.get_by_id(id)
        self.assertEquals(1, doc.counter)
        self.logger.debug('----------testIncCounter----------\n')

if __name__ == '__main__':
    unittest.main()