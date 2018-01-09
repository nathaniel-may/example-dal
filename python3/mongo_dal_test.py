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


    def testInsertingOneDoc(self):
        self.logger.debug('----------testInsertingOneDoc----------')
        testDoc = {
          'string': 'string value',
          'num': 99,
          'array': [1, 2, 3],
          'subDoc': {'string1': 'str1', 'str2': 'str2'}
        }
        #TODO: Error Handle
        id = self.dal.insertDoc(testDoc)
        self.assertTrue(isinstance(id, ObjectId))
        self.logger.debug('----------testInsertingOneDoc----------\n')

if __name__ == '__main__':
    unittest.main()