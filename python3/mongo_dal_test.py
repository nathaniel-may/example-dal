from mongo_dal import MongoDal
from mongo_dal import DbDuplicateIdError
from bson.objectid import ObjectId
from datetime import datetime
import unittest
import logging


class MongoDalTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # logging
        cls.logger = logging.getLogger('DAL TEST')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s\t%(name)s\t%(levelname)s::%(message)s', datefmt='%m/%d/%Y %H:%M:%S')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

        # connect to MongoDB
        cls.logger.debug('----------setUpClass----------')
        cls.dal = MongoDal('mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=repl0&w=majority',
                           logging.DEBUG)
        cls.dal.connect()
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
        test_doc = {
          'string': 'string value',
          'num': 99,
          'array': [1, 2, 3],
          'subDoc': {'string1': 'str1', 'str2': 'str2'}
        }
        # TODO: Error Handle
        _id = self.dal.insert_doc(test_doc)
        self.assertTrue(isinstance(_id, ObjectId))
        self.logger.debug('----------testInsertingOneDoc----------\n')

    def testRejectDuplicateKeys(self):
        self.logger.debug('----------testRejectDuplicateKeys----------')
        test_doc1 = {'test': True}
        id1 = self.dal.insert_doc(test_doc1)
        test_doc2 = {'_id': id1, 'test': True}
        try:
            self.dal.insert_doc(test_doc2)
        except Exception as e:
            self.logger.debug('successfully caught error while inserting with duplicate key: %s', e)
            self.assertTrue(True)
        else:
            self.fail('should reject if inserting duplicate keys')
        self.logger.debug('----------testRejectDuplicateKeys----------\n')

    def testGetById(self):
        self.logger.debug('----------testGetById----------')
        test_doc = {'test': True}
        _id = self.dal.insert_doc(test_doc)
        self.logger.debug('inserted doc')
        doc = self.dal.get_by_id(_id)
        self.logger.debug('read doc')
        self.assertEqual(_id, doc['_id'])
        self.assertEqual(doc['test'], test_doc['test'])
        self.assertEqual({*doc}, {'_id', 'test'})
        self.logger.debug('----------testGetById----------\n')

    def testIncCounter(self):
        self.logger.debug('----------testIncCounter----------')
        test_doc = {'counter': 0}
        _id = self.dal.insert_doc(test_doc)
        self.dal.inc_counter(_id)
        self.dal.inc_counter(_id)
        doc = self.dal.get_by_id(_id)
        self.assertEqual(2, doc['counter'])
        self.logger.debug('----------testIncCounter----------\n')

    def testRaisesDuplicateIdError(self):
        self.logger.debug('----------testRaisesDuplicateIdError----------')
        test_doc = {'test': True}
        id = self.dal.insert_doc(test_doc)
        test_doc2 = {'_id': id, 'test': True}
        self.assertRaises(DbDuplicateIdError, self.dal.insert_doc, test_doc2)

    def testNewTestCase(self):
        self.skipTest('not implemented yet')


if __name__ == '__main__':
    unittest.main()
