from mongo_dal import MongoDal
from bson.objectid import ObjectId
import unittest

class MongoDalTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global dal
        dal = MongoDal('mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=repl0&w=majority', 'silly')
        dal.init()

    def testInsertingOneDoc(self):
        testDoc = {
          'string': 'string value',
          'num': 99,
          'array': [1, 2, 3],
          'subDoc': {'string1': 'str1', 'str2': 'str2'}
        }
        #TODO: Error Handle
        global dal
        id = dal.insertDoc(testDoc)
        self.assertTrue(isinstance(id, ObjectId))

if __name__ == '__main__':
    unittest.main()