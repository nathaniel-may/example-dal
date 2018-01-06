from pymongo import MongoClient
from bson.objectid import ObjectId

class MongoDal:
	'''example functions for accessing mongodb'''

	def __init__(self, connStr, logLvl):
		global connString
		connString = connStr
		global logLevel
		logLevel = logLvl
		global client
		client = None

	def init(self):
		global client
		if(client is None):
			global connString
			client = MongoClient(connString)
		db = client.dalExample
		#create all collections
		global dalExample
		dalExample = db.example

	def insertDoc(self, doc):
		id = ObjectId()
		doc.update({'_id': id})
		#TODO RETRY and ERROR HANDLE
		global dalExample
		dalExample.insert(doc)
		return id;
