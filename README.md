# MongoDB Dal Examples

This project contains simple examples of how to build database access layers in several languages.

## Building the Local Replica Set

Each of the examples requires a replica set to use features such as read and write concern. Each example will have a connection string that can be changed to point to an existing replica set, or the following instructions can be used to setup a simple test instance locally.

### Installation

These instructions assume MongoDB is already installed. For instructions on installing MongoDB please see the [installation tutorials](https://docs.mongodb.com/v3.4/installation/#tutorials) in the documentation.

### Configuration Files

Look at the configuration files under the local-mongo/conf/ directory and make sure the storage.dbpath and the systemlog.path directories exist and are writable by mongodb. Before starting a node nothing can be occupying the port specified in net.port.

### Starting Nodes
Navigate to the local-mongo/conf directory and execute the following three commands:
```
mongod -f ./mongod0.conf
mongod -f ./mongod1.conf
mongod -f ./mongod2.conf
```

Common startup errors are addressed in the configuration files section above.

For future startups of the same replica set, use these same three commands and skip the following section.

### Configuring the Replica set

This set of instructions only needs to be followed the first time this replica set starts.

Connect to one of the three instances with a mongo shell using a command like the following:
```
mongo
```

Initiate the replica set with this command:
```
rs.initiate(
   {
      _id: "repl0",
      version: 1,
      members: [
         { _id: 0, host : "localhost:27017" },
         { _id: 1, host : "localhost:27018" },
         { _id: 2, host : "localhost:27019" }
      ]
   }
)
```

The replica set is now running and ready to use. 


## Javascript: Getting Started

The Javascript example is written using ES6 classes and native promises.

To install dependencies with npm, navigate to the js-es6 directory and run
```
npm install
```

Before running the tests, be sure a replica set is up with nodes running at the following addresses:
```
localhost:27017
localhost:27018
localhost:27019
``` 

To run the Jasmine unit tests run
```
npm test
```

There are several types of tests in the spec directory. Simple unit tests are under DalSpec.js and optional rudimentary load tests are in LoadSpex.js. 

To run the load tests, first change the name of the file
```
mv spec/LoadSpex.js spec/LoadSpec.js
```
then run
```
npm test
```
to run both files

These tests are simple and are only designed to prove the functions work under normal conditions and to guide developers when attempting to make changes to the example functions.

## Python: Getting Started

The Python example is written in Python 3 and is currently in the early stages of development. Unit tests are written with the unittest package.

To install the mongodb driver run 
```
python3 -m pip install pymongo
```

Before running the tests, be sure a replica set is up with nodes running at the following addresses:
```
localhost:27017
localhost:27018
localhost:27019
``` 

To run the unit tests run
```
python3 ./mongo_dal_test.py
```

## Learning Resources 

* [Jesse Davis's Blog](https://emptysqua.re/blog/how-to-write-resilient-mongodb-applications/) - Driver developer at MongoDB
* [Nathaniel May's MDBW17 Talk](https://explore.mongodb.com/developer/nathaniel-may) - Video and slides with code examples

## Authors

* **Nathaniel May** - [Maybe I'll put up my website one day](http://nathanielmay.com)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
