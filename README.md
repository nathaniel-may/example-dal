# MongoDB Dal Examples

This project contains simple examples of how to build MonogDB database access layers in Python and NodeJS.

## Building the Local Replica Set

Each example requires a replica set to use features such as read and write concern. Use the following instructions to setup a simple test instance locally.

### Installation

These instructions assume MongoDB is already installed. For instructions on installing MongoDB please see the [installation tutorials](https://docs.mongodb.com/v3.4/installation/#tutorials) in the documentation. Either enterprise or community version 3.4+ is required.

### Starting Nodes
Navigate to the local-mongo/conf directory and execute the following three commands:
```
mongod -f ./mongod0.conf
mongod -f ./mongod1.conf
mongod -f ./mongod2.conf
```

For future startups of the same replica set, use these same three commands and skip the configuration section.

### Common Startup Issues

Look at the configuration files under the local-mongo/conf/ directory and make sure the storage.dbpath and the systemlog.path directories exist and are writable by mongodb. Before starting a node nothing can be occupying the port specified in net.port.

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

The replica set is now running and ready to use once the shell shows primary status:
```
MongoDB Enterprise repl0:PRIMARY>
```

## Javascript: Getting Started

The Javascript example is written using classes, native promises and async/await. Unit tests are written with Jasmine.

To install dependencies with npm, navigate to the javascript directory and run
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

These tests are designed to help developers understand the intended usage of the module and prove the functions work under normal conditions.

## Python: Getting Started

The Python example is written in Python 3. Unit tests are written with the unittest package.

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

To run the unit tests, navigate to the python directory and run
```
python3 ./mongo_dal_test.py
```

These tests are designed to help developers understand the intended usage of the module and prove the functions work under normal conditions.

## Learning Resources 

* [Jesse Davis's Blog](https://emptysqua.re/blog/how-to-write-resilient-mongodb-applications/) - Driver developer at MongoDB
* [Nathaniel May's MDBW17 Talk](https://explore.mongodb.com/developer/nathaniel-may) - Video and slides with code examples

## Authors

* [Nathaniel May](http://nathanielmay.com)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
