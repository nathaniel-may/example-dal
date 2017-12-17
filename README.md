# MongoDB Dal Examples

This project contains simple examples of how to build database access access layers in several languages.

### Javascript: Getting Started

The Javascript example is written using ES6 classes and native promises.

To install dependencies with npm, navigate to the js-es6 directory and run
```
npm install
```

Before running the tests, set up the required replica set with nodes running at the following addresses by following the documentation for [deploying a replica set](https://docs.mongodb.com/v3.4/tutorial/deploy-replica-set/)
```
localhost:27017
localhost:27018
localhost:27019
``` 

To run the Jasmine tests run
```
npm test
```
The tests are only simple unit tests designed to prove the functions work and to guide developers when attempting ot make changes to the example.

## Learning Resources 

* [Jesse Davis's Blog](https://emptysqua.re/blog/how-to-write-resilient-mongodb-applications/) - Driver developer at MongoDB
* [Nathaniel May's MDBW17 Talk](https://explore.mongodb.com/developer/nathaniel-may) - Video and slides with code examples

## Authors

* **Nathaniel May** - [Maybe I'll put up my website one day](https://nathanielmay.com)

## License

TODO: Figure out the licensing
