module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = {
        host: "localhost",
        port: 28015,
        db: "test"
    };

/*if (process.env.CI) {
  config = {
    host: 'localhost',
    database: 'lb-ds-mongodb-test-' + (
      process.env.TRAVIS_BUILD_NUMBER || process.env.BUILD_NUMBER || '1'
    ),
  };
}*/

process.env.TESTING = process.env.CI || process.env.NODE_ENV;

global.getDataSource = global.getSchema = function (customConfig) {
  var db = new DataSource(require('../'), customConfig || config);
  //var db = new DataSource(require('loopback-datasource-juggler/lib/connectors/memory'));
  db.log = function (a) {
    console.log(a);
  };

  return db;
};
