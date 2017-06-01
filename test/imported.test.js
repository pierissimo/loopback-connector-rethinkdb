describe('rethinkdb imported features', function () {
  this.timeout(60000);
  before(function () {
    require('./init.js');
  });

  /* TODO:
   * - changes to the data juggler to include rethinkdb
   * - implements methods required to run common.batch.js test
   */
  
  //require('loopback-datasource-juggler/test/common.batch.js');
  //require('loopback-datasource-juggler/test/default-scope.test.js');
  require('./default-scope.test.js');
  require('loopback-datasource-juggler/test/include.test.js');

});
