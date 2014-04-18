var utils = require('utilities')
  , assert = require('assert')
  , model = require('../../../../lib')
  , helpers = require('.././helpers')
  , Adapter = require('../../../../lib/adapters/azureblob').Adapter
  , adapter
  , tests
  , config = require('../../../config')
  , shared = require('../shared')
  , unique = require('../unique_id');

tests = {
  'before': function (next) {
    var relations = helpers.fixtures.slice()
      , models = [];
    adapter = new Adapter();

    adapter.once('connect', function () {
      adapter.createTable(relations, next);
    });
    adapter.connect();

    model.adapters = {};
    relations.forEach(function (r) {
      model[r].adapter = adapter;
      models.push({
        ctorName: r
      });
    });

    model.registerDefinitions(models);

  }

, 'after': function (next) {
    var relations = helpers.fixtures.slice();
    adapter.dropTable(relations, function () {
      adapter.disconnect(function (err) {
        if (err) { throw err; }
        next();
      });
    });
  }

, 'test create adapter': function () {
    assert.ok(adapter instanceof Adapter);
  }
};

for (var p in shared) {
  if (p == 'beforeEach' || p == 'afterEach') {
    tests[p] = shared[p];
  }
  else {
    tests[p + ' (AzureBlob)'] = shared[p];
  }
}

for (var p in unique) {
  tests[p + ' (AzureBlob)'] = unique[p];
}

module.exports = tests;

