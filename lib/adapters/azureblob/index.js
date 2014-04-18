var model = require('../../index')
  , utils = require('utilities')
  , azure
  , mr = require('../transformers/mr')
  , operation = require('../../query/operation')
  , comparison = require('../../query/comparison')
  , datatypes = require('../../datatypes')
  , request = utils.request
  , BaseAdapter = require('../base_adapter').BaseAdapter
  , _data = {}
  , delimiter = '!';

// DB lib should be locally installed in the consuming app
azure = utils.file.requireLocal('azure', model.localRequireError);
// azure = require('azure');

var _comparisonTypeMap = {
    '=': 'eq'
  , '!=': 'ne'
  , '>': 'gt'
  , '<': 'lt'
  , '>=': 'ge'
  , '<=': 'le'
};


function cloneWithKeys(obj) {
  "use strict";
  var clone;
  if(obj.toJSON) {
    clone = JSON.parse(JSON.stringify(obj.toJSON()));
  } else {
    clone = JSON.parse(JSON.stringify(obj));
  }
  if (clone.RowKey === undefined) {
    clone.RowKey = obj.id || utils.string.uuid();
    clone.PartitionKey = obj.type;
    obj.id = clone.RowKey;
  }
  return clone;
}

var Adapter = function (options) {
  "use strict";
  var opts = options || {};

  this.name = 'azureblob';
  this.config = { };
  this.client = null;
  this.tableService = null;

  utils.mixin(this.config, opts);

  this.init.apply(this, arguments);
};

Adapter.prototype = new BaseAdapter();
Adapter.prototype.constructor = Adapter;

utils.mixin(Adapter.prototype, new (function () {
  "use strict";

  this.init = function () {
    // It's expected that the user will set AZURE_STORAGE_ACCOUNT
    // and AZURE_STORAGE_ACCESS_KEY environment variables. If they
    // don't want to then they can set it in the config
    var config = this.config;
    if (config.storageAccount) {
      this.tableService = azure.createTableService(config.storageAccount, config.storageAccessKey);
    } else {
      this.tableService = azure.createTableService();
    }
    var retryOperations = new azure.ExponentialRetryPolicyFilter();
    this.tableService.withFilter(retryOperations);
  };

  this.transformConditions = function (azureQuery, conditions) {
    return this.transformOperation(azureQuery, conditions);
  };

  this.transformOperation = function (azureQuery, op) {
    var self = this;
    if (!op.isEmpty()) {
      if (op.type == 'not') {
        throw new Error('"not" clause is not supported')
      }
      else {
        // 'and' or 'or', ignore 'null' for now
        op.forEach(function (o) {
          if (o instanceof operation.OperationBase) {
            self.transformOperation(azureQuery, o);
          }
          else {
            self.transformComparison(azureQuery, o);
          }
        });
      }
    }
    return;
  };

  this.transformComparison = function (azureQuery, comp) {
    var startsWithWildcard
      , endsWithWildcard
      , val = comp.value
      , field = comp.field;

    if (field == 'id') {
      field = 'RowKey';
    }

    if (val instanceof Date) {
      val = val.toISOString();
    }

    if(comp instanceof comparison.LikeComparison) {
      startsWithWildcard = val.charAt(0) == '%';
      endsWithWildcard = val.charAt(val.length - 1) == '%';
      var str = val.substring(
        startsWithWildcard ? 1 : 0,
        val.length - (endsWithWildcard ? 2 : 1));
      if(startsWithWildcard && endsWithWildcard) {
        azureQuery.and('contains(?, ' + field + ')', str);
      }
      else if(startsWithWildcard) {
        azureQuery.and('endswith(' + field + ', ?)', str);
      }
      else if(endsWithWildcard) {
        azureQuery.and('startswith(' + field + ', ?)', str);
      }
      else {
        azureQuery.and(field + ' eq ?', str);
      }
    }
    else if(comp instanceof comparison.InclusionComparison) {
      if (!Array.isArray(val)) {
        return;
      }
      azureQuery.and('(');
      azureQuery.where(field + " eq ?", val[0]);
      for(var i=1,ii=val.length; i<ii; i++) {
        azureQuery.or(field + " eq ?", val[i]);
      }
      azureQuery.where(')');
    }
    else if(val !== undefined) {
      var symbol = _comparisonTypeMap[comp.sqlComparatorString];
      azureQuery.and(field + " " + symbol + " ?", val);
    } else {
      console.log("val was undefined for %j", field);
      // console.log(comp);
    }
  };

  this.load = function (query, callback) {
    var tableService = this.tableService;
    var tableName = query.model.modelName;
    var id = query.byId;
    var conditions = query.conditions;
    var limit = query.opts.limit;

    function createModel(entity) {
      var reg = model.descriptionRegistry[tableName];
      Object.keys(reg.properties).forEach(function(propertyDefName) {
        var propertyDef = reg.properties[propertyDefName];
        if (propertyDef.datatype == 'datetime') {
          entity[propertyDef.name] = Date.parse(entity[propertyDef.name]);
        }
      });
      var inst = query.model.create(entity, {scenario: query.opts.scenario});
      inst.RowKey = entity.id;
      inst.PartitionKey = tableName;
      if(undefined === inst.id) {
        inst.id = entity.RowKey;
      }
      inst._saved = true;
      return inst;
    }

    if (id) {
      tableService.queryEntity(tableName, tableName, id, function (err, entity) {
        if (err) {
          if(err.statusCode && err.statusCode === 404) {
            return callback(null, (limit === 1) ? undefined : []);
          }
          return callback(err, null);
        }
        var inst = createModel(entity);

        if (limit === 1) {
          callback(null, inst);
        }
        else {
          callback(null, [inst]);
        }
      });
    } else {
      var azureQuery = azure.TableQuery.select().from(tableName).where("PartitionKey eq ?", tableName);
      if(!conditions.isEmpty()) {
        this.transformConditions(azureQuery, conditions);
        // sort = this.transformSortOrder(query.opts.sort);
        if(limit != 1) {
          azureQuery.top(limit);
        }
      }
      tableService.queryEntities(azureQuery, function (err, entities) {
        if (err) {
          return callback(err, null);
        }
        var instances = entities.map(createModel);
        callback(null, limit == 1 ? instances[0] : instances);
      });
    }
  };

  this.update = function (data, query, callback) {
    var tableService = this.tableService;
    var tableName = query.model.modelName;
    var id = query.byId;

    if (id) {
      var clone = cloneWithKeys(data);
      tableService.updateEntity(tableName, clone, function (err) {
        if (err) {
          return callback(err, null);
        }
        if (data instanceof model.ModelBase) {
          data._saved = true;
          callback(null, data);
        }
        else {
          callback(null, data);
        }
      });
    }
    else {
      callback(new Error('Bulk update is not supported'), null);
    }
  };

  this.remove = function (query, callback) {
    var tableService = this.tableService;
    var tableName = query.model.modelName;
    var id = query.byId;
    var ids = [];

    function remove() {
      var id = ids.shift();
      var obj = { PartitionKey: tableName, RowKey: id };
      tableService.deleteEntity(tableName, obj, function (err) {
        if (err) {
          return callback(err, null);
        }
        if (ids.length === 0) {
          return callback(null, true);
        }
        remove();
      });
    }

    // single id
    if (id) {
      ids.push(id);
      return remove();
    }
    // Remove via query
    // We have a list of ids
    if (Array.isArray(query.rawConditions.id)) {
      // Make a copy; going to be pulling shit off to iterate
      ids = query.rawConditions.id.slice();
      return remove();
    }
    // Do a fetch to get the matching items -- this is like, anti-optimal
    this.load(query, function (err, items) {
      if (err) {
        return callback(err, null);
      }
      ids = items.map(function (item) { return item.id || item.RowKey; });
      return remove();
    });
  };

  this.insert = function (data, opts, callback) {
    var items = Array.isArray(data) ? data : [data];
    var tableService = this.tableService;
    var tableName = items[0].type;
    var thingsToInsert = items.map(cloneWithKeys);

    function insert() {
      var item = thingsToInsert.shift();
      var clone = cloneWithKeys(item);
      tableService.insertEntity(tableName, clone, function (err) {
        if (err) {
          return callback(err, null);
        }
        if (item instanceof model.ModelBase) {
          item._saved = true;
          if(thingsToInsert.length === 0) {
            return callback(null, items);
          }
        }
        if(thingsToInsert.length === 0) {
          return callback(null, items);
        }
        insert();
      });
    }
    insert();
  };

  this.createTable = function (names, callback) {
    var tableNames = Array.isArray(names) ? names.slice() : [names];
    var tableService = this.tableService;
    function create() {
      var name = tableNames.shift();
      tableService.createTableIfNotExists(name, function(error){
        if (error) {
          return callback(error);
        }
        if (tableNames.length === 0) {
          return callback(null, true);
        }
        create();
      });
    }
    create();
  };

  this.dropTable = function (names, callback) {
    var tableNames = Array.isArray(names) ? names.slice() : [names];
    var tableService = this.tableService;

    function drop() {
      var name = tableNames.shift();
      tableService.deleteTable(name, function(error){
        if (error) {
          return callback(error);
        }
        if (tableNames.length === 0) {
          return callback(null, true);
        }
        drop();
      });
    }
    drop();
  };

})());

// utils.mixin(Adapter.prototype, mr);

module.exports.Adapter = Adapter;

