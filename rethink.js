/* jshint sub: true */
var r = require("rethinkdb");
var moment = require("moment");
var async = require("async");
var _ = require("lodash-node");
var util = require("util");
var Rx = require('rx');

var Connector = require("loopback-connector").Connector;

exports.initialize = function initializeDataSource(dataSource, callback) {
    if (!r) return;

    var s = dataSource.settings;

    if (dataSource.settings.rs) {

        s.rs = dataSource.settings.rs;
        if (dataSource.settings.url) {
            var uris = dataSource.settings.url.split(',');
            s.hosts = [];
            s.ports = [];
            uris.forEach(function(uri) {
                var url = require('url').parse(uri);

                s.hosts.push(url.hostname || 'localhost');
                s.ports.push(parseInt(url.port || '28015', 10));

                if (!s.database) s.database = url.pathname.replace(/^\//, '');
                if (!s.username) s.username = url.auth && url.auth.split(':')[0];
                if (!s.password) s.password = url.auth && url.auth.split(':')[1];
            });
        }

        s.database = s.database || 'test';

    } else {

        if (dataSource.settings.url) {
            var url = require('url').parse(dataSource.settings.url);
            s.host = url.hostname;
            s.port = url.port;
            s.database = (url.pathname || '' ).replace(/^\//, '');
            s.username = url.auth && url.auth.split(':')[0];
            s.password = url.auth && url.auth.split(':')[1];
        }

        s.host = s.host || 'localhost';
        s.port = parseInt(s.port || '28015', 10);
        s.database = s.database || 'test';

    }

    s.safe = s.safe || false;

    dataSource.adapter = new RethinkDB(s, dataSource);
    dataSource.connector = dataSource.adapter

    if (callback) {
        dataSource.connector.connect(callback);
    }

    process.nextTick(callback);
};

function RethinkDB(s, dataSource) {
    Connector.call(this, "rethink", s);
    this.dataSource = dataSource;
    this.database = s.database;
}

util.inherits(RethinkDB, Connector);

RethinkDB.prototype.connect = function(cb) {
    var self = this
    var s = self.settings
    if (self.db) {
        process.nextTick(function () {
            cb && cb(null, self.db);
        });
    } else {
        var cOpts = Object.assign({host: s.host, port: s.port, authKey: s.password}, s.additionalSettings);
        if (cOpts.ssl && cOpts.ssl.ca) {
            //check if is a base64 encoded string
            if(/^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$/.test(cOpts.ssl.ca)){
                cOpts.ssl.ca = Buffer.from(cOpts.ssl.ca, 'base64');
            }
        }
        r.connect(cOpts, function (error, client) {
            self.db = client
            cb && cb(error, client)
        });
    }
};

RethinkDB.prototype.getTypes = function () {
  return ["db", "nosql", "rethinkdb"];
};

RethinkDB.prototype.getDefaultIdType = function () {
  return String;
};

RethinkDB.prototype.tableName = function (model) {
    var modelSettings = this._models[model].settings;
    return modelSettings.tableName || modelSettings.plural || model;
};

//Override define model function
RethinkDB.prototype.define = function(modelDefinition) {
    modelDefinition.settings = modelDefinition.settings || {};
    this._models[modelDefinition.model.modelName] = modelDefinition;
    this.autoupdate(modelDefinition.model.modelName,function(){})
};

// creates tables if not exists
RethinkDB.prototype.autoupdate = function(models, done) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.autoupdate(models, done);
        });
        return
    }

    if ((!done) && ('function' === typeof models)) {
      done = models;
      models = undefined;
    }

    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(_this._models);

    r.db(_this.database).tableList().run(client, function(error, cursor) {
        if (error) {
            return done(error);
        }

        cursor.toArray(function(error, list) {
            async.each(models, function(model, cb) {
                const tableName = _this.tableName(model);
                if (list.length === 0 || list.indexOf(tableName) < 0) {
                    r.db(_this.database).tableCreate(tableName).run(client, function(error) {
                        if (error) return cb(error);
                        createIndices(cb, model, client);
                    });
                }
                else {
                    createIndices(cb, model, client);
                }
            }, function(e) {
                done(e);
            });
        });

    });

    function createIndices(cb, model, client) {
        var properties = _this._models[model].properties;
        var settings = _this._models[model].settings;
        var indexCollection = _.extend({}, properties, settings);

        function checkAndCreate(list, indexName, indexOption, indexFunction, cb3) {

            // Don't attempt to create an index on primary key 'id'
            if (indexName !== 'id' && _hasIndex(_this, model, indexName) && list.indexOf(indexName) < 0) {
                var query = r.db(_this.database).table(_this.tableName(model));
                if (indexFunction) {
                    query = query.indexCreate(indexName, indexFunction, indexOption);
                }
                else {
                    query = query.indexCreate(indexName, indexOption);
                }
                query.run(client, cb3);
            }
            else {
                cb3();
            }
        }

        if (!_.isEmpty(indexCollection)) {
            r.db(_this.database).table(_this.tableName(model)).indexList().run(client, function(error, cursor) {
                if (error) return cb(error);

                cursor.toArray(function(error, list) {
                    if (error) return cb(error);

                    async.each(Object.keys(indexCollection), function (indexName, cb4) {
                        var indexConf = indexCollection[indexName];
                        checkAndCreate(list, indexName, indexConf.indexOption || {}, indexConf.indexFunction, cb4);
                    }, function(err) {
                        cb(err);
                    });
                });
            });
        } else {
            cb();
        }
    }
};

// drops tables and re-creates them
RethinkDB.prototype.automigrate = function(models, cb) {
    this.autoupdate(models, cb);
};

// checks if database needs to be actualized
RethinkDB.prototype.isActual = function(cb) {
    var _this = this;
    var client = this.db;

    r.db(_this.database).tableList().run(client, function(error, cursor) {
        if (error) return cb(error)
        if (!cursor.next()) return cb(null, _.isEmpty(_this._models))

        cursor.toArray(function(error, list) {
            if (error) {
                return cb(error);
            }
            var actual = true;
            async.each(Object.keys(_this._models), function(model, cb2) {
                if(!actual) return cb2();

                var properties = _this._models[model].properties;
                var settings = _this._models[model].settings;
                var indexCollection = _.extend({}, properties, settings);
                if (list.indexOf(model) < 0) {
                    actual = false;
                    cb2();
                } else {
                    r.db(_this.database).table(_this.tableName(model)).indexList().run(client, function(error, cursor) {
                        if (error) return cb2(error);

                        cursor.toArray(function(error, list) {
                            if (error || !actual) return cb2(error);


                            Object.keys(indexCollection).forEach(function (property) {
                                if (_hasIndex(_this, model, property) && list.indexOf(property) < 0)
                                    actual = false;
                            });
                            cb2();
                        });
                    });
                }
            }, function(err) {
                cb(err, actual);
            });
        });
    });
};

RethinkDB.prototype.create = function (model, data, callback) {
    var idValue = this.getIdValue(model, data)
    var idName = this.idName(model)

    if (idValue === null || idValue === undefined) {
        delete data[idName];
    } else {
        data.id = idValue; // Set it to _id
        idName !== 'id' && delete data[idName];
    }

    this.save(model, data, callback, true);
};

RethinkDB.prototype.updateOrCreate = function (model, data, callback) {
    var idValue = this.getIdValue(model, data)
    var idName = this.idName(model)

    if (idValue === null || idValue === undefined) {
        delete data[idName];
    } else {
        data.id = idValue; // Set it to _id
        idName !== 'id' && delete data[idName];
    }

    this.save(model, data, callback, false, true);
}

RethinkDB.prototype.save = function (model, data, callback, strict, returnObject) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.save(model, data, callback, strict, returnObject);
        });
        return
    }

    var idValue = _this.getIdValue(model, data)
    var idName = _this.idName(model)

    if (strict == undefined)
        strict = false

    Object.keys(data).forEach(function (key) {
        if (data[key] === undefined)
            data[key] = null;
    });

    r.db(_this.database).table(_this.tableName(model)).insert(data, { conflict: strict ? "error": "update", returnChanges: true }).run(client, function (err, m) {
        err = err || m.first_error && new Error(m.first_error);
        if (err) {
            callback && callback(err)
        }
        else {
            var info = {}
            var object = null

            if (m.inserted > 0) {
                // create
                info.isNewInstance = true
                idValue = m.changes[0].new_val.id
            }
            if (m.changes && m.changes.length > 0) {
                // update
                object = m.changes[0].new_val
                _this.setIdValue(model, object, idValue)
                idName !== 'id' && delete object._id
            }

            if (returnObject && m.changes && m.changes.length > 0) {
                callback && callback(null, object, info)
            } else {
                callback && callback(null, idValue, info);
            }
        }
    });
};

RethinkDB.prototype.exists = function (model, id, callback) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.exists(model, id, callback);
        });
        return
    }

    r.db(_this.database).table(_this.tableName(model)).get(id).run(client, function (err, data) {
        callback(err, !!(!err && data));
    });
};

RethinkDB.prototype.find = function find(model, id, options, callback) {
    var _this = this,
        _keys;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.find(model, id, callback);
        });
        return
    }

    var idName = this.idName(model)

    var promise = r.db(_this.database).table(_this.tableName(model))

    if (idName == "id")
        promise = promise.get(id)
    else
        promise = promise.filter({ idName: id })

    var rQuery = promise.toString()

    promise.run(client, function(error, data) {
        // Acquire the keys for this model
        _keys = _this._models[model].properties;

        if (data) {
            // Pass to expansion helper
            _expandResult(data, _keys);
        }

        // Done
        callback && callback(error, data, rQuery);
    });
};

RethinkDB.prototype.destroy = function destroy(model, id, callback) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.destroy(model, id, callback);
        });
        return
    }

    r.db(_this.database).table(_this.tableName(model)).get(id).delete().run(client, function(error, result) {
        callback(error);
    });
};

RethinkDB.prototype.all = function all(model, filter, options, callback) {
    if (options && options.observe) {
        this._observe(model, filter, options, callback);
    } else {
        this._all(model, filter, options, callback);
    }
};

RethinkDB.prototype._observe = function (model, filter, options, callback) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this._observe(model, filter, options, callback);
        });
        return
    }

    if (!filter) {
        filter = {};
    }

    var promise = r.db(_this.database).table(_this.tableName(model));

    var idName = this.idName(model)

    if (filter.order) {
        var keys = filter.order;
        if (typeof keys === 'string') {
            keys = keys.split(',');
        }
        keys.forEach(function (key) {
            var m = key.match(/\s+(A|DE)SC$/);
            key = key.replace(/\s+(A|DE)SC$/, '').trim();
            if (m && m[1] === 'DE') {
                promise = promise.orderBy(r.desc(key));
            } else {
                promise = promise.orderBy(r.asc(key));
            }
        });
    } else {
        // default sort by id
        promise = promise.orderBy({ "index": r.asc("id") });
    }

    if (filter.where) {
        if (filter.where[idName]) {
            var id = filter.where[idName];
            delete filter.where[idName];
            filter.where.id = id;
        }
        promise = buildWhere(_this, model, filter.where, promise)
        if (promise === null)
            return callback && callback(null, [])
    }

    if (filter.limit) {
        promise = promise.limit(filter.limit);
    }

    var defaultOptions = {
        changesOptions: {
            include_states: true
        }
    };
    
    var feedOptions = _.defaultsDeep({}, defaultOptions, options);

    var rQuery = promise.toString()

    //console.log(rQuery)

    var observable = Rx.Observable.create(function (observer) {

        var sendResults = function () {
            _this._all(model, filter, options, function (error, data) {
                if (error) {
                    return observer.onError(error);
                }
                
                observer.onNext(data);
            });
        };
        
        if (_.isNumber(feedOptions.throttle)) {
            sendResults = _.throttle(sendResults, feedOptions.throttle);
        }

        sendResults();

        var feed;
        promise = promise.changes(feedOptions.changesOptions);
        if (filter.skip) {
            promise = promise.skip(filter.skip);
        } else if (filter.offset) {
            promise = promise.skip(filter.offset);
        }
        
        promise.run(client).then(function (res) {
                feed = res;
                var isReady = false;
                feed.eachAsync(function (item) {
                    if (item.state === 'ready') {
                        isReady = true;
                        sendResults();
                    } else if (!item.state && isReady) {
                        sendResults();
                    }
                });
            })
            .catch(function (err) {
                observer.onError(err);
            });

        return function () {
            if (feed) {
                feed.close().catch(function () {});
            }
        };
    });

    callback && callback(null, observable);
};

RethinkDB.prototype._all = function _all(model, filter, options, callback) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this._all(model, filter, options, callback);
        });
        return
    }

    if (!filter) {
        filter = {};
    }

    var promise = r.db(_this.database).table(_this.tableName(model));

    var idName = this.idName(model)

    if (filter.order) {
        var keys = filter.order;
        if (typeof keys === 'string') {
            keys = keys.split(',');
        }
        keys.forEach(function(key) {
            var m = key.match(/\s+(A|DE)SC$/);
            key = key.replace(/\s+(A|DE)SC$/, '').trim();
            if (m && m[1] === 'DE') {
                promise = promise.orderBy(r.desc(key));
            } else {
                promise = promise.orderBy(r.asc(key));
            }
        });
    } else {
        // default sort by id
        promise = promise.orderBy({ "index": r.asc("id") });
    }

    if (filter.where) {
        if (filter.where[idName]) {
            var id = filter.where[idName];
            delete filter.where[idName];
            filter.where.id = id;
        }
        promise = buildWhere(_this, model, filter.where, promise)
        if (promise === null)
            return callback && callback(null, [])
    }

    if (filter.skip) {
        promise = promise.skip(filter.skip);
    } else if (filter.offset) {
        promise = promise.skip(filter.offset);
    }

    if (filter.limit) {
        promise = promise.limit(filter.limit);
    }

    var rQuery = promise.toString()

    //console.log(rQuery)

    promise.run(client, function(error, cursor) {

        if (error || !cursor) {
            return callback(error, null);
        }

        cursor.toArray(function (err, data) {
            if (err) {
                return callback(err);
            }

            var _keys = _this._models[model].properties;
            var _model = _this._models[model].model;

            data.forEach(function(element, index) {
                 if (element["id"] && idName !== 'id') {
                    element[idName]= element["id"];
                    delete element["id"];
                }
                _expandResult(element, _keys);
            });

            if (filter && filter.include && filter.include.length > 0) {
                _model.include(data, filter.include, options, callback);
            } else {
                callback && callback(null, data, rQuery);
            }
        });
    });
};

RethinkDB.prototype.destroyAll = function destroyAll(model, where, callback) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.destroyAll(model, where, callback);
        });
        return
    }

    if (!callback && "function" === typeof where) {
        callback = where
        where = undefined
    }

    var promise = r.db(_this.database).table(_this.tableName(model))
    if (where !== undefined)
        promise = buildWhere(_this, model, where, promise)

    if (promise === null)
        return callback(null, { count: 0 })

    promise.delete().run(client, function(error, result) {
        callback(error, { count: result ? result.deleted : null});
    });
};

RethinkDB.prototype.count = function count(model, where, options, callback) {
    var _this = this;
    var client = this.db;

    callback = callback || function(){}
    
    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.count(model, where, callback);
        });
        return
    }

    var promise = r.db(_this.database).table(_this.tableName(model));

    if (where && typeof where === "object")
        promise = buildWhere(_this, model, where, promise);

    if (promise === null)
        return callback(null, 0)

    promise.count().run(client, function (err, count) {
        callback(err, count);
    });
};

RethinkDB.prototype.updateAttributes = function updateAttrs(model, id, data, cb) {
    var _this = this;
    var client = this.db;


    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.updateAttributes(model, id, data, callback);
        });
        return
    }

    //data.id = id;
    Object.keys(data).forEach(function (key) {
        if (data[key] === undefined)
            data[key] = null;
    });
    r.db(_this.database).table(_this.tableName(model)).get(id).update(data).run(client, function(err, object) {
        cb(err, data);
    });
};

RethinkDB.prototype.update = RethinkDB.prototype.updateAll = function update(model, where, data, callback) {
    var _this = this;
    var client = this.db;


    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.update(model, where, data, callback);
        });
        return
    }

    var promise = r.db(_this.database).table(_this.tableName(model))
    if (where !== undefined)
        promise = buildWhere(_this, model, where, promise)

    if (promise === null)
        return callback && callback(null, { count: 0 })

    Object.keys(data).forEach(function(k) {
        if(data[k].$add) {
            data[k] = r.row(k).add(data[k].$add);
        }
    })

    promise.update(data, { returnChanges: true }).run(client, function(error, result) {
        callback(error, { count: result ? result.replaced : null });
    });
}

RethinkDB.prototype.disconnect = function () {
    this.db.close()
    this.db = null
};

/*
    Some values may require post-processing. Do that here.
*/
function _expandResult(result, keys) {

    Object.keys(result).forEach(function (key) {

        if (!keys.hasOwnProperty(key)) return;

        if (keys[key]['type'] &&
            keys[key]['type']['name'] === 'Date' &&
            !(result[key] instanceof Date)) {

            // Expand date result data, backward compatible
            result[key] = moment.unix(result[key]).toDate();
        }
    });
}

function _hasIndex(_this, model, key) {

    // Primary key always hasIndex
    if (key === 'id') return true;

    var modelDef = _this._models[model];
    return (_.isObject(modelDef.properties[key]) && modelDef.properties[key].index) ||
        (_.isObject(modelDef.settings[key]) && modelDef.settings[key].index);
}

function _toMatchExpr(regexp) {
    var expr = regexp.toString(),
        exprStop = expr.lastIndexOf('/'),
        exprCi = expr.slice(exprStop).search('i');

    expr = expr.slice(1, exprStop);

    if (exprCi > -1) {
        expr = '(?i)' + expr;
    }

    return expr;
}

function _matchFn(k, cond) {

    var matchExpr = _toMatchExpr(cond);
    return function (row) {
        return row(k).match(matchExpr);
    };
}

function _inqFn(k, cond) {

    return function (row) {
        return r.expr(cond).contains(row(k));
    };
}

//Handle nested properties
function getRow(key, r){
    //get nested row
    var props = key.split('.');
    return props.reduce(function (row, val, index) {
        return row(val);
    }, r.row)
}

var operators = {
    "between": function(key, value) {
        var row = getRow(key, r);
        return row.gt(value[0]).and(row.lt(value[1]))
    },
    "gt": function(key, value) {
        if (value === null || value === undefined) return null
        return getRow(key, r).gt(value)
    },
    "lt": function(key, value) {
        if (value === null || value === undefined) return null
        return getRow(key, r).lt(value)
    },
    "gte": function(key, value) {
        if (value === null || value === undefined) return null
        return getRow(key, r).ge(value)
    },
    "lte": function(key, value) {
        if (value === null || value === undefined) return null
        return getRow(key, r).le(value)
    },
    "inq": function(key, value) {
        var query = []

        value.forEach(function(v) {
            query.push(getRow(key, r).eq(v))
        })

        var condition = _.reduce(query, function(sum, qq) {
            return sum.or(qq)
        })

        return condition
    },
    "nin": function(key, value) {
        var query = []

        value.forEach(function(v) {
            query.push(getRow(key, r).ne(v))
        })

        var condition = _.reduce(query, function(sum, qq) {
            return sum.and(qq)
        })

        return condition
    },
    "neq": function(key, value) {
        return getRow(key, r).ne(value)
    },
    "like": function(key, value) {
        return getRow(key, r).match(value)
    },
    "nlike": function(key, value) {
        return getRow(key, r).match(value).not()
    }
}

function buildFilter(where) {
    var filter = []

    Object.keys(where).forEach(function(k) {

        // determine if k is field name or condition name
        var conditions = ["and", "or", "between", "gt", "lt", "gte", "lte", "inq", "nin", "near", "neq", "like", "nlike"]
        var condition = where[k]

        if (k === "and" || k === "or") {
            if (_.isArray(condition)) {
                var query = _.map(condition, function(c) {
                    return buildFilter(c)
                })

                if (k === "and")
                    filter.push(_.reduce(query, function(s, f) {
                        return s.and(f)
                    }))
                else
                    filter.push(_.reduce(query, function(s, f) {
                        return s.or(f)
                    }))
            }
        } else {
            if (_.isObject(condition) && _.intersection(_.keys(condition), conditions).length > 0) {
                // k is condition
                _.keys(condition).forEach(function(operator) {
                    if (conditions.indexOf(operator) >= 0) {
                        filter.push(operators[operator](k, condition[operator]))
                    }
                })
            } else {
                // k is field equality
                filter.push(getRow(k, r).eq(condition))
            }
        }

    })

    if (_.findIndex(filter, function(item) { return item === null }) >= 0)
        return null

    if (filter.length == 0)
        return undefined

    return _.reduce(filter, function(s, f) {
        return s.and(f)
    })
}

function buildWhere(self, model, where, promise) {
    if (where === null || (typeof where !== 'object')) {
        return promise;
    }

    var query = buildFilter(where)

    if (query === undefined)
        return promise
    else if (query === null)
        return null
    else
        return promise.filter(query)
}
