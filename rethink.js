'use strict';

const r = require('rethinkdbdash')();
const moment = require('moment');
const async = require('async');
const _ = require('lodash');
const util = require('util');
const Rx = require('rx');

const Connector = require('loopback-connector').Connector;

exports.initialize = function initializeDataSource(dataSource, callback) {
	if (!r) return;

	const s = dataSource.settings;

	if (dataSource.settings.rs) {
		s.rs = dataSource.settings.rs;
		if (dataSource.settings.url) {
			const uris = dataSource.settings.url.split(',');
			s.hosts = [];
			s.ports = [];
			uris.forEach(uri => {
				const url = require('url').parse(uri);

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
			const url = require('url').parse(dataSource.settings.url);
			s.host = url.hostname;
			s.port = url.port;
			s.database = (url.pathname || '').replace(/^\//, '');
			s.username = url.auth && url.auth.split(':')[0];
			s.password = url.auth && url.auth.split(':')[1];
		}

		s.host = s.host || 'localhost';
		s.port = parseInt(s.port || '28015', 10);
		s.database = s.database || 'test';
	}

	s.safe = s.safe || false;

	dataSource.adapter = new RethinkDB(s, dataSource, r);
	dataSource.connector = dataSource.adapter;

	if (callback) {
		dataSource.connector.connect(callback);
	}

	process.nextTick(callback);
};

function RethinkDB(s, dataSource, r) {
	Connector.call(this, 'rethink', s);
	this.dataSource = dataSource;
	this.database = s.database;
	this.r = r;
}

util.inherits(RethinkDB, Connector);

RethinkDB.prototype.connect = function (cb) {
	const _this = this;
	const s = _this.settings;
	if (_this.db) {
		process.nextTick(() => {
			cb && cb(null, _this.db);
		});
	} else {
		const cOpts = Object.assign(
			{
				host: s.host,
				port: s.port,
				authKey: s.password
			},
			s.additionalSettings
		);
		if (cOpts.ssl && cOpts.ssl.ca) {
			// check if is a base64 encoded string
			if (
				/^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$/.test(
					cOpts.ssl.ca
				)
			) {
				cOpts.ssl.ca = Buffer.from(cOpts.ssl.ca, 'base64');
			}
		}
		r.connect(cOpts, (error, client) => {
			_this.db = client;
			cb && cb(error, client);
		});
	}
};

RethinkDB.prototype.getTypes = function () {
	return ['db', 'nosql', 'rethinkdb'];
};

RethinkDB.prototype.getDefaultIdType = function () {
	return String;
};

RethinkDB.prototype.tableName = function (model) {
	const modelClass = this._models[model];
	if (modelClass.settings.rethinkdb) {
		model = _.get(modelClass, 'settings.rethinkdb.collection') ||
			modelClass.settings.plural ||
			model;
	}
	return model;
};

// Override define model function
RethinkDB.prototype.define = function (modelDefinition) {
	modelDefinition.settings = modelDefinition.settings || {};
	this._models[modelDefinition.model.modelName] = modelDefinition;
	// this.autoupdate(modelDefinition.model.modelName,function(){})
};

RethinkDB.prototype.autoupdate = function (models, cb) {
	const _this = this;
	if (_this.db) {
		if (_this.debug) {
			debug('autoupdate');
		}
		if (!cb && typeof models === 'function') {
			cb = models;
			models = undefined;
		}
		// First argument is a model name
		if (typeof models === 'string') {
			models = [models];
		}

		models = models || Object.keys(_this._models);

		const enableGeoIndexing = this.settings.enableGeoIndexing === true;

		async.each(
			models,
			(model, modelCallback) => {
				const indexes = _this._models[model].settings.indexes || [];
				let indexList = [];
				let index = {};
				let options = {};

				if (typeof indexes === 'object') {
					for (const indexName in indexes) {
						index = indexes[indexName];
						if (index.keys) {
							// The index object has keys
							options = index.options || {};
							index.options = options;
						} else {
							index = {
								keys: index,
								options: {}
							};
						}
						index.name = indexName;
						indexList.push(index);
					}
				} else if (Array.isArray(indexes)) {
					indexList = indexList.concat(indexes);
				}

				if (_this.debug) {
					debug('create indexes: ', indexList);
				}

				r
					.db(_this.database)
					.table(_this.tableName(model))
					.indexList()
					.run(_this.db, (error, alreadyPresentIndexes) => {
						if (error) return cb(error);

						async.each(
							indexList,
							(index, indexCallback) => {
								if (_this.debug) {
									debug('createIndex: ', index);
								}

								if (alreadyPresentIndexes.includes(index.name)) {
									return indexCallback();
								}

								let query = r.db(_this.database).table(_this.tableName(model));
								const keys = Object.keys(
									index.fields || index.keys
								).map(key => _this.getRow(model, key));
								query = query.indexCreate(
									index.name,
									keys.length === 1 ? keys[0] : keys,
									index.options
								);
								query.run(_this.db, indexCallback);
							},
							modelCallback
						);
					});
			},
			cb
		);
	} else {
		_this.dataSource.once('connected', () => {
			_this.autoupdate(models, cb);
		});
	}
};

// drops tables and re-creates them
RethinkDB.prototype.automigrate = _.debounce(
	function (models, done) {
		const _this = this;
		const client = this.db;

		if (!client) {
			_this.dataSource.once('connected', () => {
				_this.automigrate(models, done);
			});
			return;
		}

		if (!done && typeof models === 'function') {
			done = models;
			models = undefined;
		}

		// First argument is a model name
		if (typeof models === 'string') {
			models = [models];
		}

		models = _.uniq(models || Object.keys(_this._models));

		r.db(_this.database).tableList().run(client, (error, list) => {
			if (error) {
				return done(error);
			}
			const migratedTables = [];

			async.eachSeries(
				models,
				(model, cb) => {
					const tableName = _this.tableName(model);
					if (migratedTables.indexOf(tableName) === -1) {
						migratedTables.push(tableName);
						r.db(_this.database).tableDrop(tableName).run(client, error => {
							r.db(_this.database).tableCreate(tableName).run(client, error => {
								if (error) return cb(error);
								cb();
							});
						});
					} else {
						cb();
					}
				},
				err => {
					if (err) return done(e);
					_this.autoupdate(models, done);
				}
			);
		});
	},
	4000
);

RethinkDB.prototype.create = function (model, data, callback) {
	const idValue = this.getIdValue(model, data);
	const idName = this.idName(model);

	if (idValue === null || idValue === undefined) {
		delete data[idName];
	} else {
		data.id = idValue; // Set it to _id
		idName !== 'id' && delete data[idName];
	}

	if (process.env.TESTING) {
		data._createdAt = new Date();
	}

	this.save(model, data, callback, true);
};

RethinkDB.prototype.updateOrCreate = function (model, data, callback) {
	const idValue = this.getIdValue(model, data);
	const idName = this.idName(model);

	if (idValue === null || idValue === undefined) {
		delete data[idName];
	} else {
		data.id = idValue; // Set it to _id
		idName !== 'id' && delete data[idName];
	}

	this.save(model, data, callback, false, true);
};

RethinkDB.prototype.save = function (
	model,
	data,
	callback,
	strict,
	returnObject) {
	const _this = this;
	const client = this.db;

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.save(model, data, callback, strict, returnObject);
		});
		return;
	}

	let idValue = _this.getIdValue(model, data);
	const idName = _this.idName(model);

	if (strict == undefined) {
		strict = false;
	}

	Object.keys(data).forEach(key => {
		if (data[key] === undefined) {
			data[key] = null;
		}
	});

	r
		.db(_this.database)
		.table(_this.tableName(model))
		.insert(data, {
			conflict: strict ? 'error' : 'update',
			returnChanges: true
		})
		.run(client, (err, m) => {
			err = err || (m.first_error && new Error(m.first_error));
			if (err) {
				callback && callback(err);
			} else {
				const info = {};
				let object = null;

				if (m.inserted > 0) {
					// create
					info.isNewInstance = true;
					idValue = m.changes[0].new_val.id;
				}
				if (m.changes && m.changes.length > 0) {
					// update
					object = m.changes[0].new_val;
					_this.setIdValue(model, object, idValue);
					idName !== 'id' && delete object._id;
				}

				if (returnObject && m.changes && m.changes.length > 0) {
					callback && callback(null, object, info);
				} else {
					callback && callback(null, idValue, info);
				}
			}
		});
};

RethinkDB.prototype.exists = function (model, id, callback) {
	const _this = this;
	const client = this.db;

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.exists(model, id, callback);
		});
		return;
	}

	r
		.db(_this.database)
		.table(_this.tableName(model))
		.get(id)
		.run(client, (err, data) => {
			callback(err, !!(!err && data));
		});
};

RethinkDB.prototype.find = function find(model, id, options, callback) {
	let _this = this,
		_keys;
	const client = this.db;

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.find(model, id, callback);
		});
		return;
	}

	const idName = this.idName(model);

	let promise = r.db(_this.database).table(_this.tableName(model));

	if (idName == 'id') {
		promise = promise.get(id);
	} else {
		promise = promise.filter({ idName: id });
	}

	const rQuery = promise.toString();

	promise.run(client, (error, data) => {
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
	const _this = this;
	const client = this.db;

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.destroy(model, id, callback);
		});
		return;
	}

	r
		.db(_this.database)
		.table(_this.tableName(model))
		.get(id)
		.delete()
		.run(client, (error, result) => {
			callback(error);
		});
};

RethinkDB.prototype.changeFeed = function (model, filter, options) {
	const _this = this;
	const client = this.db;

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.changeFeed(model, filter, options);
		});
		return;
	}

	if (!filter) {
		filter = {};
	}

	let promise = r.db(_this.database).table(_this.tableName(model));

	const idName = this.idName(model);

	if (filter.order) {
		let keys = filter.order;
		if (typeof keys === 'string') {
			keys = keys.split(',');
		}
		keys.forEach(key => {
			const m = key.match(/\s+(A|DE)SC$/);
			key = key.replace(/\s+(A|DE)SC$/, '').trim();
			if (m && m[1] === 'DE') {
				promise = promise.orderBy({ index: r.desc(key) });
			} else {
				promise = promise.orderBy({ index: r.asc(key) });
			}
		});
	} else {
		// default sort by id
		promise = promise.orderBy({ index: r.asc('id') });
	}

	if (filter.where) {
		if (filter.where[idName]) {
			const id = filter.where[idName];
			delete filter.where[idName];
			filter.where.id = id;
		}
		promise = this.buildWhere(model, filter.where, promise);
		/* if (promise === null)
		 return callback && callback(null, []) */
	}

	if (!_.isEmpty(filter.fields)) {
		promise = _this.buildPluck(model, filter.fields, promise);
	}

	if (filter.limit) {
		promise = promise.limit(filter.limit);
	}

	const defaultOptions = {
		changesOptions: {
			includeStates: true
		}
	};

	const feedOptions = _.defaultsDeep({}, defaultOptions, options);

	const rQuery = promise.toString();

	// console.log(rQuery)

	const observable = Rx.Observable.create(observer => {
		let sendResults = function () {
			_this.all(model, filter, options, (error, data) => {
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

		let feed;
		promise = promise.changes(feedOptions.changesOptions);
		if (filter.skip) {
			promise = promise.skip(filter.skip);
		} else if (filter.offset) {
			promise = promise.skip(filter.offset);
		}

		promise
			.run(client)
			.then(res => {
				feed = res;
				let isReady = false;
				return feed.eachAsync(item => {
					if (item.state === 'ready') {
						isReady = true;
						sendResults();
					} else if (!item.state && isReady) {
						sendResults();
					}
				});
			})
			.catch(err => {
				observer.onError(err);
			});

		return function () {
			if (feed) {
				feed.close().catch(() => {});
			}
		};
	});

	return observable;
};

RethinkDB.prototype.all = function all(model, filter, options, callback) {
	const _this = this;
	const client = this.db;

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.all(model, filter, options, callback);
		});
		return;
	}

	if (!filter) {
		filter = {};
	}

	let promise = r.db(_this.database).table(_this.tableName(model));

	const idName = this.idName(model);

	if (filter.order) {
		let keys = filter.order;
		if (typeof keys === 'string') {
			keys = keys.split(',');
		}
		keys.forEach(key => {
			const m = key.match(/\s+(A|DE)SC$/);
			key = key.replace(/\s+(A|DE)SC$/, '').trim();
			if (m && m[1] === 'DE') {
				promise = promise.orderBy(r.desc(key));
			} else {
				promise = promise.orderBy(r.asc(key));
			}
		});
	} else {
		// default sort by id
		if (process.env.TESTING) {
			promise = promise.orderBy(r.asc('_createdAt'));
		} else {
			promise = promise.orderBy({ index: r.asc('id') });
		}
	}

	if (filter.where) {
		if (filter.where[idName]) {
			const id = filter.where[idName];
			delete filter.where[idName];
			filter.where.id = id;
		}
		promise = this.buildWhere(model, filter.where, promise);
		if (promise === null) {
			return callback && callback(null, []);
		}
	}

	if (filter.skip) {
		promise = promise.skip(filter.skip);
	} else if (filter.offset) {
		promise = promise.skip(filter.offset);
	}

	if (!_.isEmpty(filter.fields)) {
		promise = _this.buildPluck(model, filter.fields, promise);
	}

	if (filter.limit) {
		promise = promise.limit(filter.limit);
	}

	const rQuery = promise.toString();

	// console.log(rQuery)

	promise.run(client, (err, data) => {
		if (err) {
			return callback(err);
		}

		const _keys = _this._models[model].properties;
		const _model = _this._models[model].model;

		data.forEach((element, index) => {
			if (element.id && idName !== 'id') {
				element[idName] = element.id;
				delete element.id;
			}
			_expandResult(element, _keys);
		});

		if (process.env.TESTING) {
			data = data.map(item => {
				delete item._createdAt;
				return item;
			});
		}

		if (filter && filter.include) {
			_this._models[model].model.include(data, filter.include, (err, data) => {
				callback(err, data);
			});
		} else {
			callback && callback(null, data, rQuery);
		}
	});
};

RethinkDB.prototype.destroyAll = function destroyAll(model, where, callback) {
	const _this = this;
	const client = this.db;

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.destroyAll(model, where, callback);
		});
		return;
	}

	if (!callback && typeof where === 'function') {
		callback = where;
		where = undefined;
	}

	let promise = r.db(_this.database).table(_this.tableName(model));
	if (where !== undefined) {
		promise = this.buildWhere(model, where, promise);
	}

	if (promise === null) {
		return callback(null, { count: 0 });
	}

	promise.delete().run(client, (error, result) => {
		callback(error, { count: result ? result.deleted : null });
	});
};

RethinkDB.prototype.count = function count(model, where, options, callback) {
	const _this = this;
	const client = this.db;

	callback = callback || function () {};

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.count(model, where, callback);
		});
		return;
	}

	let promise = r.db(_this.database).table(_this.tableName(model));

	if (where && typeof where === 'object') {
		promise = this.buildWhere(model, where, promise);
	}

	if (promise === null) {
		return callback(null, 0);
	}

	promise.count().run(client, (err, count) => {
		callback(err, count);
	});
};

RethinkDB.prototype.updateAttributes = function updateAttrs(
	model,
	id,
	data,
	cb) {
	const _this = this;
	const client = this.db;

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.updateAttributes(model, id, data, callback);
		});
		return;
	}

	// data.id = id;
	Object.keys(data).forEach(key => {
		if (data[key] === undefined) {
			data[key] = null;
		}
	});
	r
		.db(_this.database)
		.table(_this.tableName(model))
		.get(id)
		.update(data)
		.run(client, (err, object) => {
			cb(err, data);
		});
};

RethinkDB.prototype.update = (RethinkDB.prototype.updateAll = function update(
	model,
	where,
	data,
	callback) {
	const _this = this;
	const client = this.db;

	if (!client) {
		_this.dataSource.once('connected', () => {
			_this.update(model, where, data, callback);
		});
		return;
	}

	let promise = r.db(_this.database).table(_this.tableName(model));
	if (where !== undefined) {
		promise = this.buildWhere(model, where, promise);
	}

	if (promise === null) {
		return callback && callback(null, { count: 0 });
	}

	Object.keys(data).forEach(k => {
		if (data[k].$add) {
			data[k] = r.row(k).add(data[k].$add);
		}
	});

	promise.update(data, { returnChanges: true }).run(client, (error, result) => {
		callback(error, { count: result ? result.replaced : null });
	});
});

RethinkDB.prototype.disconnect = function () {
	this.db.close();
	this.db = null;
};

RethinkDB.prototype.buildWhere = function (model, where, promise) {
	const _this = this;
	if (where === null || typeof where !== 'object') {
		return promise;
	}

	const query = this.buildFilter(where, model);

	if (query === undefined) {
		return promise;
	} else if (query === null) {
		return null;
	}
	return promise.filter(query);
};

RethinkDB.prototype.buildFilter = function (where, model) {
	const filter = [];
	const _this = this;

	Object.keys(where).forEach(k => {
		// determine if k is field name or condition name
		const conditions = [
			'and',
			'or',
			'between',
			'gt',
			'lt',
			'gte',
			'lte',
			'inq',
			'nin',
			'near',
			'neq',
			'like',
			'nlike',
			'regexp'
		];
		const condition = where[k];

		if (k === 'and' || k === 'or') {
			if (_.isArray(condition)) {
				const query = _.map(condition, c => _this.buildFilter(c, model));

				if (k === 'and') {
					filter.push(_.reduce(query, (s, f) => s.and(f)));
				} else {
					filter.push(_.reduce(query, (s, f) => s.or(f)));
				}
			}
		} else if (
			_.isObject(condition) &&
			_.intersection(_.keys(condition), conditions).length > 0
		) {
			// k is condition
			_.keys(condition).forEach(operator => {
				if (conditions.indexOf(operator) >= 0) {
					// filter.push(operators[operator](k, condition[operator], _this.getPropertyDefinition.bind(_this, model)))
					filter.push(
						_this.getCondition(
							model,
							k,
							null,
							null,
							condition[operator],
							operator
						)
					);
				}
			});
		} else {
			// k is field equality
			filter.push(_this.getCondition(model, k, null, null, condition));
		}
	});

	if (_.findIndex(filter, item => item === null) >= 0) {
		return null;
	}

	if (filter.length == 0) {
		return undefined;
	}

	return _.reduce(filter, (s, f) => s.and(f));
};

RethinkDB.prototype.buildPluck = function (model, fields, promise) {
	const pluckObj = fields.reduce(
		(acc, fieldName) => {
			_.set(acc, fieldName, true);
			return acc;
		},
		{}
	);

	return promise.pluck(pluckObj);
};

// Handle nested properties
RethinkDB.prototype.getCondition = function (
	model,
	key,
	relativePath,
	partialPath,
	criteria,
	operator,
	row) {
	const _this = this;

	row = this.getRow(
		model,
		key,
		relativePath,
		partialPath,
		criteria,
		operator,
		row
	);
	if (row.toString().indexOf('contains(') > -1) return row;

	return _this.applyOperator(row, operator, criteria);
};

RethinkDB.prototype.applyOperator = function (row, operator, criteria) {
	const operators = {
		between(row, value) {
			return row.gt(value[0]).and(row.lt(value[1]));
		},
		gt(row, value) {
			if (value === null || value === undefined) return null;
			return row.gt(value);
		},
		lt(row, value) {
			if (value === null || value === undefined) return null;
			return row.lt(value);
		},
		gte(row, value) {
			if (value === null || value === undefined) return null;
			return row.ge(value);
		},
		lte(row, value) {
			if (value === null || value === undefined) return null;
			return row.le(value);
		},
		inq(row, value) {
			const query = [];

			value.forEach(v => {
				query.push(row.eq(v));
			});

			const condition = _.reduce(query, (sum, qq) => sum.or(qq));

			return condition;
		},
		nin(row, value) {
			const query = [];

			value.forEach(v => {
				query.push(row.ne(v));
			});

			const condition = _.reduce(query, (sum, qq) => sum.and(qq));

			return condition;
		},
		neq(row, value) {
			return row.ne(value);
		},
		like(row, value) {
			return row.match(value);
		},
		nlike(row, value) {
			return row.match(value).not();
		},
		regexp(row, value) {
			return row.match(_.trim(`${value}`, '//'));
		}
	};

	if (operators[operator]) {
		return operators[operator](row, criteria);
	}
	return row.eq(criteria);
};

RethinkDB.prototype.getNestedPropertyDefinition = function (model, path) {
	const modelDefinition = this.getModelDefinition(model);
	return _.get(modelDefinition, `properties.${path}`);
};

// Handle nested properties
RethinkDB.prototype.getRow = function (
	model,
	key,
	relativePath,
	partialPath,
	criteria,
	operator,
	row,
	isNested) {
	const _this = this;
	const props = relativePath || key.split('.');
	partialPath = partialPath || [];

	return props.reduce(
		(row, prop, index) => {
			partialPath.push(prop);
			const propDef = _this.getNestedPropertyDefinition(
					model,
					partialPath.join('.')
				) || {};

			if (Array.isArray(propDef) || Array.isArray(propDef.type)) {
				const _relativePath = props.slice(index + 1);
				return row(prop).contains(doc =>
					_this.getCondition(
						model,
						key,
						_relativePath,
						partialPath,
						criteria,
						operator,
						doc
					));
			}
			if (row.toString().indexOf('contains(') > -1) return row;
			return row(prop);
		},
		row || this.r.row
	);
};

/*
 Some values may require post-processing. Do that here.
 */
function _expandResult(result, keys) {
	Object.keys(result).forEach(key => {
		if (!keys.hasOwnProperty(key)) return;

		if (
			keys[key].type &&
			keys[key].type.name === 'Date' &&
			!(result[key] instanceof Date)
		) {
			// Expand date result data, backward compatible
			result[key] = moment.unix(result[key]).toDate();
		}
	});
}

function _hasIndex(_this, model, key) {
	// Primary key always hasIndex
	if (key === 'id') return true;

	const modelDef = _this._models[model];
	return (_.isObject(modelDef.properties[key]) &&
		modelDef.properties[key].index) ||
		(_.isObject(modelDef.settings[key]) && modelDef.settings[key].index);
}

function _toMatchExpr(regexp) {
	let expr = regexp.toString(),
		exprStop = expr.lastIndexOf('/'),
		exprCi = expr.slice(exprStop).search('i');

	expr = expr.slice(1, exprStop);

	if (exprCi > -1) {
		expr = `(?i)${expr}`;
	}

	return expr;
}

function _matchFn(k, cond) {
	const matchExpr = _toMatchExpr(cond);
	return function (row) {
		return row(k).match(matchExpr);
	};
}

function _inqFn(k, cond) {
	return function (row) {
		return r.expr(cond).contains(row(k));
	};
}
