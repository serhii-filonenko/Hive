'use strict';

const _ = require('lodash');
const async = require('async');
const thriftService = require('./thriftService/thriftService');
const hiveHelper = require('./thriftService/hiveHelper');
const TCLIService = require('./TCLIService/Thrift_0.9.3_Hive_2.1.1/TCLIService');
const TCLIServiceTypes = require('./TCLIService/Thrift_0.9.3_Hive_2.1.1/TCLIService_types');

module.exports = {
	connect: function(connectionInfo, logger, cb){
		logger.clear();
		logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);

		thriftService.connect({
			host: connectionInfo.host,
			port: connectionInfo.port,
			username: connectionInfo.user,
			password: connectionInfo.password,
			authMech: 'NOSASL',
			configuration: {}
		})(cb)(TCLIService, TCLIServiceTypes);
	},

	disconnect: function(connectionInfo, cb){
		cb();
	},

	testConnection: function(connectionInfo, logger, cb){
		this.connect(connectionInfo, logger, cb);
	},

	getDbCollectionsNames: function(connectionInfo, logger, cb) {
		const { includeSystemCollection } = connectionInfo;

		this.connect(connectionInfo, logger, (err, session, cursor) => {
			if (err) {
				return cb(err);
			}
			const exec = cursor.asyncExecute.bind(null, session.sessionHandle);
			const execWithResult = getExecutorWithResult(cursor, exec);

			execWithResult('show databases')
				.then(databases => databases.map(d => d.database_name))
				.then(databases => {
					async.mapSeries(databases, (dbName, next) => {
						exec(`use ${dbName}`)
							.then(() => execWithResult(`show tables`))
							.then((tables) => tables.map(table => table.tab_name))
							.then(dbCollections => next(null, {
								isEmpty: !Boolean(dbCollections.length),
								dbName,
								dbCollections
							}))
							.catch(err => next(err))
					}, cb);
				});
		});
	},

	getDbCollectionsData: function(data, logger, cb){
		const tables = data.collectionData.collections;
		const databases = data.collectionData.dataBaseNames;
		const pagination = data.pagination;
		const includeEmptyCollection = data.includeEmptyCollection;
		const recordSamplingSettings = data.recordSamplingSettings;
		const fieldInference = data.fieldInference;
	
		this.connect(data, logger, (err, session, cursor) => {
			if (err) {
				return cb(err);
			}

			async.mapSeries(databases, (dbName, nextDb) => {
				const exec = cursor.asyncExecute.bind(null, session.sessionHandle);
				const execWithResult = getExecutorWithResult(cursor, exec);
				const getJsonSchema = hiveHelper.getJsonSchemaCreator(...cursor.getTCLIService());
				const tableNames = tables[dbName] || [];

				exec(`use ${dbName}`)
					.then(() => execWithResult(`describe database ${dbName}`))
					.then((databaseInfo) => {
						async.mapSeries(tableNames, (tableName, nextTable) => {
							execWithResult(`describe formatted ${tableName}`)
								.then(data => {
									console.log(data);
								})
								.then(() => execWithResult(`select count(*) as count from ${tableName}`))
								.then((data) => {
									return getLimitByCount(data[0].count, recordSamplingSettings);
								})
								.then(limit => {
									return getDataByPagination(pagination, limit, (limit, offset, next) => {
										execWithResult(`select * from ${tableName} limit ${limit} offset ${offset}`)
											.then(data => next(null, data), err => next(err));
									});
								})
								.then((documents) => {
									const documentPackage = {
										dbName,
										collectionName: tableName,
										documents,
										indexes: [],
										bucketIndexes: [],
										views: [],
										validation: false,
										emptyBucket: false,
										containerLevelKeys: [],
										bucketInfo: {
											comments: _.get(databaseInfo, '[0].comment', '')
										}
									};

									if (fieldInference.active === 'field') {
										documentPackage.documentTemplate = _.cloneDeep(documents[0]);
									}

									return documentPackage;
								})
								.then((documentPackage) => exec(`select * from ${tableName} limit 1`)
									.then(cursor.getSchema)
									.then(getJsonSchema)
									.then(jsonSchema => {
										if (jsonSchema) {
											documentPackage.validation = { jsonSchema };
										}

										return documentPackage;
									})
								)
								.then((documentPackage) => {
									nextTable(null, documentPackage);
								})
								.catch(err => nextTable(err));
						}, nextDb);
					});
			}, cb);
		});
	}
};

const getLimitByCount = (count, recordSamplingSettings) => {
	let limit = count;

	if (recordSamplingSettings.active === 'relative') {
		limit = Math.ceil((count * Number(recordSamplingSettings.relative.value)) / 100);
	} else {
		const absolute = Number(recordSamplingSettings.absolute.value);
		limit = count > absolute ? absolute : count;
	}

	const maxValue = Number(recordSamplingSettings.maxValue);

	if (limit > maxValue) {
		limit = maxValue;
	}

	return limit;
};

const getPages = (total, pageSize) => {
	const generate = (size) => size <= 0 ? [0] : [...generate(size - 1), size];

	return generate(Math.ceil(total / pageSize) - 1);
};

const getDataByPagination = (pagination, limit, callback) => new Promise((resolve, reject) => {
	const getResult = (err, data) => err ? reject(err) : resolve(data);
	const pageSize = Number(pagination.value);

	if (!pagination.enabled) {
		return callback(limit, 0, getResult);
	}

	async.reduce(
		getPages(limit, pageSize),
		[],
		(result, page, next) => {
			callback(pageSize, page, (err, data) => {
				if (err) {
					next(err);
				} else {
					next(null, result.concat(data));
				}
			});
		},
		getResult
	);
});

const getExecutorWithResult = (cursor, exec) => {
	const resultParser = hiveHelper.getResultParser(...cursor.getTCLIService());
	
	return (statement, limit) => {
		return exec(statement).then(resp => Promise.all([
			cursor.fetchResult(resp, limit),
			cursor.getSchema(resp)
		])).then(([ resultResp, schemaResp ]) => {
			return resultParser(schemaResp, resultResp)
		});
	};
};
