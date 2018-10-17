const thrift = require('thrift');

const getConnectionByMechanism = (authMech) => {
	if (authMech === 'NOSASL') {
		return {
			transport: thrift.TBufferedTransport,
			protocol: thrift.TBinaryProtocol
		};
	} else {
		throw new Error("The authentication mechanism " + authMech + " is not supported!");
	}
};

const cacheCall = (func) => {
	let cache = null;
	return (...args) => {
		if (!cache) {
			cache = func(...args)
		}

		return cache;
	};
};

const getConnection = cacheCall((TCLIService, { host, port, authMech, options }) => {
	const connection = thrift.createConnection(host, port, Object.assign({
		https: false,
		debug: true,
		max_attempts: 1,
		retry_max_delay: 2,
		connect_timeout: 1000,
		timeout: 1000
	}, getConnectionByMechanism(authMech), (options || {})));
	
	return thrift.createClient(TCLIService, connection);
});

const connect = ({ host, port, username, password, authMech, options, configuration }) => (handler) => (TCLIService, TCLIServiceTypes) => {
	const execute = (sessionHandle, statement, options = {}) => {
		return new Promise((resolve, reject) => {
			const requestOptions = Object.assign({
				sessionHandle,
				statement,
				confOverlay: undefined,
				runAsync: false,
				queryTimeout: 100000
			}, options);
			const request = new TCLIServiceTypes.TExecuteStatementReq(requestOptions);
		
			getConnection().ExecuteStatement(request, (err, res) => {
				if (err) {
					reject(err);
				} else {
					resolve(res);
				}
			});
		});
	};
	
	const asyncExecute = (sessionHandle, statement, options = {}) => {
		return execute(sessionHandle, statement, Object.assign({}, options, { runAsync: true }))
			.then((res) => {
				return waitFinish(res.operationHandle)
					.then(() => {
						return Promise.resolve(res);
					});
			});
	};
	
	const fetchResult = (executeStatementResponse, limit = 100) => {
		if (!executeStatementResponse.operationHandle.hasResultSet) {
			return Promise.resolve([]);
		}
	
		const getResult = (res, next) => {
			if (typeof next === 'function') {
				next()
					.then(getResult)
					.then(prevRes => [...res, ...prevRes]);
			} else {
				return Promise.resolve(res);
			}
		};
	
		return fetchFirstResult(executeStatementResponse.operationHandle, limit)
			.then(res => {
				if (res.hasMoreRows) {
					return Promise.resolve([res], () => {
						return fetchNextResult(executeStatementResponse.operationHandle, limit);
					});
				} else {
					return Promise.resolve([res]);
				}
			})
			.then(getResult);
	};
	
	const getSchema = (executeStatementResponse) => new Promise((resolve, reject) => {
		if (!executeStatementResponse.operationHandle) {
			return;
		}
	
		const request = new TCLIServiceTypes.TGetResultSetMetadataReq(executeStatementResponse);
	
		getConnection().GetResultSetMetadata(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		});
	});

	const getSchemas = (sessionHandle) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetSchemasReq({ sessionHandle });

		getConnection().GetSchemas(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		});
	});

	const getPrimaryKeys = (sessionHandle, schemaName, tableName) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetPrimaryKeysReq({ sessionHandle, schemaName, tableName });

		getConnection().GetPrimaryKeys(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		});
	});

	const getCatalogs = (sessionHandle) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetCatalogsReq({ sessionHandle });

		getConnection().GetCatalogs(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		});
	});

	const getColumns = (sessionHandle, schemaName, tableName) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetColumnsReq({ sessionHandle, schemaName, tableName });

		getConnection().GetColumns(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		});
	});
	
	const fetchFirstResult = (operationHandle, maxRows) => {
		return fetchResultRequest({
			orientation: TCLIServiceTypes.TFetchOrientation.FETCH_FIRST,
			operationHandle,
			maxRows
		});
	};
	
	const fetchNextResult = (operationHandle, maxRows) => {
		return fetchResultRequest({
			orientation: TCLIServiceTypes.TFetchOrientation.FETCH_NEXT,
			operationHandle,
			maxRows
		});
	};
	
	const fetchResultRequest = (options) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TFetchResultsReq(options);	
	
		getConnection().FetchResults(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		});	
	});
	
	const isFinish = (status) => {
		return (
			status === TCLIServiceTypes.TOperationState.FINISHED_STATE ||
			status === TCLIServiceTypes.TOperationState.CANCELED_STATE ||
			status === TCLIServiceTypes.TOperationState.CLOSED_STATE ||
			status === TCLIServiceTypes.TOperationState.ERROR_STATE
		);
	};
	
	const getOperationStatus = (operationHandle) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetOperationStatusReq({ operationHandle });
	
		getConnection().GetOperationStatus(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		});
	});
	
	const waitFinish = (operationHandle) => new Promise((resolve, reject) => {
		const repeat = (count) => {
			getOperationStatus(operationHandle)
				.then(res => {
					if (isFinish(res.operationState)) {
						resolve(res);
					} else if (count - 1 <= 0) {
						reject(res);
					} else {
						sleep(1000).then(() => {
							repeat(count - 1);
						});
					}
				})
				.catch(reject);
		};
	
		repeat(5);
	});
	
	const sleep = (timeout) => {
		return new Promise((resolve, reject) => {
			setTimeout(resolve, timeout);
		});
	};

	const getTCLIService = () => [TCLIService, TCLIServiceTypes];

	const connectionsParams = { host, port, authMech, options };
	const request = new TCLIServiceTypes.TOpenSessionReq({
		client_protocol: TCLIServiceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9,
		configuration,
		username,
		password
	});
	const cursor = {
		execute,
		asyncExecute,
		fetchResult,
		getSchema,
		getSchemas,
		getPrimaryKeys,
		getCatalogs,
		getColumns,
		getTCLIService
	};

	getConnection(TCLIService, connectionsParams).OpenSession(request, (err, session) => {
		handler(err, session, cursor);
	});
};

module.exports = {
	connect
};