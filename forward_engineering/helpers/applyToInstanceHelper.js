const { connect } = require('../../reverse_engineering/api');
const { isDeactivatedStatement } = require('./generalHelper');

const applyToInstance = (connectionInfo, logger, app) => {
	const async = app.require('async');
	
	return new Promise((resolve, reject) => {
		const callback = async (err, session, cursor) => {
			if (err) {
				logger.log('error', err, 'Connection failed');
		
				return reject(prepareError(err));
			}
		
			const queries = getQueries(connectionInfo.script);
			
			try {
				await async.mapSeries(queries, async query => {

					const message = `Query: ${query.split('\n').shift().substring(0, 150)}`;
					logger.progress({ message });
					logger.log('info', { message }, 'Apply to instance');
					
					await cursor.asyncExecute(session.sessionHandle, query);
				});
				await cursor.closeSession();
				resolve();
			} catch (error) {
				logger.log('error', { message: error.message, stack: error.stack, error: error }, 'Error applying to instance');
				reject(prepareError(error));
			};	
		};
		
		connect(connectionInfo, logger, callback, app);
	});
};

const getQueries = (script = '') => {
	return script
		.split(';')
		.map(script => script.trim())
		.filter(query => {
			if (Boolean(query)) {
				return !isDeactivatedStatement(query);
			}

			return false;
		});
};

const prepareError = error => {
	error = JSON.stringify(error, Object.getOwnPropertyNames(error));
	return JSON.parse(error);
};

module.exports = {
	applyToInstance
};