'use strict'

const { getDatabaseStatement } = require('./helpers/databaseHelper');
const { getTableStatement } = require('./helpers/tableHelper');

module.exports = {
	generateScript(data, logger, callback) {
		try {
			const jsonSchema = JSON.parse(data.jsonSchema);
			const modelDefinitions = JSON.parse(data.modelDefinitions);
			const internalDefinitions = JSON.parse(data.internalDefinitions);
			const externalDefinitions = JSON.parse(data.externalDefinitions);
			const containerData = data.containerData;
			const entityData = data.entityData;
			
			callback(null, buildScript(
				getDatabaseStatement(containerData),
				getTableStatement(containerData, entityData, jsonSchema, [
					modelDefinitions,
					internalDefinitions,
					externalDefinitions
				]),
				JSON.stringify(jsonSchema, null, 2),
				JSON.stringify(data, null, 2)
			));
		} catch (e) {
			logger.log('error', { message: e.message, stack: e.stack }, 'Hive Forward-Engineering Error');

			setTimeout(() => {
				callback({ message: e.message, stack: e.stack });
			}, 150);
		}
	},

	generateContainerScript(data, logger, callback) {
		try {
			callback(null, JSON.stringify(data));
		} catch (e) {
			logger.log('error', { message: e.message, stack: e.stack }, 'Cassandra Forward-Engineering Error');

			setTimeout(() => {
				callback({ message: e.message, stack: e.stack });
			}, 150);
		}
	}
};

const buildScript = (...statements) => {
	return statements.join('\n\n');
};
