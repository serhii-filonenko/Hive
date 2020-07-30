'use strict'

const { getDatabaseStatement } = require('./helpers/databaseHelper');
const { getTableStatement } = require('./helpers/tableHelper');
const { getIndexes } = require('./helpers/indexHelper');
const foreignKeyHelper = require('./helpers/foreignKeyHelper');
let _;
const sqlFormatter = require('sql-formatter');

module.exports = {
	generateScript(data, logger, callback, app) {
		try {
			initDependencies(app);
			const jsonSchema = JSON.parse(data.jsonSchema);
			const modelDefinitions = JSON.parse(data.modelDefinitions);
			const internalDefinitions = JSON.parse(data.internalDefinitions);
			const externalDefinitions = JSON.parse(data.externalDefinitions);
			const containerData = data.containerData;
			const entityData = data.entityData;
			const areColumnConstraintsAvailable = data.modelData[0].dbVersion.startsWith('3');
			const areForeignPrimaryKeyConstraintsAvailable = !data.modelData[0].dbVersion.startsWith('1');
			const needMinify = (_.get(data, 'options.additionalOptions', []).find(option => option.id === 'minify') || {}).value;
			
			callback(null, buildScript(needMinify)(
				getDatabaseStatement(containerData),
				getTableStatement(
					containerData,
					entityData,
					jsonSchema,
					[
						modelDefinitions,
						internalDefinitions,
						externalDefinitions
					],
					null,
					areColumnConstraintsAvailable,
					areForeignPrimaryKeyConstraintsAvailable
				),
				getIndexes(containerData, entityData, jsonSchema, [
					modelDefinitions,
					internalDefinitions,
					externalDefinitions
				])
			));
		} catch (e) {
			logger.log('error', { message: e.message, stack: e.stack }, 'Hive Forward-Engineering Error');

			setTimeout(() => {
				callback({ message: e.message, stack: e.stack });
			}, 150);
		}
	},

	generateContainerScript(data, logger, callback, app) {
		try {
			initDependencies(app);
			const containerData = data.containerData;
			const modelDefinitions = JSON.parse(data.modelDefinitions);
			const externalDefinitions = JSON.parse(data.externalDefinitions);
			const databaseStatement = getDatabaseStatement(containerData);
			const jsonSchema = parseEntities(data.entities, data.jsonSchema);
			const internalDefinitions = parseEntities(data.entities, data.internalDefinitions);
			const areColumnConstraintsAvailable = data.modelData[0].dbVersion.startsWith('3');
			const areForeignPrimaryKeyConstraintsAvailable = !data.modelData[0].dbVersion.startsWith('1');
			const needMinify = (_.get(data, 'options.additionalOptions', []).find(option => option.id === 'minify') || {}).value;
			const foreignKeyHashTable = foreignKeyHelper.getForeignKeyHashTable(
				data.relationships,
				data.entities,
				data.entityData,
				jsonSchema,
				internalDefinitions,
				[
					modelDefinitions,
					externalDefinitions
				]
			);

			const entities = data.entities.reduce((result, entityId) => {
				const args = [
					containerData,
					data.entityData[entityId],
					jsonSchema[entityId], [
						internalDefinitions[entityId],
						modelDefinitions,
						externalDefinitions
					]
				];

				return result.concat([
					getTableStatement(...args, null, areColumnConstraintsAvailable, areForeignPrimaryKeyConstraintsAvailable),
					getIndexes(...args),
				]);
			}, []);

			const foreignKeys = getForeignKeys(data, foreignKeyHashTable, areForeignPrimaryKeyConstraintsAvailable);

			callback(null, buildScript(needMinify)(
				databaseStatement,
				...entities,
				foreignKeys
			));
		} catch (e) {
			logger.log('error', { message: e.message, stack: e.stack }, 'Cassandra Forward-Engineering Error');

			setTimeout(() => {
				callback({ message: e.message, stack: e.stack });
			}, 150);
		}
	}
};

const buildScript = needMinify => (...statements) => {
	const script = statements.filter(statement => statement).join('\n\n');
	if (needMinify) {
		return script;
	}

	return sqlFormatter.format(script);
};

const parseEntities = (entities, serializedItems) => {
	return entities.reduce((result, entityId) => {
		try {
			return Object.assign({}, result, { [entityId]: JSON.parse(serializedItems[entityId]) });
		} catch (e) {
			return result;
		}
	}, {});
};

const getForeignKeys = (data, foreignKeyHashTable, areForeignPrimaryKeyConstraintsAvailable) => {
	if (!areForeignPrimaryKeyConstraintsAvailable) {
		return null;
	}
	return data.entities.reduce((result, entityId) => {
		const foreignKeyStatement = foreignKeyHelper.getForeignKeyStatementsByHashItem(foreignKeyHashTable[entityId] || {});
	
		if (foreignKeyStatement) {foreignKeyStatement
			return [...result, foreignKeyStatement];
		}

		return result;
	}, []).join('\n');
}

const initDependencies = app => {
	_ = app.require('lodash');
};
