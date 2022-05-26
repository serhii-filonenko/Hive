'use strict'

const jsonSchemaHelper = require('./jsonSchemaHelper');

const filterPaths = (keys, paths) => paths.filter(path => keys.find(key => path[path.length - 1] === key.keyId));
const sortedKey = getNameByPath => (keys, paths) => {
	return keys.map(key => {
		const path = paths.find(path => path[path.length - 1] === key.keyId);

		return {
			name: getNameByPath(path),
			type: key.type === 'ascending' ? 'ASC' : 'DESC'
		};
	});
};

const findName = (keyId, properties) => {
	return Object.keys(properties).find(name => properties[name].GUID === keyId);
};

const checkIfActivated = (keyId, properties) => {
	return (Object.values(properties).find(prop => prop.GUID === keyId) || {})['isActivated'] || true;
};

const getKeys = (keys, jsonSchema) => {
	return (keys || []).map(key => {
		return {
			name: findName(key.keyId, jsonSchema.properties),
			isActivated: checkIfActivated(key.keyId, jsonSchema.properties),
		};
	});
};

const hydrateUniqueKeys = jsonSchema => {
	const hydrate = options => ({
			keyType: 'UNIQUE',
			name: options['constraintName'] || '',
			rely: options['rely'] ? ` ${options['rely']}` : '',
	});

	return (jsonSchema.uniqueKey || [])
		.filter(uniqueKey => Boolean((uniqueKey.compositeUniqueKey || []).length))
		.map(uniqueKey => ({
			...hydrate(uniqueKey),
			columns: getKeys(uniqueKey.compositeUniqueKey, jsonSchema),
		}));

};

const getKeyNames = (tableData, jsonSchema, definitions, areColumnConstraintsAvailable) => {
	const compositeClusteringKey = tableData.compositeClusteringKey || [];
	const compositePartitionKey = tableData.compositePartitionKey || [];
	const skewedby = tableData.skewedby || [];
	const sortedByKey = tableData.sortedByKey || [];

	const ids = [
		...compositeClusteringKey,
		...compositePartitionKey,
		...skewedby,
		...sortedByKey,
	].map(key => key.keyId);

	const keysPaths = jsonSchemaHelper.getPathsByIds(ids, [jsonSchema, ...definitions]);
	const primaryKeysPath = jsonSchemaHelper.getPrimaryKeys(jsonSchema, areColumnConstraintsAvailable)
		.filter(pkPath => !keysPaths.find(path => path[path.length - 1] === pkPath[pkPath.length - 1]));
	const idToNameHashTable = jsonSchemaHelper.getIdToNameHashTable([jsonSchema, ...definitions]);
	const getNameByPath = jsonSchemaHelper.getNameByPath.bind(null, idToNameHashTable);

	return {
		primaryKeys: primaryKeysPath.map(getNameByPath),
		compositeClusteringKey: filterPaths(compositeClusteringKey, keysPaths).map(getNameByPath),
		compositePartitionKey: filterPaths(compositePartitionKey, keysPaths).map(getNameByPath),
		skewedby: filterPaths(skewedby, keysPaths).map(getNameByPath),
		sortedByKey: sortedKey(getNameByPath)(sortedByKey, filterPaths(sortedByKey, keysPaths))
	};
};

const getUniqueKeyStatement = (jsonSchema, isParentItemActivated) => {
	const getStatement = ({ keys, rely, name, keyType }) => `CONSTRAINT ${name} ${keyType} (${keys}) DISABLE NOVALIDATE${rely}`;
	const getColumnsName = columns => columns.map(column => column.name).join(', ');
	const hydratedUniqueKeys = hydrateUniqueKeys(jsonSchema);
	const constraintsStatement = hydratedUniqueKeys.map(uniqueKey => {
		const columns = uniqueKey.columns;
		if (!Array.isArray(columns) || !columns.length) {
			return '';
		}

		const columnsName = getColumnsName(columns);
	
		if (!isParentItemActivated) {
			return getStatement({ keys: columnsName, ...uniqueKey });
		}

		const isActivatedColumnsName = getColumnsName(columns.filter(column => column.isActivated));

		if (!Boolean(isActivatedColumnsName.length)) {
			return '-- ' + getStatement({ keys: columnsName, ...uniqueKey });
		}
		return getStatement({ keys: isActivatedColumnsName, ...uniqueKey });
	});

	return constraintsStatement.join(',\n');
};

module.exports = {
	getKeyNames,
	getUniqueKeyStatement,
};
