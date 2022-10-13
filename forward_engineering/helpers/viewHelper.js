'use strict'

let _;
const { dependencies } = require('./appDependencies');
const { prepareName, commentDeactivatedStatements } = require('./generalHelper');

const setDependencies = ({ lodash }) => _ = lodash;

const itemIsDeactivated = item => item.startsWith('-- ');

const joinLastDeactivatedItem = (items = []) => {
	const activatedItems = _.dropRightWhile(items, itemIsDeactivated);
	if (activatedItems.length === items.length || activatedItems.length === 0) {
		return items;
	}
	
	const deactivatedItems = items.slice(activatedItems.length);
	return [...activatedItems.slice(0, -1), _.last(activatedItems) + ' -- ', ...deactivatedItems];
};

const areAllColumnsDeactivated = (columns = []) => columns.length && columns.every(itemIsDeactivated);

const getColumnNames = (collectionRefsDefinitionsMap, columns, isViewActivated) => {
	return _.uniq(Object.keys(columns).map(name => {
		const id = _.get(columns, [name, 'GUID']);

		const itemDataId = Object.keys(collectionRefsDefinitionsMap).find(viewFieldId => {
			const definitionData = collectionRefsDefinitionsMap[viewFieldId];

			return definitionData.definitionId === id;
		});
		const isActivated = isViewActivated ? _.get(columns[name], 'isActivated') : true;
		const itemData = collectionRefsDefinitionsMap[itemDataId] || {};
		if (!itemData.name || itemData.name === name) {
			return commentDeactivatedStatements(prepareName(itemData.name), isActivated);
		}
		const collection = _.first(itemData.collection) || {};
		const collectionName = collection.collectionName || collection.code;
		const columnName = `${prepareName(collectionName)}.${prepareName(itemData.name)} as ${prepareName(name)}`;

		return commentDeactivatedStatements(columnName, isActivated);
	})).filter(_.identity);
};

const getFromStatement = (collectionRefsDefinitionsMap, columns) => {
	const sourceCollections = _.uniq(Object.keys(columns).map(name => {
		const refId = columns[name].refId;
		const source = collectionRefsDefinitionsMap[refId];
		const collection = _.first(source.collection) || {};
		if (_.isEmpty(collection)) {
			return;
		}
		const bucket = _.first(source.bucket) || {};
		const collectionName = prepareName(collection.collectionName || collection.code);
		const bucketName = prepareName(bucket.name || bucket.code || '');
		const fullCollectionName = bucketName ? `${bucketName}.${collectionName}` : `${collectionName}`;

		return fullCollectionName;
	})).filter(Boolean);
	if (_.isEmpty(sourceCollections)) {
		return '';
	}

	return 'FROM ' + sourceCollections.join(' INNER JOIN ');
};

const retrivePropertyFromConfig = (config, tab, propertyName, defaultValue = "") => ((config || [])[tab] || {})[propertyName] || defaultValue;

const retrieveContainerName = (containerConfig) => retrivePropertyFromConfig(
		containerConfig, 0, "code", 
		retrivePropertyFromConfig(containerConfig, 0, "name", "")	
	);
module.exports = {
	getViewScript({
		schema,
		viewData,
		containerData,
		collectionRefsDefinitionsMap,
	}) {
		setDependencies(dependencies);
		let script = [];
		const columns = schema.properties || {};
		const view = _.first(viewData) || {};
		
		const bucketName = prepareName(retrieveContainerName(containerData));
		const viewName = prepareName(view.code || view.name);
		const isMaterialized = schema.materialized;
		const ifNotExist = view.ifNotExist;
		const orReplace = view.orReplace;
		const ifNotExists = view.ifNotExist;
		const fromStatement = getFromStatement(collectionRefsDefinitionsMap, columns);
		const name = bucketName ? `${bucketName}.${viewName}` : `${viewName}`;
		const createStatement = `CREATE ${(orReplace && !ifNotExists) ? 'OR REPLACE ' : ''}${isMaterialized ? 'MATERIALIZED ' : ''}VIEW ${ifNotExist ? 'IF NOT EXISTS ' : ''}${name}`;

		script.push(createStatement);

		if (schema.selectStatement) {
			return createStatement + ' ' + schema.selectStatement + ';\n\n';
		}

		if (_.isEmpty(columns)) {
			return;
		}

		const columnsNames = getColumnNames(collectionRefsDefinitionsMap, columns, view.isActivated);
		const allColumnsAreDeactivated = areAllColumnsDeactivated(columnsNames);
	
		if (allColumnsAreDeactivated) {
			return;
		}
	
		const joinedColumns = joinLastDeactivatedItem(columnsNames).join(',\n');
		script.push(`AS SELECT ${joinedColumns}`);
		script.push(fromStatement);


		return commentDeactivatedStatements(script.join('\n  ')  + ';', view.isActivated);
	}
};
