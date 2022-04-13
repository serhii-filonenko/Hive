const templates = require('./config/templates');

module.exports = app => {
	const { assignTemplates } = app.require('@hackolade/ddl-fe-utils');
	return {
		dropView({ name, isMaterialized }) {
			const dropTemplate = isMaterialized ? templates.dropMaterializedView : templates.dropView;
			return assignTemplates(dropTemplate, { name });
		},

		dropTableIndex(indexes = []) {
			return indexes.map(({ name, indexName }) => 
				name && indexName ? assignTemplates(templates.dropTableIndex, { name, indexName }) : ''
			)
		},

		dropTable(name) {
			return assignTemplates(templates.dropTable, { name });
		},

		alterTable(data) {
			const { alterTableName, tableProperties, serDeProperties, keys } = data;
			let script = [this.alterTableName(alterTableName || {})];
			script = script.concat(this.alterTableProperties(tableProperties || {}));
			script = script.concat(this.alterSerDeProperties(serDeProperties || {}));
			script = script.concat(this.alterTableKeys(keys || {}));
			script = script.concat(this.alterTableSkewedBy(keys || {}));
			return script.concat(this.setTableLocation(data));
		},

		alterTableName({ oldName, newName }) {
			return !oldName || !newName ? '' : assignTemplates(templates.alterTableName, { oldName, newName });
		},

		alterTableColumnName({ collectionName, columns } = {}) {
			if (!collectionName) {
				return [];
			}
			const columnsScripts = columns.map(({ oldName, newName, type, comment }) => {
				if (!oldName && !newName && !type) {
					return ''
				}
				return comment ? 
					assignTemplates(templates.alterTableColumnNameWithComment, { collectionName, oldName, newName, type, comment }) :
					assignTemplates(templates.alterTableColumnName, { collectionName, oldName, newName, type });
			});
			return columnsScripts.filter(Boolean);
		},

		alterTableProperties({ dataProperties, name }) {
			if (!name) {
				return '';
			}
			const { add: addProperties = '' } = dataProperties;
			return addProperties.length ? assignTemplates(templates.setTableProperties, { name, properties: addProperties }) : '';
		},
		
		setTableProperties({ name, properties } = {}) {
			return !name || !properties ? '' : assignTemplates(templates.setTableProperties, { name, properties });
		},
		
		addTableColumns({ name, columns }) {
			return !name || !columns ? '' : assignTemplates(templates.addTableColumns, { name, columns });
		},

		dropDatabase(name) {
			return !name ? '' : assignTemplates(templates.dropDatabase, { name });
		},

		alterSerDeProperties({ properties, serDe, name }) {
			if (!name) {
				return [];
			}
			const { add, drop } = properties?.dataProperties || {};
			let script = [];

			if (add) {
				script = script.concat(serDe ?
					assignTemplates(templates.alterSerDeProperties, { name, serDe, properties: add }) :
					assignTemplates(templates.alterSerDePropertiesWithOutSerDE, { name, properties: add })
				);
			} else if (!add && serDe) {
				script = script.concat(assignTemplates(templates.alterSerDePropertiesOnlySerDe, { name, serDe }));
			}
			if (drop) {
				script = script.concat(assignTemplates(templates.unsetSerDeProperties, { name, properties: drop }));
			}

			return script;
		},

		alterView({ dataProperties, name }) {
			const { add: properties = '' } = dataProperties || {};
			if (!name) {
				return '';
			}

			return properties.length ? assignTemplates(templates.setViewProperties, { name, properties }) : '';
		},

		alterTableKeys(data) {
			const { compositeClusteringKey: keys, sortedByKey, intoBuckets } = data.clusteringKeyData;
			const name = data?.name;
			if (!name || !keys || !intoBuckets) {
				return '';
			}
			return sortedByKey.length ? 
				assignTemplates(templates.alterTableClusteringKey, { keys, sortedByKey, intoBuckets }) :
				assignTemplates(templates.alterTableClusteringKeyWithSortedKey, { keys, intoBuckets })
			;
		},

		alterTableSkewedBy(data) {
			const { skewedBy, skewedOn, notSkewed, storedAsDirectories, isChange } = data.skewedByData || {};
			const name = data?.name;

			if (!name) {
				return '';
			}
			let script = [];

			if (notSkewed) {
				return assignTemplates(templates.dropSkewBy, {name});
			}
			if (storedAsDirectories?.changed && !storedAsDirectories?.value) {
				script = script.concat(assignTemplates(templates.dropSkewByStoredAsDirection, { name }));
			}
			if (!isChange) {
				return script;
			}

			return storedAsDirectories?.value ?
				[...script, assignTemplates(templates.alterTableSkewBy, { name, skewedBy, skewedOn })] :
				[...script, assignTemplates(templates.alterTableSkewByWithoutDirection, { name, skewedBy, skewedOn })];
		},

		setTableLocation({ name, location }) {
			return name && location ? assignTemplates(templates.setTableLocation, { name, location }) : '';
		}

	}
};