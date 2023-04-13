module.exports = {
	dropView: 'DROP VIEW IF EXISTS ${name};',

	dropMaterializedView: 'DROP MATERIALIZED VIEW ${name};',

	dropDatabase: 'DROP DATABASE IF EXISTS ${name};',

	alterViewName: 'ALTER VIEW ${oldName} RENAME TO ${newName};',

	dropTableIndex: 'DROP INDEX IF EXISTS ${indexName} ON ${name};',

	dropTable: 'DROP TABLE IF EXISTS ${name};',

	setViewProperties: 'ALTER VIEW ${name} SET TBLPROPERTIES (${properties});',

	unsetViewProperties: 'ALTER VIEW ${name} UNSET TBLPROPERTIES IF EXISTS (${properties});',

	alterViewStatement: 'ALTER VIEW ${name} AS ${query};',

	alterTableName: 'ALTER TABLE ${oldName} RENAME TO ${newName};',

	alterTableColumnName: 'ALTER TABLE ${collectionName} CHANGE ${oldName} ${newName} ${type};',

	alterTableColumnNameWithComment: 'ALTER TABLE ${collectionName} CHANGE ${oldName} ${newName} ${type} COMMENT "${comment}";',

	addTableColumns: 'ALTER TABLE ${name} ADD COLUMNS (${columns});',

	setTableProperties: 'ALTER TABLE ${name} SET TBLPROPERTIES (${properties});',

	alterSerDeProperties: 'ALTER TABLE ${name} SET SERDE ${serDe} WITH SERDEPROPERTIES (${properties});',
	
	alterSerDePropertiesOnlySerDe: 'ALTER TABLE ${name} SET SERDE ${serDe};',

	alterSerDePropertiesWithOutSerDE: 'ALTER TABLE ${name} SET SERDEPROPERTIES (${properties});',
	
	unsetSerDeProperties: 'ALTER TABLE ${name} UNSET SERDEPROPERTIES (${properties});',

	alterTableClusteringKey: 'ALTER TABLE ${name} CLUSTERED BY (${keys}) SORTED BY (${sortedByKey}) INTO ${intoBuckets} BUCKETS;',
	
	alterTableClusteringKeyWithSortedKey: 'ALTER TABLE ${name} CLUSTERED BY (${keys}) INTO ${intoBuckets} BUCKETS;',

	alterTableSkewBy: 'ALTER TABLE ${name} SKEWED BY (${skewedBy}) ON (${skewedOn}), STORED AS DIRECTORIES;',
	
	alterTableSkewByWithoutDirection: 'ALTER TABLE ${name} SKEWED BY (${skewedBy}) ON (${skewedOn});',

	dropSkewBy: 'ALTER TABLE ${name} NOT SKEWED;',

	dropSkewByStoredAsDirection: 'ALTER TABLE ${name} NOT STORED AS DIRECTORIES;',

	setTableLocation: 'ALTER TABLE ${name} SET LOCATION "${location}";',

};
