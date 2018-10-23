'use strict'

const { buildStatement, getName, getTab, indentString } = require('./generalHelper');
const { getColumnsStatement } = require('./columnHelper');

// CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name    -- (Note: TEMPORARY available in Hive 0.14.0 and later)
//   [(col_name data_type [COMMENT col_comment], ... [constraint_specification])]
//   [COMMENT table_comment]
//   [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
//   [CLUSTERED BY (col_name, col_name, ...) [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]
//   [SKEWED BY (col_name, col_name, ...)                  -- (Note: Available in Hive 0.10.0 and later)]
//      ON ((col_value, col_value, ...), (col_value, col_value, ...), ...)
//      [STORED AS DIRECTORIES]
//   [
//    [ROW FORMAT row_format] 
//    [STORED AS file_format]
//      | STORED BY 'storage.handler.class.name' [WITH SERDEPROPERTIES (...)]  -- (Note: Available in Hive 0.6.0 and later)
//   ]
//   [LOCATION hdfs_path]
//   [TBLPROPERTIES (property_name=property_value, ...)]   -- (Note: Available in Hive 0.6.0 and later)
//   [AS select_statement];

const getCreateStatement = ({
	dbName, tableName, isTemporary, isExternal, columnStatement, comment, partitionedByKeys, clusteredStatement, skewedStatement,
	rowFormatStatement, storedAsStatement, location, tableProperties, selectStatement
}) => {
	const temporary = isTemporary ? 'TEMPORARY' : '';
	const external = isExternal ? 'EXTERNAL' : '';
	const tempExtStatement = [temporary, external].filter(d => d).join(' ');

	return buildStatement(`CREATE ${tempExtStatement} TABLE IF NOT EXISTS ${dbName}.${tableName}`)
		(columnStatement, `(\n${indentString(columnStatement)}\n)`)
		(comment, `COMMENT "${comment}"`)
		(partitionedByKeys, `PARTITIONED BY (${partitionedByKeys})`)
		(clusteredStatement, clusteredStatement)
		(skewedStatement, skewedStatement)
		(rowFormatStatement, `ROW FORMAT ${rowFormatStatement}`)
		(storedAsStatement, storedAsStatement)
		(location, `LOCATION "${location}"`)
		(tableProperties, `TBLPROPERTIES ${tableProperties}`)
		(selectStatement, `AS ${selectStatement}`)
		() + ';';
};

const getTableStatement = (containerData, entityData, jsonSchema) => {
	const dbName = getName(getTab(0, containerData));
	const tableData = getTab(0, entityData);
	const tableName = getName(tableData);
	const indexes = getTab(1, entityData).SecIndxs || {};

	const tableStatement = getCreateStatement({
		dbName,
		tableName,
		isTemporary: tableData.temporaryTable,
		isExternal: tableData.externalTable,
		columnStatement: getColumnsStatement(jsonSchema),
		comment: tableData.comments,
		partitionedByKeys: '',
		clusteredStatement: '',
		skewedStatement: '',
		rowFormatStatement: '',
		storedAsStatement: '',
		location: tableData.location,
		tableProperties: tableData.tableProperties,
		selectStatement: ''
	});

	return tableStatement;
};

module.exports = {
	getTableStatement
};
