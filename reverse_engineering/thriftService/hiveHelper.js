const _ = require('lodash');
const Big = require('big.js');

const getInt64 = (buffer, offset) => {
	// This code from Int64 toNumber function. Using Big.js, convert to string.
	const b = buffer;
	const o = offset;

	// Running sum of octets, doing a 2's complement
	const negate = b[o] & 0x80;
	let value = new Big(0);
	let m = new Big(1);
	let carry = 1;

	for (let i = 7; i >= 0; i -= 1) {
	  let v = b[o + i];

	  // 2's complement for negative numbers
	  if (negate) {
		v = (v ^ 0xff) + carry;
		carry = v >> 8;
		v &= 0xff;
	  }

	  value = value.plus((new Big(v)).times(m));
	  m = m.times(256);
	}

	if (negate) {
	  value = value.times(-1);
	}

	return value;
};

const getColumnValueKeyByTypeDescriptor = (TCLIServiceTypes) =>  (typeDescriptor) => {
	switch (typeDescriptor.type) {
		case TCLIServiceTypes.TTypeId.BOOLEAN_TYPE:
			return 'boolVal';
		case TCLIServiceTypes.TTypeId.TINYINT_TYPE:
			return 'byteVal';
		case TCLIServiceTypes.TTypeId.SMALLINT_TYPE:
			return 'i16Val';
		case TCLIServiceTypes.TTypeId.INT_TYPE:
			return 'i32Val';
		case TCLIServiceTypes.TTypeId.BIGINT_TYPE:
		case TCLIServiceTypes.TTypeId.TIMESTAMP_TYPE:
			return 'i64Val';
		case TCLIServiceTypes.TTypeId.FLOAT_TYPE:
		case TCLIServiceTypes.TTypeId.DOUBLE_TYPE:
			return 'doubleVal';
		default:
			return 'stringVal';
	}
};

const noConversion = value => value;
const toString = value => value.toString();
const convertBigInt = value => {
	const result = getInt64(value.buffer, value.offset);
	const max = new Big(Number.MAX_SAFE_INTEGER);

	if (result.cmp(max) > 0) {
		return Number.MAX_SAFE_INTEGER;
	} else {
		return parseInt(result.toString());
	}
};
const toNumber = value => Number(value);
const toJSON = defaultValue => value => {
	try {
		return JSON.parse(value);
	} catch (e) {
		return defaultValue;
	}
};

const getDataConverter = (TCLIServiceTypes) => (typeDescriptor) => {
	switch (typeDescriptor.type) {
		case TCLIServiceTypes.TTypeId.NULL_TYPE:
			return noConversion;
		case TCLIServiceTypes.TTypeId.UNION_TYPE:
		case TCLIServiceTypes.TTypeId.USER_DEFINED_TYPE:
			return toString;

		case TCLIServiceTypes.TTypeId.DECIMAL_TYPE:
			return toNumber;
		case TCLIServiceTypes.TTypeId.STRUCT_TYPE:
		case TCLIServiceTypes.TTypeId.MAP_TYPE:
			return toJSON({});
		case TCLIServiceTypes.TTypeId.ARRAY_TYPE:
			return toJSON([]);
		
		case TCLIServiceTypes.TTypeId.BIGINT_TYPE:
			return convertBigInt;
		case TCLIServiceTypes.TTypeId.TIMESTAMP_TYPE:
		case TCLIServiceTypes.TTypeId.DATE_TYPE:
		case TCLIServiceTypes.TTypeId.BINARY_TYPE:
			return toString;
		case TCLIServiceTypes.TTypeId.INTERVAL_YEAR_MONTH_TYPE:
		case TCLIServiceTypes.TTypeId.INTERVAL_DAY_TIME_TYPE:
		case TCLIServiceTypes.TTypeId.FLOAT_TYPE:
		case TCLIServiceTypes.TTypeId.DOUBLE_TYPE:
		case TCLIServiceTypes.TTypeId.INT_TYPE:
		case TCLIServiceTypes.TTypeId.SMALLINT_TYPE:
		case TCLIServiceTypes.TTypeId.TINYINT_TYPE:
		case TCLIServiceTypes.TTypeId.BOOLEAN_TYPE:
		case TCLIServiceTypes.TTypeId.STRING_TYPE:
		case TCLIServiceTypes.TTypeId.CHAR_TYPE:
		case TCLIServiceTypes.TTypeId.VARCHAR_TYPE:
		default:
			return noConversion;
	}
};

const getTypeDescriptorByColumnDescriptor = (columnDescriptor) => {
	return _.get(columnDescriptor, 'typeDesc.types[0].primitiveEntry', null);
};

const getColumnValuesBySchema = (TCLIServiceTypes) => (columnDescriptor, valuesColumn) => {
	const typeDescriptor = getTypeDescriptorByColumnDescriptor(columnDescriptor);
	const valueType = getColumnValueKeyByTypeDescriptor(TCLIServiceTypes)(typeDescriptor);
	const values = _.get(valuesColumn, `${valueType}.values`, []);

	return values.map(getDataConverter(TCLIServiceTypes)(typeDescriptor));
};

const getColumnName = (columnDescriptor) => {
	const name = columnDescriptor.columnName || '';

	return name.split('.').pop();
};

const getResultParser = (TCLIService, TCLIServiceTypes) => {
	return (schemaResponse, fetchResultResponses) => {
		return fetchResultResponses.reduce((result, fetchResultResponse) => {
			const columnValues = _.get(fetchResultResponse, 'results.columns', []);
			const rows = [...schemaResponse.schema.columns]
				.sort((c1, c2) => c1.position > c2.position ? 1 : c1.position < c2.position ? -1 : 0)
				.reduce((rows, columnDescriptor) => {
					return getColumnValuesBySchema(TCLIServiceTypes)(
						columnDescriptor,
						columnValues[columnDescriptor.position - 1]
					).reduce((result, columnValue, i) => {
						if (!result[i]) {
							result[i] = {};
						}

						result[i][getColumnName(columnDescriptor)] = columnValue;

						return result;
					}, rows);
				}, []);

			return result.concat(rows);
		}, []);
	};
};

const getQualifier = (typeDescriptor, qualifierName, defaultValue) => {
	const result = _.get(typeDescriptor, `typeQualifiers.qualifiers.${qualifierName}`, {});

	return result.i32Value || result.stringValue || defaultValue;
};

const getJsonSchemaByTypeDescriptor = (TCLIServiceTypes) => (typeDescriptor) => {
	switch (typeDescriptor.type) {
		case TCLIServiceTypes.TTypeId.NULL_TYPE:		
		case TCLIServiceTypes.TTypeId.STRING_TYPE:
			return {
				type: "text",
				mode: "string"
			};
		case TCLIServiceTypes.TTypeId.VARCHAR_TYPE:
			return {
				type: "text",
				mode: "varchar",
				maxLength: getQualifier(typeDescriptor, "characterMaximumLength", "")
			};
		case TCLIServiceTypes.TTypeId.CHAR_TYPE:
			return {
				type: "text",
				mode: "char",
				maxLength: getQualifier(typeDescriptor, "characterMaximumLength", "")
			};
		case TCLIServiceTypes.TTypeId.INT_TYPE:
			return {
				type: "numeric",
				mode: "int"
			};
		case TCLIServiceTypes.TTypeId.TINYINT_TYPE:
			return {
				type: "numeric",
				mode: "tinyint"
			};
		case TCLIServiceTypes.TTypeId.SMALLINT_TYPE:
			return {
				type: "numeric",
				mode: "smallint"
			};
		case TCLIServiceTypes.TTypeId.BIGINT_TYPE:
			return {
				type: "numeric",
				mode: "bigint"
			};
		case TCLIServiceTypes.TTypeId.FLOAT_TYPE:
			return {
				type: "numeric",
				mode: "float"
			};
		case TCLIServiceTypes.TTypeId.DOUBLE_TYPE:
			return {
				type: "numeric",
				mode: "double"
			};
		case TCLIServiceTypes.TTypeId.DECIMAL_TYPE:
			return {
				type: "numeric",
				mode: "decimal",
				precision: getQualifier(typeDescriptor, "precision", ""),
				scale: getQualifier(typeDescriptor, "scale", "")
			};
		case TCLIServiceTypes.TTypeId.BOOLEAN_TYPE:
			return {
				type: "bool"
			};
		case TCLIServiceTypes.TTypeId.BINARY_TYPE:
			return {
				type: "binary"
			};
		case TCLIServiceTypes.TTypeId.TIMESTAMP_TYPE:
			return {
				type: "timestamp"
			};
		case TCLIServiceTypes.TTypeId.DATE_TYPE:
			return {
				type: "date"
			};
		case TCLIServiceTypes.TTypeId.ARRAY_TYPE:
			return {
				type: "array",
				subtype: "array<txt>",
				items: []
			};
		case TCLIServiceTypes.TTypeId.MAP_TYPE:
			return {
				type: "map",
				keySubtype: "string",
				subtype: "map<txt>",
				properties: {}
			};
		case TCLIServiceTypes.TTypeId.STRUCT_TYPE:
			return {
				type: "struct",
				keyType: "string",
				subtype: "struct<txt>",
				properties: {}
			};
		case TCLIServiceTypes.TTypeId.INTERVAL_YEAR_MONTH_TYPE:
		case TCLIServiceTypes.TTypeId.INTERVAL_DAY_TIME_TYPE:
			return {
				type: "interval"
			};
		case TCLIServiceTypes.TTypeId.UNION_TYPE:
		case TCLIServiceTypes.TTypeId.USER_DEFINED_TYPE:
		default:
			return noConversion;
	}
};

const getJsonSchemaCreator = (TCLIService, TCLIServiceTypes) => (schemaResp) => {
	const columnDescriptors = _.get(schemaResp, 'schema.columns', []);

	const jsonSchema = columnDescriptors.reduce((jsonSchema, columnDescriptor) => {
		const typeDescriptor = getTypeDescriptorByColumnDescriptor(columnDescriptor);

		jsonSchema.properties[getColumnName(columnDescriptor)] = Object.assign(
			{},
			getJsonSchemaByTypeDescriptor(TCLIServiceTypes)(typeDescriptor),
			{ comments: columnDescriptor.comment || "" }
		);

		return jsonSchema;
	}, {
		$schema: "http://json-schema.org/draft-04/schema#",
		type: "object",
		additionalProperties: false,
		properties: {}
	});

	return jsonSchema;
};

module.exports = {
	getResultParser,
	getJsonSchemaCreator
};