const { setDependencies, dependencies } = require('./appDependencies');
const mapJsonSchema = require('./thriftService/mapJsonSchema');

const adaptJsonSchema = (data, logger, callback, app) => {
	try {
		setDependencies(app);
		_ = dependencies.lodash;
		const jsonSchema = JSON.parse(data.jsonSchema);
		const result = mapJsonSchema(_)(jsonSchema, {}, (schema, parentJsonSchema, key) => {
			if (Array.isArray(schema.type)) {
				clearOutRequired(parentJsonSchema, key);
				const noNullType = schema.type.filter(type => type !== 'null');
				return {
					...schema,
					type: noNullType.length === 1 ? noNullType[0] : noNullType,
				};
			} else if (schema.type === 'array' && !schema.subtype) {
				return {
					...schema,
					subtype: getArraySubtypeByChildren(_, schema),
				};
			} else if (schema.type === 'null') {
				clearOutRequired(parentJsonSchema, key);

				return;
			} else {
				return schema;
			}
		});

		callback(null, {
			...data,
			jsonSchema: JSON.stringify(result),
		});
	} catch (error) {
		const err = {
			message: error.message,
			stack: error.stack,
		};
		logger.log('error', err, 'Remove nulls from JSON Schema');
		callback(err);
	}
};

const clearOutRequired = (parentJsonSchema, key) => {
	if (!Array.isArray(parentJsonSchema.required)) {
		return;
	}
	parentJsonSchema.required = parentJsonSchema.required.filter(propertyName => propertyName !== key);
};

const getArraySubtypeByChildren = (_, arraySchema) => {
	const subtype = type => `array<${type}>`;

	if (!arraySchema.items) {
		return;
	}

	if (Array.isArray(arraySchema.items) && _.uniq(arraySchema.items.map(item => item.type)).length > 1) {
		return subtype('union');
	}

	let item = Array.isArray(arraySchema.items) ? arraySchema.items[0] : arraySchema.items;

	if (!item) {
		return;
	}

	switch (item.type) {
		case 'string':
		case 'text':
			return subtype('txt');
		case 'number':
		case 'numeric':
			return subtype('num');
		case 'interval':
			return subtype('intrvl');
		case 'object':
		case 'struct':
			return subtype('struct');
		case 'array':
			return subtype('array');
		case 'map':
			return subtype('map');
		case 'union':
			return subtype('union');
		case 'timestamp':
			return subtype('ts');
		case 'date':
			return subtype('date');
	}

	if (item.items) {
		return subtype('array');
	}

	if (item.properties) {
		return subtype('struct');
	}
};

module.exports = { adaptJsonSchema };
