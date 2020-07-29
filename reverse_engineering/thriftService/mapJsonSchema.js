
const mapJsonSchema = (_) => (jsonSchema, parentJsonSchema, callback, key) => {
	const mapProperties = (properties, mapper) =>
		Object.keys(properties).reduce((newProperties, propertyName) => {
			return { ...newProperties, [propertyName]: mapper(properties[propertyName], propertyName) };
		}, {});
	const mapItems = (items, mapper) => {
		if (Array.isArray(items)) {
			return items.map((jsonSchema, i) => mapper(jsonSchema, i));
		} else if (_.isPlainObject(items)) {
			return mapper(items, 0);
		} else {
			return items;
		}
	};
	const applyTo = (properties, jsonSchema, mapper) => {
		return properties.reduce((jsonSchema, propertyName) => {
			if (!jsonSchema[propertyName]) {
				return jsonSchema;
			}

			return Object.assign({}, jsonSchema, {
				[propertyName]: mapper(jsonSchema[propertyName], propertyName),
			});
		}, jsonSchema);
	};
	if (!_.isPlainObject(jsonSchema)) {
		return jsonSchema;
	}
	const copyJsonSchema = Object.assign({}, jsonSchema);
	const mapper = _.partial(mapJsonSchema(_), _, copyJsonSchema, callback);
	const propertiesLike = ['properties', 'definitions', 'patternProperties'];
	const itemsLike = ['items', 'oneOf', 'allOf', 'anyOf', 'not'];

	const jsonSchemaWithNewProperties = applyTo(propertiesLike, copyJsonSchema, _.partial(mapProperties, _, mapper));
	const newJsonSchema = applyTo(itemsLike, jsonSchemaWithNewProperties, _.partial(mapItems, _, mapper));

	return callback(newJsonSchema, parentJsonSchema, key);
};

module.exports = mapJsonSchema;
