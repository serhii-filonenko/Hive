const { dependencies } = require('../appDependencies');
const { getTab } = require('../generalHelper');
const { getKeyNames } = require('../keyHelper');
const { isEqualProperty } = require('./generalHelper');

let _;
const setDependencies = ({ lodash }) => _ = lodash;

const hydrateKeys = (hydratedCollectionData, collection, definitions, fullCollectionName) => {
	setDependencies(dependencies);
	const compMod = _.get(collection, 'role.compMod', {});
	const [__, entityData, jsonSchema] = hydratedCollectionData;
	const keys = getKeyNames(getTab(0, entityData), jsonSchema, definitions);
	const skewedBy = collection?.role?.skewedBy;
	const skewedOn = collection?.role?.skewedOn;
	const skewedByData = {
		isChange: !isEqualProperty(compMod, 'skewedby') || !isEqualProperty(compMod, 'skewedOn'),
		skewedBy: keys.skewedby.join(', '),
		skewedOn: skewedOn || '',
		notSkewed: !skewedBy && !skewedOn && compMod?.skewedby?.old,
		storedAsDirectories: {
			changed: !isEqualProperty(compMod, 'skewStoredAsDir'),
			value: collection?.role?.skewStoredAsDir
		}
	};
	const clusteringKeyData = {
		isChange: !isEqualProperty(compMod, 'sortedByKey') || !isEqualProperty(compMod, 'compositeClusteringKey'),
		compositeClusteringKey: keys.compositeClusteringKey.join(', '),
		sortedByKey: keys.sortedByKey.map(sortedKey => `${sortedKey.name} ${sortedKey.type}`).join(', '),
		intoBuckets: collection?.role?.numBuckets,
	}
	return {
		clusteringKeyData,
		skewedByData,
		name: fullCollectionName
	}
};

module.exports = {
	hydrateKeys,
};