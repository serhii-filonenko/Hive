const { getDatabaseStatement } = require('../databaseHelper')
const { dependencies } = require('../appDependencies');

let _;

const setDependencies = ({ lodash }) => _ = lodash;

const hydrateDrop = container => {
	const { role } = container;
	return role?.code || role?.name;
}; 

const getAddContainerScript = container => {
	const dataContainer = [container.role || {}]
	const containerStatement = getDatabaseStatement(dataContainer);
	return containerStatement;
};

const getDeleteContainerScript = provider => container => {
	const hydratedDrop = hydrateDrop(container);
	return provider.dropDatabase(hydratedDrop);
};

const getModifyContainerScript = provider => container => {
	setDependencies(dependencies);
	const compMod = _.get(container, 'role.compMod', {});
	const getName = type => compMod.code?.[type] || compMod.name?.[type];
	const name = { 
		new: getName('new'),
		old: getName('old')
	};
	if (name.new === name.old) {
		return [];
	}
	const hydratedDrop = hydrateDrop({ role: { ...container?.role || {}, name: name.old }});
	const deletedScript = provider.dropDatabase(hydratedDrop);
	const addedScript = getAddContainerScript({ role: { ...container?.role || {}, name: name.new }});
	
	return [deletedScript, addedScript];
};

module.exports = {
	getAddContainerScript,
	getDeleteContainerScript,
	getModifyContainerScript
}