const { dependencies } = require('../appDependencies');
const { getViewScript } = require('../viewHelper');
const { getEntityData, getEntityProperties, getContainerName, generateFullEntityName, getEntityName } = require('./generalHelper');

let _;
const setDependencies = ({ lodash }) => _ = lodash;

const viewProperties = ['tableProperties', 'viewTemporary', 'viewOrReplace', 'isGlobal', 'description', 'name', 'code'];

const prepareColumnGuids = columns => 
	Object.entries(columns).reduce((columns, [name, value = {}]) => ({
			...columns,
			[name]: {
				...value,
				GUID: value.refId || '',
			}
		}), {});

const prepareRefsDefinitionsMap = definitions => 
	Object.entries(definitions).reduce((columns, [definitionId, value = {}]) => ({
		...columns,
		[definitionId]: {
			...value,
			definitionId,
		}
	}), {});

const hydrateView = view => {
	const compMod = _.get(view, 'role.compMod', {});
	const properties = prepareColumnGuids(getEntityProperties(view));
	const roleData = getEntityData(compMod, viewProperties);
	const schema = { ..._.get(view, 'role', {}), ...roleData, properties };
	const collectionRefsDefinitionsMap = prepareRefsDefinitionsMap(schema.compMod?.collectionData?.collectionRefsDefinitionsMap || {});
	return {
		schema,
		collectionRefsDefinitionsMap,
		viewData: [schema],
		containerData: [{ name: getContainerName(compMod) }],
	};
};

const hydrateAlterView = (view, code) => ({
	...view,
	role: { ...view.role || {}, code }
})

const getAddViewsScripts = view => {
	setDependencies(dependencies);
	const hydratedView = hydrateView(view);
	return getViewScript(hydratedView);
};

const getDeleteViewsScripts = provider => view => {
	const name = generateFullEntityName(view);
	const isMaterialized = view.role?.materialized;
	return provider.dropView({ name, isMaterialized });
};

const getModifyViewsScripts = provider => view => {
	setDependencies(dependencies);
	const compMod = view.role?.compMod || {};
	const viewName = getEntityName(compMod, 'name');
	if (viewName.newName === viewName.oldName) {
		return;
	}
	const dropViewScript = getDeleteViewsScripts(provider)(hydrateAlterView(view, viewName.oldName));
	const hydratedView = hydrateView(hydrateAlterView(view, viewName.newName));
	const addViewScript = getViewScript(hydratedView);
	return [dropViewScript, addViewScript];
};

module.exports = {
	getAddViewsScripts,
	getDeleteViewsScripts,
	getModifyViewsScripts,
}