const {
    set,
    findEntityIndex,
    getCaseInsensitiveKey,
    omitCaseInsensitive,
    isEqualCaseInsensitive,
    remove,
    merge,
    getCurrentBucket,
} = require('./helpers/commandsHelper');

const _ = require('lodash');

const CREATE_COLLECTION_COMMAND = 'createCollection';
const REMOVE_COLLECTION_COMMAND = 'removeCollection';
const CREATE_BUCKET_COMMAND = 'createBucket';
const REMOVE_BUCKET_COMMAND = 'removeBucket';
const USE_BUCKET_COMMAND = 'useBucket';
const ADD_FIELDS_TO_COLLECTION_COMMAND = 'addFieldsToCollection';
const ADD_COLLECTION_LEVEL_INDEX_COMMAND = 'addCollectionLevelIndex';
const RENAME_FIELD_COMMAND = 'renameField';
const CREATE_VIEW_COMMAND = 'createView';
const ADD_BUCKET_DATA_COMMAND = 'addBucketData';
const REMOVE_COLLECTION_LEVEL_INDEX_COMMAND = 'removeCollectionLevelIndex';
const ADD_RELATIONSHIP_COMMAND = 'addRelationship';
const UPDATE_ENTITY_COLUMN = 'updateColumn';
const CREATE_RESOURCE_PLAN = 'createResourcePlan';
const CREATE_TRIGGER = 'createTrigger';
const CREATE_POOL = 'createPool';
const CREATE_MAPPING = 'createMapping';

const DEFAULT_BUCKET = 'New database';

const convertCommandsToEntities = (commands, originalScript) => {
    return commands.reduce(
        (entitiesData, statementData) => {
            const command = statementData && statementData.type;

            if (!command) {
                return entitiesData;
            }

            const bucket = statementData.bucketName || entitiesData.currentBucket;
            if (command === CREATE_COLLECTION_COMMAND) {
                return createCollection(entitiesData, bucket, statementData);
            }

            if (command === REMOVE_COLLECTION_COMMAND) {
                return removeCollection(entitiesData, bucket, statementData);
            }

            if (command === CREATE_BUCKET_COMMAND) {
                return createBucket(entitiesData, statementData);
            }

            if (command === REMOVE_BUCKET_COMMAND) {
                return removeBucket(entitiesData, statementData);
            }

            if (command === USE_BUCKET_COMMAND) {
                return useBucket(entitiesData, statementData);
            }

            if (command === ADD_FIELDS_TO_COLLECTION_COMMAND) {
                return addFieldsToCollection(entitiesData, bucket, statementData);
            }

            if (command === RENAME_FIELD_COMMAND) {
                return renameField(entitiesData, bucket, statementData);
            }

            if (command === CREATE_VIEW_COMMAND) {
                return createView(entitiesData, bucket, statementData, originalScript);
            }

            if (command === ADD_BUCKET_DATA_COMMAND) {
                return addDataToBucket(entitiesData, bucket, statementData);
            }

            if (command === ADD_COLLECTION_LEVEL_INDEX_COMMAND) {
                return addIndexToCollection(entitiesData, bucket, statementData);
            }

            if (command === REMOVE_COLLECTION_LEVEL_INDEX_COMMAND) {
                return removeIndexFromCollection(entitiesData, bucket, statementData);
            }

            if (command === ADD_RELATIONSHIP_COMMAND) {
                return addRelationship(entitiesData, bucket, statementData);
            }

            if (command === UPDATE_ENTITY_COLUMN) {
                return updateColumn(entitiesData, bucket, statementData);
            }

            if (command === CREATE_RESOURCE_PLAN) {
                return createResourcePlan(entitiesData, statementData);
            }

            if (command === CREATE_TRIGGER) {
                return addToResourcePlan(entitiesData, statementData, 'trigger');
            }

            if (command === CREATE_POOL) {
                return addToResourcePlan(entitiesData, statementData, 'pool');
            }

            if (command === CREATE_MAPPING) {
                return addMapping(entitiesData, statementData);
            }

            return entitiesData;
        },
        {
            entities: [],
            views: [],
            currentBucket: DEFAULT_BUCKET,
            buckets: {},
            relationships: [],
            modelProperties: {},
        }
    );
};

const convertCommandsToReDocs = (commands, originalScript) => {
    const reData = convertCommandsToEntities(commands, originalScript);

    const result = reData.entities.map((entity) => {
        const relatedViews = reData.views.filter((view) => view.collectionName === entity.collectionName);
        return {
            objectNames: {
                collectionName: entity.collectionName,
            },
            doc: {
                dbName: entity.bucketName,
                collectionName: entity.collectionName,
                bucketInfo: reData.buckets[entity.bucketName] || {},
                entityLevel: entity.entityLevelData,
                views: relatedViews,
            },
            jsonSchema: entity.schema,
        };
    });

    return { result, info: reData.modelProperties, relationships: reData.relationships };
};

const createCollection = (entitiesData, bucket, statementData) => {
    const { entities, currentBucket } = entitiesData;
    const updatedEntityData = getTableMergedWithReferencedTable(entities, statementData);

    if (!updatedEntityData.bucketName) {
        return { ...entitiesData, entities: [...entities, { ...updatedEntityData, bucketName: bucket }] };
    }

    if (currentBucket === DEFAULT_BUCKET) {
        return {
            ...entitiesData,
            entities: [...entities, updatedEntityData],
            bucketName: updatedEntityData.bucketName,
        };
    } else {
        return { ...entitiesData, entities: [...entities, updatedEntityData] };
    }
};

const removeCollection = (entitiesData, bucket, statementData) => {
    const { entities } = entitiesData;
    const index = findEntityIndex(entities, bucket, statementData.collectionName);
    if (index === -1) {
        return entitiesData;
    }

    return { ...entitiesData, entities: remove(entities, index) };
};

const createBucket = (entitiesData, statementData) => {
    const { buckets } = entitiesData;
    const bucketName = statementData.name;
    return {
        ...entitiesData,
        currentBucket: bucketName,
        buckets: { ...buckets, [bucketName]: statementData.data || {} },
    };
};

const removeBucket = (entitiesData, statementData) => {
    const { buckets, entities } = entitiesData;
    const bucketName = statementData.name;

    return {
        currentBucket: DEFAULT_BUCKET,
        buckets: omitCaseInsensitive(buckets, bucketName),
        entities: entities.filter((entity) => !isEqualCaseInsensitive(entity.bucketName, bucketName)),
    };
};

const useBucket = (entitiesData, statementData) => {
    return {
        ...entitiesData,
        currentBucket: statementData.bucketName,
    };
};

const addFieldsToCollection = (entitiesData, bucket, statementData) => {
    const { entities } = entitiesData;
    const index = findEntityIndex(entities, bucket, statementData.collectionName);
    if (index === -1) {
        return entitiesData;
    }

    const entity = entities[index];
    return {
        ...entitiesData,
        entities: [
            ...entities.slice(0, index),
            {
                ...entity,
                schema: {
                    ...entity.schema,
                    properties: {
                        ...entity.schema.properties,
                        ...statementData.data,
                    },
                },
            },
            ...entities.slice(index + 1),
        ],
    };
};

const renameField = (entitiesData, bucket, statementData) => {
    const { entities } = entitiesData;

    const index = findEntityIndex(entities, bucket, statementData.collectionName);
    if (index === -1) {
        return entitiesData;
    }

    const entity = entities[index];
    const field = entity[statementData.nameFrom];

    return {
        ...entitiesData,
        entities: [
            ...entities.slice(0, index),
            {
                ...entity,
                schema: {
                    ...entity.schema,
                    properties: {
                        ...omitCaseInsensitive(entity.schema.properties, statementData.nameFrom),
                        [statementData.nameTo]: field,
                    },
                },
            },
            ...entities.slice(index + 1),
        ],
    };
};

const createView = (entitiesData, bucket, statementData, originalScript) => {
    const { views } = entitiesData;
    const selectStatement = `AS ${originalScript.substring(statementData.select.start, statementData.select.stop)}`;

    return {
        ...entitiesData,
        views: [
            ...views,
            {
                ...statementData,
                data: {
                    ...statementData.data,
                    selectStatement,
                },
                bucketName: statementData.bucketName || bucket,
            },
        ],
    };
};

const addDataToBucket = (entitiesData, bucket, statementData) => {
    const { buckets } = entitiesData;
    const bucketName = getCaseInsensitiveKey(buckets, bucket);
    const { key, data } = statementData;

    return {
        ...entitiesData,
        buckets: {
            ...buckets,
            [bucketName]: {
                ...buckets[bucketName],
                [key]: [...(buckets[bucketName][key] || []), data],
            },
        },
    };
};

const getTableMergedWithReferencedTable = (entities, statementData) => {
    if (!statementData.tableLikeName) {
        return statementData;
    }

    const referencedTable = entities.find((entity) => entity.collectionName === statementData.tableLikeName);

    if (!referencedTable) {
        return statementData;
    }

    return {
        ...referencedTable,
        collectionName: statementData.collectionName,
        bucketName: statementData.bucketName,
        entityLevelData: {
            ...referencedTable.entityLevelData,
            ...statementData.entityLevelData,
        },
    };
};

const addIndexToCollection = (entitiesData, bucket, statementData) => {
    const { entities } = entitiesData;
    const entityIndex = findEntityIndex(entities, bucket, statementData.collectionName);
    if (entityIndex === -1) {
        return entitiesData;
    }

    const entity = entities[entityIndex];
    const entityLevelData = entity.entityLevelData || {};
    const indexes = [
        ...(entityLevelData.SecIndxs || []),
        {
            name: statementData.name,
            SecIndxKey: statementData.columns,
            ...statementData.data,
        },
    ];

    return {
        ...entitiesData,
        entities: set(entities, entityIndex, {
            ...entity,
            entityLevelData: {
                ...entityLevelData,
                SecIndxs: indexes,
            },
        }),
    };
};

const removeIndexFromCollection = (entitiesData, bucket, statementData) => {
    const { entities } = entitiesData;
    const entityIndex = findEntityIndex(entities, bucket, statementData.collectionName);
    if (entityIndex === -1) {
        return entitiesData;
    }

    const entity = entities[entityIndex];
    const entityLevelData = entity.entityLevelData || {};
    const indexes = (entityLevelData.SecIndxs || []).filter((index) => index.name !== statementData.indexName);

    return {
        ...entitiesData,
        entities: set(entities, entityIndex, {
            ...entity,
            entityLevelData: {
                ...entityLevelData,
                SecIndxs: indexes,
            },
        }),
    };
};

const updateColumn = (entitiesData, bucket, statementData) => {
    const { entities } = entitiesData;
    const entityIndex = findEntityIndex(entities, bucket, statementData.collectionName);
    if (entityIndex === -1) {
        return entitiesData;
    }

    const entity = entities[entityIndex];

    return {
        ...entitiesData,
        entities: set(entities, entityIndex, {
            ...entity,
            schema: {
                ...entity.schema,
                properties: updateProperties(entity.schema.properties, statementData.data),
            },
        }),
    };
};

const addRelationship = (entitiesData, bucket, statementData) => {
    const { relationships } = entitiesData;

    return {
        ...entitiesData,
        relationships: relationships.concat({
            childCollection: statementData.childCollection,
            parentCollection: statementData.parentCollection,
            childField: statementData.childField,
            parentField: statementData.parentField,
            relationshipType: 'Foreign Key',
            childCardinality: '1',
            parentCardinality: '1',
            name: statementData.relationshipName,
            childDbName: statementData.childDbName || bucket,
            dbName: statementData.dbName || bucket,
        }),
    };
};

const updateProperties = (properties, statementData) => {
    return _.fromPairs(
        _.keys(properties).map((columnName) => {
            if (!statementData.fields.includes(columnName)) {
                return [columnName, properties[columnName]];
            }

            return [
                columnName,
                {
                    ...properties[columnName],
                    [statementData.type]: statementData.value,
                },
            ];
        })
    );
};

const createResourcePlan = (entitiesData, statementData) => {
    const { modelProperties } = entitiesData;

    if (statementData.like) {
        const originalPlan = (modelProperties.resourcePlans || []).find(({ name }) => name === statementData.like);

        if (!originalPlan) {
            return entitiesData;
        }

        return {
            ...entitiesData,
            modelProperties: {
                ...modelProperties,
                resourcePlans: [
                    ...(modelProperties.resourcePlans || []),
                    {
                        ...originalPlan,
                        name: statementData.name,
                    },
                ],
            },
        };
    }

    return {
        ...entitiesData,
        modelProperties: {
            ...modelProperties,
            resourcePlans: [
                ...(modelProperties.resourcePlans || []),
                {
                    name: statementData.name,
                    parallelism: statementData.parallelism,
                },
            ],
        },
    };
};

const addToResourcePlan = (entitiesData, statementData, identifier) => {
    const { modelProperties } = entitiesData;

    const resourcePlans = modelProperties.resourcePlans || [];
    const resourcePlanIndex = getResourcePlanIndex(resourcePlans, statementData.resourceName);
    const updatedResourcePlan = {
        ...resourcePlans[resourcePlanIndex],
        [identifier + 's']: _.get(resourcePlans, `${resourcePlanIndex}.${identifier + 's'}`, []).concat(
            statementData[identifier]
        ),
    };

    return {
        ...entitiesData,
        modelProperties: {
            ...modelProperties,
            resourcePlans: set(resourcePlans, resourcePlanIndex, updatedResourcePlan),
        },
    };
};

const addMapping = (entitiesData, statementData) => {
    const { modelProperties } = entitiesData;

    const resourcePlans = modelProperties.resourcePlans || [];
    const resourceIndex = getResourcePlanIndex(resourcePlans, statementData.resourceName);
    if (resourceIndex < 0) {
        return entitiesData;
    }

    const planPools = resourcePlans[resourceIndex].pools || [];
    const poolIndex = _.findIndex(planPools, ({ name }) => name === statementData.poolName);
    if (poolIndex < 0) {
        return entitiesData;
    }

    const updatedPool = addMappingRoPoolByIndex(planPools, poolIndex, statementData.mapping);
    const updatedResourcePlans = set(resourcePlans, resourceIndex, {
        ...resourcePlans[resourceIndex],
        pools: set(planPools, poolIndex, updatedPool),
    });

    return {
        ...entitiesData,
        modelProperties: {
            ...modelProperties,
            resourcePlans: updatedResourcePlans,
        },
    };
};

const getResourcePlanIndex = (resourcePlans, resourceName) => {
    return _.findIndex(resourcePlans, (plan) => plan.name === resourceName);
};

const addMappingRoPoolByIndex = (pools, poolIndex, mapping) => {
    return { ...pools[poolIndex], mappings: _.get(pools[poolIndex], 'mappings', []).concat(mapping) };
};

module.exports = {
    convertCommandsToReDocs,
    CREATE_COLLECTION_COMMAND,
    REMOVE_COLLECTION_COMMAND,
    CREATE_BUCKET_COMMAND,
    REMOVE_BUCKET_COMMAND,
    USE_BUCKET_COMMAND,
    ADD_FIELDS_TO_COLLECTION_COMMAND,
    RENAME_FIELD_COMMAND,
    CREATE_VIEW_COMMAND,
    ADD_BUCKET_DATA_COMMAND,
    ADD_COLLECTION_LEVEL_INDEX_COMMAND,
    REMOVE_COLLECTION_LEVEL_INDEX_COMMAND,
    ADD_RELATIONSHIP_COMMAND,
    UPDATE_ENTITY_COLUMN,
    CREATE_RESOURCE_PLAN,
    CREATE_TRIGGER,
    CREATE_POOL,
    CREATE_MAPPING,
};
