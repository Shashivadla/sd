const AWS = require('aws-sdk');
const get = require('lodash.get');
const dynamodb = new AWS.DynamoDB.DocumentClient();

const reserveWord = ['name', 'type', 'hidden'];

async function getItem(tableName, key, attributesToGet = null) {
    let params;
    try {
        params = {
            TableName: tableName,
            Key: key
        };
        if (attributesToGet) params.AttributesToGet = attributesToGet;
        return await dynamodb.get(params).promise();
    } catch (e) {
        console.error("Get Item Error: ", e, "\nGet params: ", params);
        throw "GetItemError";
    }
}

async function putItem(tableName, item) {
    let params;
    try {
        params = {
            TableName: tableName,
            Item: item
        };
        return await dynamodb.put(params).promise();
    } catch (e) {
        console.error("Put Item Error: ", e, "\nPut params: ", params);
        throw "PutItemError";
    }
}

async function updateItem(tableName, key, item) {
    let params;
    try {
        const [expression, expressionAtts, expressionName] = await getUpdateExpressions(item, key);
        const params = {
            TableName: tableName,
            Key: key,
            UpdateExpression: expression,
            ExpressionAttributeValues: expressionAtts,
            ExpressionAttributeNames: expressionName
        };
        return await dynamodb.update(params).promise();
    } catch (e) {
        console.error("Update Item Error: ", e, "\nUpdate params: ", params);
        throw "UpdateItemError";
    }
}

async function deleteItem(tableName, key) {
    let params;
    try {
        params = {
            TableName: tableName,
            Key: key
        };
        return await dynamodb.delete(params).promise();
    } catch (e) {
        console.error("delete Item Error: ", e, "\ndelete params: ", params);
        throw "DeleteItemError";
    }
}

async function queryWithIndex(tableName, index, keys, limit = null, lastEvaluatedKey = null) {
    let params;
    try {
        const [expression, expressionAtts] = await getQueryExpression(keys);
        params = {
            TableName: tableName,
            IndexName: index,
            KeyConditionExpression: expression,
            ExpressionAttributeValues: expressionAtts
        };
        if (limit) params.Limit = limit;
        if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;
        return await dynamodb.query(params).promise()
    } catch (e) {
        console.error("Query Item Error: ", e, "\nQuery params: ", params);
        throw "QueryItemError";
    }
}

async function queryWithPartitionKey(tableName, key) {
    let params;
    try {
        const [expression, expressionAtts] = await getQueryExpression(key);
        params = {
            TableName: tableName,
            KeyConditionExpression: expression,
            ExpressionAttributeValues: expressionAtts
        };
        return await dynamodb.query(params).promise();
    } catch (e) {
        console.error("Query Item With Partition key Error: ", e, "\nGet params: ", params);
        throw "QueryItemError";
    }
}

async function checkAndUpdateDynamo(tableName, key, item) {
    const response = await getItem(tableName, key);
    if (get(response, 'Item', null)) {
        await updateItem(tableName, key, item);
    } else {
        await putItem(tableName, item);
    }
}

async function insertIfNotExists(tableName, key, item) {
    const response = await getItem(tableName, key);
    if (!get(response, 'Item', null)) {
        await putItem(tableName, item);
    }
}

async function getUpdateExpressions(params, key) {
    let expression = "SET ";
    let expressionAtts = {};
    let expressionName = {};
    Object.keys(key).forEach(k => delete params[k]);
    Object.keys(params).forEach(p => {
        if (reserveWord.includes(p)) {
            expression += `#${p}_RES=:${p}_RES, `;
            expressionAtts[`:${p}_RES`] = params[p];
            expressionName[`#${p}_RES`] = p;
        } else {
            expression += `#${p}=:${p}, `;
            expressionAtts[`:${p}`] = params[p];
            expressionName[`#${p}`] = p;
        }
        //TODO: Check expression
        // expression += p + "=:" + p + ", ";
        // expressionAtts[":" + p] = params[p];
    });
    expression = expression.substring(0, expression.lastIndexOf(', '));
    return [expression, expressionAtts, expressionName];
}

async function getQueryExpression(keys) {
    let expression = "";
    let expressionAtts = {};
    Object.keys(keys).forEach(k => {
        expression += k + "=:" + k + " and ";
        expressionAtts[":" + k] = keys[k];
    });
    expression = expression.substring(0, expression.lastIndexOf(' and '));
    return [expression, expressionAtts];
}

async function updateRecordStatusForRemoveEvent(tableName, key) {
    let item = {
        ...key,
        RECORD_STATUS: false
    }
    const response = await getItem(tableName, key);
    if (get(response, 'Item', null)) {
        await updateItem(tableName, key, item);
    } else {
        await putItem(tableName, item);
    }
}

async function scanTable(tableName, ExclusiveStartKey = null, filterExpression = null) {
    let params;
    try {
        params = {
            TableName: tableName
        };
        if (ExclusiveStartKey) params.ExclusiveStartKey = ExclusiveStartKey;
        if (filterExpression) params = {...params, ...filterExpression};
        return await dynamodb.scan(params).promise();
    } catch (e) {
        console.error("Get Item Error: ", e, "\nGet params: ", params);
        throw "GetItemError";
    }
}

async function queryWithParams(params)  {
    return await dynamodb.query(params).promise();
}

module.exports = {
    getItem,
    putItem,
    updateItem,
    deleteItem,
    queryWithIndex,
    checkAndUpdateDynamo,
    insertIfNotExists,
    queryWithPartitionKey,
    updateRecordStatusForRemoveEvent,
    scanTable,
    queryWithParams
};
