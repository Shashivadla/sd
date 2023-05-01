const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const moment = require('moment-timezone');
const { v4 } = require("uuid");
const sourceTableName = 'salesStagingTable';
const destinationTableName = 'recon-new';
const { putItem } = require("../shared/dynamo")
const dynamodb = new AWS.DynamoDB.DocumentClient();

module.exports.handler = async (event) => {
    console.info("Event: ", JSON.stringify(event));
    const scanParams = {
        TableName: sourceTableName,
        FilterExpression: 'NOT sKey = :skey',
        ExpressionAttributeValues: {
            ':skey': 'HEADER',
        },
    };

    try {
        const scanResult = await dynamodb.scan(scanParams).promise();
        const items = scanResult.Items;

        const validAttributes = [];
        let lastUpdateTime = getCstDate();
        let expiration = await getUnixExpiration(lastUpdateTime);

        for (const item of items) {
            // Retrieve the header item for this transaction
            const headerParams = {
                TableName: sourceTableName,
                Key: {
                    pKey: item.pKey,
                    sKey: 'HEADER',
                },
            };
            const headerResult = await dynamodb.get(headerParams).promise();
            const headerItem = headerResult.Item;

            if (!headerItem) {
                console.warn(`Header item not found for transaction ${item.pKey}`);
                continue;
            }

            // Extract attributes you want to insert to destination table
            const selectedAttributes = {
                pKey: item.pKey + '_' + `${JSON.stringify(headerItem.epoch_transaction_date)}`,
                sKey: `${item.product_id}` + '_resa_' + v4(),
                sku: item.product_id,
                return_Qty: item.line_item_qty === '-1' ? '1' : '0',
                resa_Gross_Sales: item.line_item_retail_sale,
                resa_Qty: item.line_item_qty,
                resa_Net: item.line_item_retail_sale,
                transaction_Date: headerItem.transaction_date,
                expiration: expiration,
                type: "resa"
            };

            if (selectedAttributes.pKey !== undefined) {
                validAttributes.push(selectedAttributes);
            }
        }

        for (const item of validAttributes) {
            await putItem(item, destinationTableName);
        }

        console.log(`Inserted ${validAttributes.length} items to ${destinationTableName} table.`);
    } catch (error) {
        console.error(error);
    }
}

function getCstDate() {
    return moment.tz("America/Chicago").format();
}

async function getUnixExpiration(lastUpdateTime) {
    const unixTimestamp = moment(lastUpdateTime).unix();
    const expirationTimestamp = moment.unix(unixTimestamp).add(2, 'minutes').unix();
    return expirationTimestamp;
}