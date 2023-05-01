const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const moment = require('moment-timezone');
const { v4 } = require("uuid");
const sourceTableName = 'salesStagingTable';
const destinationTableName = 'recon-new';
const { putItem,getItem } = require("../shared/dynamo")
const dynamodb = new AWS.DynamoDB.DocumentClient();

module.exports.handler = async (event) => {

    try {
        await Promise.all(event.Records.map(async (record) => {
          if (record.eventName === "INSERT") {
              const newImage = AWS.DynamoDB.Converter.unmarshall(
                record.dynamodb.NewImage
              );
              let data = newImage;
              if(!data.sKey.includes('HEADER')){
                  let item = data
                let pKey = data.pKey
                let key ={
                    pKey:pKey,
                    sKey:"HEADER"
                }
                let header= await getItem("salesStagingTable",key)
                let headerItem =header.Item
                
                
                  let  selectedAttributes = {
                    pKey: item.pKey + '_' + `${JSON.stringify(headerItem.epoch_transaction_date)}`,
                    sKey: `${item.product_id}` + '_resa_' + v4(),
                    sku: item.product_id,
                    return_Qty: item.line_item_qty === '-1' ? '1' : '0',
                    resa_Gross_Sales: item.line_item_retail_sale,
                    resa_Qty: item.line_item_qty,
                    resa_Net: item.line_item_retail_sale,
                    transaction_Date: headerItem.transaction_date,
                    // expiration: expiration,
                    type: "resa"
                };
                
              await putItem("recon-new",selectedAttributes)
              }
          }
        }));
      } catch (error) {
        console.error(`Error: ${error.message}`);
        throw "Error while processing  data.";
      }
    };



function getCstDate() {
    return moment.tz("America/Chicago").format();
}

async function getUnixExpiration(lastUpdateTime) {
    const unixTimestamp = moment(lastUpdateTime).unix();
    const expirationTimestamp = moment.unix(unixTimestamp).add(2, 'minutes').unix();
    return expirationTimestamp;
}