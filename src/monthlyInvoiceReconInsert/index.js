const AWS = require('aws-sdk');
const dynamodb = new AWS.DynamoDB.DocumentClient();
const { putItem } = require("../shared/dynamo")
const { v4 } = require("uuid");
const get = require("lodash.get");

module.exports.handler = async (event) => {
    // console.info("Event: ", JSON.stringify(event));
    try {
      await Promise.all(event.Records.map(async (record) => {
        if (record.eventName === "INSERT") {
            const newImage = AWS.DynamoDB.Converter.unmarshall(
              record.dynamodb.NewImage
            );
            let data = newImage;
            if(data.entry_type=="Line"){
                console.log("after",data)
                await mapping(data)
            }
        }
      }));
    } catch (error) {
      console.error(`Error: ${error.message}`);
      throw "Error while processing  data.";
    }
  };
  
  async function mapping(data) {
    try {
        let transDate= get(data, "shipment_date", null)
const [day, month, year, hour, minute, second] = transDate.split(/[/:\s]+/); // split the date and time into parts
const epoch = new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}`).getTime() / 1000; // convert to epoch

     let map ={
         pKey: "MI#"+get(data, "order_id", null)+"_"+epoch,
         sKey: get(data, "product_id", null)+"_"+"MI"+"_"+v4(),
         type: "mi",
         order_id: get(data, "order_id", null),
         sku: get(data, "sku", null),
         qty: get(data, "qty", null),
         transaction_date: get(data, "transaction_date", null),
         shipment_date: get(data, "shipment_date", null),
         transaction_type: get(data, "transaction_type", null),
         invoice_number: get(data, "invoice_number", null),
         net_retail_price: get(data, "net_retail_price", null)
     }
     await putItem("recon-new",map)
     console.log("🚀 inserted to recon")
    } catch (error) {
        console.error(`Error: ${error.message}`);
      }
      }