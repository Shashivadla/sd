const get = require("lodash.get");
const AWS = require("aws-sdk");

const { queryWithPartitionKey } = require("../shared/dynamo");

exports.handler = async function (event, context, callback) {
  console.info("Event: ", JSON.stringify(event));
  let data ={
    "pKey": "XXX140087103"
}
  console.log("dataaaaa",data)
  let response
  try {
     response=await queryWithPartitionKey("sales-depot-monthly-invoice-dev1",data)
    console.log("🚀 fetched data",response)
} catch (e) {
  console.error("Error", e);
}

return {
  statusCode: 200,
  body: JSON.stringify(response,null,2)
};
}
exports.handler()