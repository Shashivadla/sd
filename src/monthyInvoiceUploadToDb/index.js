const get = require("lodash.get");
const AWS = require("aws-sdk");
const moment = require("moment-timezone");
const XLSX = require('xlsx');
const s3 = new AWS.S3();
const { v4 } = require("uuid");
const { putItem } = require("../shared/dynamo");




module.exports.handler = async (event) => {
  console.info("Event: ", JSON.stringify(event));
  try {
    await Promise.all(event.Records.map(async (record) => {

      const bucket = event.Records[0].s3.bucket.name;
      const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
      let tokey ="FFMonthlyInvoice/Outgoing/MI_data"+"_"+v4()
      // Get the file data from S3
      const getObjectParams = {
        Bucket:bucket,
        Key: key
        // Bucket: "sales-depot-dev2",
        // Key: "FFMonthlyInvoice/Incoming/BG Fin report sample_vShare (1).xlsx"
      };
      const data = await s3.getObject(getObjectParams).promise();
    let dataFromExcel = await dataExtract(data)
      let mapped_Data= await monthlyMapping(dataFromExcel)
      await moveFile(bucket,key,bucket,tokey)
    //    console.log(dataFromExcel)
      console.log(mapped_Data)
    }));
  } catch (error) {
    console.error(`Error: ${error.message}`);
    throw "Error while processing t log data.";
  }
};



// exports.handler()








async function dataExtract(data) {
  try {
// const workbook = XLSX.readFile(
//   "../sampleExcelFiles/BG Fin report sample_vShare (1).xlsx"
// );
const workbook = XLSX.read(data.Body);
const worksheet = workbook.Sheets["DataSet_Order"];
const options = { header: 1, raw: false, dateNF: "yyyy-mm-dd h:mm:ss" };
const jsonData = XLSX.utils.sheet_to_json(worksheet, options, {
  blankRows: false,
  defval: null,
});
let jsonMapped = [];
 jsonData.map((row) => {
  const headers = jsonData[0];
  const newRow = {};
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    newRow[header] = row[i] === undefined ? null : row[i];
  }
  jsonMapped.push(newRow)
  
});  
console.log("extracting the data from excel and mapping completed")
return jsonMapped;
} catch (error) {
  console.error(`Error: ${error.message}`);
}
}




async function monthlyMapping(dataFromExcel) {
  try {
    let glob=[]
    for (let i = 0; i < (dataFromExcel.length-1); i++) {
      let records = dataFromExcel[i+1];
      let map = {
        pKey: get(records, "Order Id", null),
        sKey:
          get(records, "Order Date", null) +
          "_" +
          get(records, "Product Id", null),
        "order_id": get(records, "Order Id", null),
        "transaction_type": get(records, "Transaction Type", null),
        "tenant": get(records, "Tenant", null),
        "stockpoint_id": get(records, "StockPoint Id", null),
        "stockpoint_name": get(records, "StockPoint Name", null),
        "currency": get(records, "Currency", null),
        "tax_nontax": get(records, "Tax-NonTax", null),
        "entry_type": get(records, "Entry Type", null),
        "order_date": get(records, "Order Date", null),
        "shipment_date": get(records, "Ship. Date", null),
        "refund_date": get(records, "Refund Date", null),
        "return_date": get(records, "Return Date", null),
        "special_payment_date": get(records, "Special Payment Date", null),
        "adjustment_date": get(records, "Adjustment Date", null),
        "transaction_date": get(records, "Transaction Date", null),
        "posting_date": get(records, "Posting Date", null),
        "invoice_number": get(records, "Invoice No", null),
        "destination_country": get(records, "Destination Country", null),
        "destination_state": get(records, "Destination State", null),
        "ship_to_city": get(records, "Ship-to City", null),
        "ship_to_zipcode": get(records, "Ship-to ZIP Code", null),
        "qty": get(records, "Qty", null),
        "description": get(records, "Description", null),
        "brand": get(records, "Brand", null),
        "product_id": get(records, "Product Id", null),
        "sku": get(records, "SKU", null),
        "designer_id": get(records, "Designer Id", null),
        "size": get(records, "Size", null),
        "season": get(records, "Season", null),
        "gender": get(records, "Gender", null),
        "tree_category": get(records, "Tree Category", null),
        "sub_category": get(records, "Sub Category", null),
        "reason": get(records, "Reason", null),
        "reason": get(records, "Reason", null),
        "reason": get(records, "Reason", null),
        "sales_price": get(records, "Sales Price", null),
        "net_sales_price": get(records, "Net Sales Price", null),
        "promo_codes": get(records, "Promo. Codes", null),
        "Promo Code Rate": get(records, "Promo Code Rate", null),
        "tax_rate": get(records, "TAX Rate", null),
        "total_tax": get(records, "Total TAX", null),
        "total_items_paid": get(records, "Total Items Paid", null),
        "order_ship": get(records, "Order Ship.", null),
        "commission_base": get(records, "Commission Base", null),
        "effective_commission": get(records, "Effective Commission", null),
        "effective_commission_rate": get(records,"Effective Commission Rate",null),
        "software_or_hosting Fee": get(records, "Software/Hosting Fee", null),
        "special_payment": get(records, "Special Payment", null),
        "adjustment": get(records, "Adjustment", null),
        "net_retail_price": get(records, "Net Retail Price", null),
        "correction": get(records, "Correction", null),
        "courier_name": get(records, "Courier Name", null),
        "tracking_code": get(records, "Tracking Code", null),
        "ship_to_name": get(records, "Ship-to Name", null),
        "ship_to_phone": get(records, "Ship-to Phone", null),
        "version": get(records, "Version", null),
        "us_sales_price": get(records, "US Sales Price", null),
        "tax_rate_2": get(records, "Tax Rate2", null),
        "total_tax_2": get(records, "Total Tax2", null),
        "us_total_items_paid": get(records, "US Total Items Paid", null),
        "entity": get(records, "Entity", null),
        "payment": get(records, "Payment", null),
        "item_duties": get(records, "Item Duties", null),
        "create_date_time": moment().format(),
        // "expiration": getUnixExpiration(timenow),
        "expiration": 365,
      };
  glob.push(map)
      await putItem("sales-depot-monthly-invoice-dev1",map)

    }
    console.log("mapping completed and inserted to db"+"mapping done according to   "+JSON.stringify(dataFromExcel[0],null,2))
    return glob
  } catch (error) {
    console.error(`Error: ${error.message}`);
  }
}


async function moveFile(fromBucket, fromKey, toBucket, toKey) {
  try {
    // Copy the file to the new location
    await s3.copyObject({
      Bucket: toBucket,
      CopySource: `${fromBucket}/${fromKey}`,
      Key: toKey
    }).promise();

    // Delete the original file
    await s3.deleteObject({
      Bucket: fromBucket,
      Key: fromKey
    }).promise();

    console.log(`Moved ${fromKey} from ${fromBucket} to ${toBucket}/${toKey}`);
  } catch (err) {
    console.error(`Error moving file: ${err}`);
  }
}

// let getCstDate = (dateString = Date.now()) => {
//  return moment(new Date(dateString)).tz("America/Chicago");
// };

// async function getUnixExpiration(timenow) {
//   let key= {
//      pKey:"LOOKUP#ADMIN",
//      sKey:"GLOBAL"
//    }
//    let config_data = await getItem(process.env.CONFIG_TABLE,key)
//    let sd_MonthlyInvoiceDeleteDays = get(config_data.Item, "sd_MonthlyInvoiceDeleteDays", 366)
//    console.log("sd_MonthlyInvoiceDeleteDays",sd_MonthlyInvoiceDeleteDays)
//    let unixExpiration = timenow.add(sd_MonthlyInvoiceDeleteDays, "days");
//     let  epochTimestamp = unixExpiration.unix();
//     return epochTimestamp
//  }


// monthlyMapping(json);
