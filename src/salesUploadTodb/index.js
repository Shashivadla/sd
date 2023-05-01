const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const AWS = require('aws-sdk');
const os = require('os');
const moment = require('moment-timezone');
AWS.config.update({ region: 'us-east-2' });
const dynamodb = new AWS.DynamoDB.DocumentClient();
const s3 = new AWS.S3();

module.exports.handler = async (event, context) => {
    console.info("Event: ", JSON.stringify(event));
    // const bucket = process.env.SalesTransactionBucket;
    // const folder = process.env.SalesTransactionBucketFolder;
    // const outputDir = process.env.SalesTransactionBucketFolderoutputDir;

    const bucket = 'sales-depot-dev2';
    const folder = 'RESAEBO/Incoming';
    const outputDir = path.join(os.tmpdir());
    // console.log(outputDir)

    const brands = ['NM', 'GEN', 'BG'];
    let errors = [];
    let headerFilePath, detailsFilePath;

    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
    }

    for (let brand of brands) {
        // console.log(brand)
        const params = {
            Bucket: bucket,
            Prefix: `${folder}/`,
            Delimiter: '/',
        };

        const data = await s3.listObjectsV2(params).promise();

        const fileNames = data.Contents.map((obj) => obj.Key).filter((key) => key.endsWith('.gz') && key.includes(`${brand}`));

        if (fileNames.length === 0) {
            console.log(`No .gz files found in ${folder} folder with brand ${brand}`);
        }
        else {
            let remainingFiles = fileNames.length;

            const promises = fileNames.map(async (fileName) => {
                const downloadParams = {
                    Bucket: bucket,
                    Key: fileName,
                };

                const downloadStream = s3.getObject(downloadParams).createReadStream();
                const gzip = zlib.createGunzip();
                const outputFilePath = path.join(outputDir, fileName.replace(/^.+\//, '').replace(/\.gz$/, ''));
                const output = fs.createWriteStream(outputFilePath);
                await new Promise((resolve, reject) => {
                    downloadStream.pipe(gzip).pipe(output);

                    output.on('finish', async () => {
                        console.log(`${fileName} extracted to ${outputFilePath}`);

                        if (fileName.includes('transactions_header')) {
                            headerFilePath = outputFilePath;
                        } else if (fileName.includes('transactions_details')) {
                            detailsFilePath = outputFilePath;
                        }

                        remainingFiles--;
                        if (remainingFiles === 0) {
                            await insertDataToDynamoDB(headerFilePath, detailsFilePath)
                            await moveAndDeleteFiles(bucket, fileNames, outputDir)
                        }
                        resolve();
                    });
                    output.on('error', (err) => {
                        console.error(`Error extracting ${fileName}: ${err}`);
                        errors.push(err);
                    });
                });
            });
            try {
                await Promise.all(promises);
                console.log('Extraction and insertion to DynamoDB successful');
            } catch (err) {
                console.error(`Extraction failed: ${err}`);
            }
        }
    }
};

async function moveAndDeleteFiles(bucket, fileNames, outputDir) {

    // Move the original files to the archive folder
    const archivePromises = fileNames.map(async (fileName) => {
        const archiveParams = {
            Bucket: bucket,
            CopySource: `/${bucket}/${fileName}`,
            Key: `RESAEBO/Outgoing/${fileName}`.replace('RESAEBO/Incoming/', ''),
        };
        console.log("archiveParams => ", archiveParams)
        await s3.copyObject(archiveParams).promise();
    });
    await Promise.all(archivePromises);

    // Delete the original files
    const deletePromises = fileNames.map(async (fileName) => {
        const deleteParams = {
            Bucket: bucket,
            Key: fileName
        };
        await s3.deleteObject(deleteParams).promise();
    });

    await Promise.all(deletePromises);

    console.log('All files moved to Outgoing folder and deleted');
    deleteAllFilesFromOutputDir(outputDir);
}


function deleteAllFilesFromOutputDir(outputDir) {
    const files = fs.readdirSync(outputDir);
    files.forEach((file) => {
        const filePath = path.join(outputDir, file);
        if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
            console.log(`${filePath} deleted from output directory`);
        }
    });
}

function convertDataToJson(filePath) {
    const fileContents = fs.readFileSync(filePath, 'utf8');

    const lines = fileContents.split('\n');

    const headers = lines[0].split('\t');
    const data = [];

    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split('\t');
        const obj = {};
        for (let j = 0; j < headers.length; j++) {
            obj[headers[j]] = values[j];
        }
        data.push(obj);
    }
    return data;
}


async function insertDataToDynamoDB(headerFilePath, detailsFilePath) {

    const headerJson = convertDataToJson(headerFilePath);
    // console.log(headerJson)
    const detailsJson = convertDataToJson(detailsFilePath);
    // console.log(detailsJson)
    const tableName = 'salesStagingTable';
    let lastUpdateTime = getCstDate();
    let expiration = await getUnixExpiration(lastUpdateTime);

    for (let i = 0; i < headerJson.length; i++) {
        const header = headerJson[i];
        const transactionId = header['sales_audit_order_id'];
        const transactionDate = header['transaction_date'];

        if (typeof header['store_order_id'] === 'undefined') {
            console.log('Skipping header item with undefined properties');
            continue;
        }

        let paramsHeader = {
            'pKey': 'SALES#' + transactionId,
            'sKey': 'HEADER',
            'store_order_id': header['store_order_id'],
            'store_id': header['store_id'],
            'transaction_date': transactionDate,
            // 'epoch_transaction_date': new Date(transactionDate).getTime().toString(),
            'epoch_transaction_date': Number(new Date(transactionDate).getTime()),
            'transaction_type': header['transaction_type'],
            'return_flag': header['return_flag'],
            'register_id': header['register_id'],
            'associate_id': header['associate_id'],
            'source_channel': header['source_channel'],
            'gross_sales': header['gross_sales'],
            'total_returns': header['total_returns'],
            'net_sales': header['net_sales'],
            'sold_units': header['sold_units'],
            'return_units': header['return_units'],
            'net_units': header['net_units'],
            'transaction_audit_flag': header['transaction_audit_flag'],
            'expiration': expiration,
        }

        try {
            await putItem(tableName, paramsHeader);
        }
        catch (err) {
            console.log('Error inserting header item: ', err);
            return {
                statusCode: 500,
                body: JSON.stringify('Error inserting header item: ' + err),
            };
        }


        const matchingDetails = detailsJson.filter(detail => detail['sales_audit_order_id'] === transactionId);

        for (let j = 0; j < matchingDetails.length; j++) {
            const detail = matchingDetails[j];

            if (typeof detail['product_id'] === 'undefined') {
                console.log('Skipping detail item with undefined properties');
                continue;
            }

            let paramsDetails = {
                'pKey': 'SALES#' + transactionId,
                'sKey': 'T#' + new Date(transactionDate).getTime().toString() + '_I#' + detail['product_id'],
                'product_id': detail['product_id'],
                'web_item_id': detail['web_item_id'],
                'web_item_name': detail['web_item_name'],
                'brand_name': detail['brand_name'],
                'line_item_id': detail['line_item_id'],
                'line_item_retail_sale': detail['line_item_retail_sale'],
                'line_item_promotional_sale': detail['line_item_promotional_sale'],
                'line_item_clearance_sale': detail['line_item_clearance_sale'],
                'line_item_retail_return_sale': detail['line_item_retail_return_sale'],
                'line_item_promotional_return_sale': detail['line_item_promotional_return_sale'],
                'line_item_clearance_return_sale': detail['line_item_clearance_return_sale'],
                'line_item_qty': detail['line_item_qty'],
                'line_item_status': detail['line_item_status'],
                'department': detail['department'],
                'class': detail['class'],
                'subclass': detail['subclass'],
                'expiration': expiration,
            }

            try {
                await putItem(tableName, paramsDetails);
            }
            catch (err) {
                console.error('Error inserting detail item:', err);
                return {
                    statusCode: 500,
                    body: JSON.stringify('Error inserting detail item: ' + err),
                };
            }
        }
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