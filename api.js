"use strict"

const AWS = require('aws-sdk')

let dynamodb = null

const invoiceDB = {}

const table = 'INVOICE';
const gsIndex = 'INVOICE_BY_STATUS';

const Invoice = {
  TableName : table,
  KeySchema: [       
    { AttributeName: "number", KeyType: "HASH" }
  ],
  AttributeDefinitions: [       
    { AttributeName: "number", AttributeType: "S" },
    { AttributeName: "status", AttributeType: "S" }
  ],
  GlobalSecondaryIndexes: [{
    IndexName: gsIndex,
    KeySchema: [
      { AttributeName: "status", KeyType: "HASH"},     
    ],
    Projection: {
        ProjectionType: "ALL"
    },
    ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1
    }
  }],
  ProvisionedThroughput: {       
      ReadCapacityUnits: 1, 
      WriteCapacityUnits: 1
  }
}

const db = {
  _ready: false,

  createTable(done) {
    if (!this._ready) {
      console.error("DynamoDB is not ready yet")
      return this;
    }

    dynamodb.createTable(Invoice, function(err, data) {
      if (err) {
        done && done(err);
      } else {
        done && done();
      }
    });

    return this;
  },

  dropTable(done) {
    if (!this._ready) {
      console.error("DynamoDB is not ready yet")
      return this;
    }
    dynamodb.deleteTable({ TableName: table }, done)
  },

  getInvoice(number, done) {
    if (!number) {
      done && done({error: 'no Invoice ID'}, null)
      return
    }
    
    const params = { 
      TableName: table, 
      Key: {
        number
      }
    }
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.get(params, function(err, data) {
      if (err) {
        done && done({ error:`Unable to read item: ${JSON.stringify(err, null, 2)}`}, null);
      } else {
        if (data && data.Item) {
          done && done(null, data.Item);
        } else {
          done && done(null, null);
        }
      }
    });

  },

  batchGetInvoices(invoiceNums, done) {
    const param = { RequestItems: {} };
    
    param.RequestItems[table] = { Keys: [] };
    invoiceNums.forEach( id => {
      param.RequestItems[table].Keys.push({ 'number' :id })
    })
    param.RequestItems[table].AttributesToGet = ['number', 'subTotal', 'items', 'issueAt']; // option (attributes to retrieve from this table)
    param.RequestItems[table].ConsistentRead = false; // optional (true | false)

    param.ReturnConsumedCapacity = 'NONE'; // optional (NONE | TOTAL | INDEXES)
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.batchGet(param, (err, data) => {
      if (err) {
        done && done(err, null)
      } else {
        done && done(null, data.Responses[table])
      }
    })
  },

  queryInvoicesByStatus({status}, done) {

    const params = {
      TableName: table, 
      IndexName: gsIndex,
      KeyConditionExpression: `#status = :stt`,
      ExpressionAttributeNames: { 
        "#status": "status" 
      },
      ExpressionAttributeValues: {
        ':stt' : status
      } 
    }

    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.query(params,
      (err, data) => {
        if (err) { done({error:err}, null) }
        else {
          if (data && data.Items && data.Items.length > 0) {
            done && done(null, data.Items)
          } else {
            done && done(null, null)
          }
        }
        
      }
    );

  },

  createMasterRecord(done) {
    const invoice = {
      number: 'M-1111', 
      issueAt: new Date(),
    }

    for (let y = 18; y < 20; y++) {
      invoice[`y${y}`] = {};
      const yr = invoice[`y${y}`];
      for (let m = 1; m < 13; m++) {
        yr[`m${m}`] = { cnt: 10}
      }
    }

    const params = {
      TableName: table,
      Item: invoice
    };

    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.put(params, (err, data) => {
      if (err) {
        done && done(err);
      } else {
        done && done();
      }
    });
  },

  async createInvoice( invoice, done) {
    if (!invoice) {
      done && done(null, null);
      return
    }

    const now = new Date();
    invoice.issueAt = now.getTime();
    invoice.status = 'billing'

    // prepare unique Invoice number
    const yy = String(now.getFullYear()).slice(-2);
    const mm = String(now.getMonth() + 1).padStart(2,0);
    invoice.number = `${yy}${mm}`;

    // register invoice to master record and get number
    try {
      const cnt = await this._getInvoiceNumber(now);
      invoice.number += String(cnt).padStart(3,0);
    } catch (err) {
      done && done(err, null);
      return
    }
   
    // write invoice into db
    try {
      await this._writeInvoiceToDb(invoice);
      done && done (null, invoice);
    } catch (err) {
      done && done(err, null);
      return
    }

  },

  _getInvoiceNumber(now) {
    const yy = String(now.getFullYear()).slice(-2);
    const mm = now.getMonth() + 1;
    const params = {
      TableName: table,
      Key: { number: 'M-1111'},
      UpdateExpression: `set y${yy}.m${mm}.cnt = y${yy}.m${mm}.cnt + :val`,
      ExpressionAttributeValues:{
        ":val": 1
      },
      ReturnValues:"UPDATED_NEW"
    };

    return new Promise((resolve, reject) => {
      const docClient = new AWS.DynamoDB.DocumentClient();
      docClient.update(params, function(err, data) {
        if (err) {
          reject(err)
        } else {
          const cnt = data.Attributes[`y${yy}`][`m${mm}`].cnt
          resolve(cnt)
        }
      });
    })

  },

  _writeInvoiceToDb(invoice) {
    const params = {
      TableName: table,
      Item: invoice
    };
    return new Promise((resolve, reject) => {
      const docClient = new AWS.DynamoDB.DocumentClient();
      docClient.put(params, (err, data) => {
        if (err) {
          reject(err);
        } else {
          resolve(data);
        }
      });
    })
  },

  resolve( {updatedBy, number, status}, done) {
    const now = new Date();
    const d = now.getTime();

    const params = {
      TableName: table,
      Key: {
        number: number
      },
      UpdateExpression: `set #status = :s, resolvedBy = :u, resolvedAt = :d`,
      ExpressionAttributeNames: { 
        "#status": "status" 
      },
      ExpressionAttributeValues: {
        ":s": status,
        ":u": updatedBy,
        ":d": d
      }
    }

    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.update(params, done)
  },

}

function DynamoDB({ region = 'us-west-2', endpoint = 'http://localhost:8000' }, onReady) {
 
  AWS.config.update({ region, endpoint });
 
  dynamodb = new AWS.DynamoDB();

  if (onReady) {
    dynamodb.listTables(function (err, data) {
      if (err) {
        console.log("Error when checking DynamoDB status")
        db._ready = false;
        onReady(err, null);
      } else {
        db._ready = true;
        onReady(null, data);
      }
    });
  } else {
    db._ready = true;
  }

  return db;

}

module.exports = DynamoDB;

