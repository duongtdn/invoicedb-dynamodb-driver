"use strict"

const AWS = require('aws-sdk')

let dynamodb = null

const invoiceDB = {}

const table = 'INVOICE';

const Invoice = {
  TableName : "INVOICE",
  KeySchema: [       
    { AttributeName: "number", KeyType: "HASH" }
  ],
  AttributeDefinitions: [       
    { AttributeName: "number", AttributeType: "S" }
  ],
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


  createInvoice( invoice, done) {
    if (!invoice) {
      done && done(null, null)
    }

    const now = new Date();
    invoice.createdAt = now.getTime();
    invoice.status = 'active'

    // create unique Invoice number
    const yy = String(now.getFullYear()).slice(-2);
    const mm = now.getMonth() + 1;
    const num = 1;

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

  updateInvoice( invoice, done) {
    
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

