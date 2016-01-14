import zlib from 'zlib';
import parse from 'csv-parse';
import AWS from 'aws-sdk';
import redis from 'redis';
let client = redis.createClient();
let s3 = new AWS.S3();

let remaining = 0;
keyPop();
function keyPop() {
  client.llen('keylist', (err, response) => {
    if (!err) {
      if (response > 0) {
        remaining = response;
        console.log('remaining: ', remaining);
        client.rpop('keylist', (err, response) => {
          if (!err) {
            getFile(response);
          } else {
            console.log('rpop err: ', err);
          }
        });
      }
    } else {
      console.log('llen err: ', err);
    }
  });
}

function getFile(fileKey) {
  if (fileKey) {
    let params = {
      Bucket: 'resizer-logs',
      Key: fileKey
    };
    s3.getObject(params, (err, data) => {
      keyPop();
      if (!err) {
        if (data.Body) {
          let parser = parse({comment: '#', delimiter: '\t'});
          let gunzip = zlib.createGunzip();
          createReadStream(data.Body).pipe(gunzip).pipe(parser);
          parser.on('readable', () => {
            while(data = parser.read()){
              let arrayOfStrings = splitStr(data[7], '/');
              if (arrayOfStrings[2] === 'shop') {
                let rawData = {
                  shop: arrayOfStrings[3],
                  date: data[0],
                  bytes: data[3]
                };
                organizeData(rawData);
              }
            }
          });
          // Catch any error
          parser.on('error', (err) => {
            console.log('parser err: ', err.message);
          });
        }
      } else {
        console.log('getObject err: ' + err + ' err.stack: ' + err.stack);
      }
    });
  } else {
    console.log('invalid fileKey: ', fileKey);
  }
}

function splitStr(strToSplit, separator) {
  let arrayOfStrings = strToSplit.split(separator);
  return arrayOfStrings;
}

function organizeData(rawData) {
  let shop = rawData.shop;
  let date = rawData.date;
  let bytes = parseInt(rawData.bytes, 10);
  client.hincrby(shop, date, bytes);
  client.hincrby(shop, 'total', bytes);
  client.hincrby('IWantMyMoneyBackNew', 'total', bytes);
}

let util = require('util');
let stream = require('stream');

let createReadStream = function (object, options) {
  return new MultiStream (object, options);
};

let MultiStream = function (object, options) {
  if (object instanceof Buffer || typeof object === 'string') {
    options = options || {};
    stream.Readable.call(this, {
      highWaterMark: options.highWaterMark,
      encoding: options.encoding
    });
  } else {
    stream.Readable.call(this, { objectMode: true });
  }
  this._object = object;
};

util.inherits(MultiStream, stream.Readable);

MultiStream.prototype._read = function () {
  this.push(this._object);
  this._object = null;
};
