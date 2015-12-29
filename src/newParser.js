var AWS = require('aws-sdk'), s3 = new AWS.S3();
var zlib = require('zlib');
var parse = require('csv-parse');
var redis = require("redis"),
    client = redis.createClient();


var remaining = 0;

keyPop();

function keyPop() {
  client.llen('keylist', function (err, response) {
    if (!err) {
      if (response > 0) {
        remaining = response;
        console.log('remaining: ', remaining);
        client.rpop('keylist', function (err, response) {
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
    var params = {
      Bucket: 'resizer-logs',
      Key: fileKey
    };
    s3.getObject(params, function (err, data) {
      keyPop();
      if (!err) {
        if (data.Body) {
          var parser = parse({comment: '#', delimiter: '\t'});
          var gunzip = zlib.createGunzip();
          createReadStream(data.Body).pipe(gunzip).pipe(parser);
          parser.on('readable', function(){
            while(data = parser.read()){
              var arrayOfStrings = splitStr(data[7], '/');
              if (arrayOfStrings[2] === 'shop') {
                rawData = {
                  shop: arrayOfStrings[3],
                  date: data[0],
                  bytes: data[3]
                };
                organizeData(rawData);
              }
            }
          });
          // Catch any error
          parser.on('error', function(err){
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
  var arrayOfStrings = strToSplit.split(separator);
  return arrayOfStrings;
}

function organizeData(rawData) {
  var shop = rawData.shop;
  var date = rawData.date;
  var bytes = parseInt(rawData.bytes, 10);
  client.hincrby(shop, date, bytes);
  client.hincrby(shop, 'total', bytes);
  client.hincrby('IWantMyMoneyBackNew', 'total', bytes);
}

var util = require('util');
var stream = require('stream');

var createReadStream = function (object, options) {
  return new MultiStream (object, options);
};

var MultiStream = function (object, options) {
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
