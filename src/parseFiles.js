var kue = require('kue'), jobs = kue.createQueue();
var fs = require('fs');
var parse = require('csv-parse');
var redis = require("redis"),
    client = redis.createClient();

jobs.process('file_parser', 1, function (job, done) {
  var fileName = job.data.fileName;
  parseFile(fileName);
  done(null, fileName);
});

function parseFile(fileName) {
  var parser = parse({comment: '#', delimiter: '\t'});
  var input = fs.createReadStream(fileName);
  input.pipe(parser);
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

// helper

function splitStr(strToSplit, separator) {
  var arrayOfStrings = strToSplit.split(separator);
  return arrayOfStrings;
}

function organizeData(rowData) {
  var shop = rowData.shop;
  var date = rowData.date;
  var bytes = parseInt(rowData.bytes, 10);
  client.hincrby(shop, date, bytes);
  client.hincrby(shop, 'total', bytes);
  client.hincrby('IWantMyMoneyBack', 'total', bytes);
}
