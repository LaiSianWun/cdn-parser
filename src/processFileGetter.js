var AWS = require('aws-sdk'), s3 = new AWS.S3();
var kue = require('kue'), jobs = kue.createQueue();
var fs = require('fs');
var exec = require('child_process').exec;
var redis = require("redis"),
    client = redis.createClient();

var remaining = 0;
var hasDownloaded = 0;
var hasParsed = 0;

jobs.process('fileGetter', 3, function (job, done) {
  getFile(job.data.fileKey, done);
});

function getFile(fileKey, done) {
  if (fileKey) {
    var params = {
      Bucket: 'resizer-logs',
      Key: fileKey
    };
    s3.getObject(params, function (err, data) {
      hasDownloaded++;
      if (!err) {
        done(null, fileKey);
        if (data.Body) {
          fs.writeFileSync(__dirname+'/../'+fileKey, data.Body);
          exec('gzip -d '+__dirname+'/../'+fileKey, function (error, stdout, stderr) {
            if (error) console.log('gzip error: ', error);
            else {
              var unZipedFileName = __dirname+'/../'+fileKey.replace(/.gz/, '');
              createFileParser(unZipedFileName);
            }
          });
        }
      } else {
        console.log('getObject err: ' + err + ' err.stack: ' + err.stack);
      }
    });
  } else {
    return done(new Error('invalid fileKey: ', fileKey));
  }
}

function createFileParser(fileName) {
  var fileParser = jobs.create('file_parser', {
    fileName: fileName
  }).priority('high').removeOnComplete(true).save();

  fileParser.on( 'complete', function (result) {
    console.log( "parse file complete with " + result );
    hasParsed++;
    console.log('filesCompleted: ' + hasParsed + '/' + hasDownloaded + '/' + remaining);
    fs.unlink(result, function(err) {
      if (err) {
        console.log('delete file err: ', err);
      }
    });
  } ).on( 'failed', function () {
    // console.log( " Job failed" );
  } ).on( 'progress', function ( progress ) {
    // process.stdout.write( '\r  fileParser #' + fileParser.id + ' ' + progress + '% complete' );
  } );
}
