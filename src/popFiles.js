var AWS = require('aws-sdk'), s3 = new AWS.S3();
var kue = require('kue'), jobs = kue.createQueue();
var fs = require('fs');
var exec = require('child_process').exec;
var redis = require("redis"),
    client = redis.createClient();

var hasListed = 0;
var hasDownloaded = 0;
var hasParsed = 0;

keyPop();

function keyPop() {
  client.llen('keylist', function (err, response) {
    if (response > 0) {
      for (var i=1; i<=10; i++) {
        client.rpop('keylist', function (err, response) {
          createFileGetter(response);
        })
      }
    }
  })
}

function createFileGetter(fileKey) {
  var fileGetter = jobs.create('fileGetter', {
    fileKey: fileKey
  }).priority('high').removeOnComplete(true).save();

  fileGetter.on('complete', function (fileKey) {
    //console.log('fileGetter completed with: ', fileKey);
  });

}

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
    keyPop();
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
    console.log('filesCompleted: ' + hasParsed + '/' + hasDownloaded + '/' + hasListed);
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
