var AWS = require('aws-sdk'), s3 = new AWS.S3();
var kue = require('kue'), jobs = kue.createQueue();
var fs = require('fs');
var exec = require('child_process').exec;
var redis = require("redis"),
    client = redis.createClient();

var remaining = 0;

keyPop();

function keyPop() {
  client.llen('keylist', function (err, response) {
    if (response > 0) {
      remaining = response;
      console.log('remaining: ', remaining);
      for (var i=1; i<=10; i++) {
        client.rpop('keylist', function (err, response) {
          createFileGetter(response);
        })
      }
      keyPop();
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
