var AWS = require('aws-sdk'), s3 = new AWS.S3();
var redis = require("redis"),
    client = redis.createClient();

var marker = '';
var nextMarker;
listFileKeys(marker);
function listFileKeys(marker) {
  var params = {
    Bucket: 'resizer-logs', /* required */
    Delimiter: 'E298J7GNJEYIN1.2015-12',
    EncodingType: 'url',
    Marker: marker,
    Prefix: 'cf-logs/E298J7GNJEYIN1.2015-12'
  };
  s3.listObjects(params, function (err, data) {
    if (!err) {
      data.Contents.forEach(function (content) {
        client.lpush('keylist', [content.Key]);
      });
      if (data.IsTruncated) {
        console.log('nextMarker: ', nextMarker);
        nextMarker = data.NextMarker;
        listFileKeys(nextMarker);
      } else {
        console.log('listObjects completed, start downloading');
      }
    } else {
      console.log('listObjects err: ', err);
    }
  });
}
