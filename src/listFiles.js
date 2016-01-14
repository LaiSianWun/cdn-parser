import AWS from 'aws-sdk';
import redis from "redis";
let client = redis.createClient();
let s3 = new AWS.S3();

let marker = 'cf-logs/E298J7GNJEYIN1.2016-01-11-04.9e0906c3.gz';
let nextMarker;
listFileKeys(marker);
function listFileKeys(marker) {
  let params = {
    Bucket: 'resizer-logs', /* required */
    Delimiter: 'E298J7GNJEYIN1.2016-01',
    EncodingType: 'url',
    Marker: marker,
    Prefix: 'cf-logs/E298J7GNJEYIN1.2016-01'
  };
  s3.listObjects(params, (err, data) => {
    if (!err) {
      data.Contents.forEach((content) => {
        client.lpush('keylist', [content.Key]);
      });
      if (data.IsTruncated) {
        nextMarker = data.NextMarker;
        console.log('nextMarker: ', nextMarker);
        listFileKeys(nextMarker);
      } else {
        console.log('listObjects completed, start downloading');
      }
    } else {
      console.log('listObjects err: ', err);
    }
  });
}
