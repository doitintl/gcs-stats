const storage = require('@google-cloud/storage')();
const bigquery = require('@google-cloud/bigquery')();
const config = require('./config');

const dataset = bigquery.dataset(config.dataset);
const table = dataset.table(config.table);
const bucket = storage.bucket(config.logsBucket);
const processedBucket = storage.bucket(config.processedBucket);
const errorsBucket = storage.bucket(config.errorsBucket);
const usageLogsBucket = storage.bucket(config.usageLogsBucket);

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * Receives an Object Notification Message.
 *
 * @param {object} event The Cloud Functions event.
 * @param {object} event.data The Pub/Sub message data.
 * @param {object} event.data.attributes The Pub/Sub message attributes.
 */
exports.gcsStatsHandler = (data, context) => {
  const pubsubMessage = data;
  const attributes = pubsubMessage.attributes;
  const objectId = attributes.objectId;
  const prefixedLogRegExp = /^PROJECT_([a-z0-9\-]+)_BUCKET_([a-z0-9\-\_\.]+)_storage_([0-9]{4}_[0-9]{2}_[0-9]{2})_[0-9]{2}_[0-9]{2}_[0-9]{2}_[a-z0-9]+_v0$/;
  const defaultLogRegexp = /^([a-z0-9\-\_\.]+)_storage_([0-9]{4}_[0-9]{2}_[0-9]{2})_[0-9]{2}_[0-9]{2}_[0-9]{2}_[a-z0-9]+_v0$/
  const usageLogRegExp = /^([a-zA-Z0-9\-\_\.]+)_usage_([0-9]{4}_[0-9]{2}_[0-9]{2})_[0-9]{2}_[0-9]{2}_[0-9]{2}_[a-z0-9]+_v0$/

  var file = bucket.file(objectId);
  var fileProcessed = processedBucket.file(objectId);

  var match, loggedBucket, loggedProject, dateString;

  if ((match = prefixedLogRegExp.exec(objectId)) || (match = defaultLogRegexp.exec(objectId))) {
    if (match.length === 4) {
      // logs that match storage logs name with the custom prefix
      loggedProject = match[1];
      loggedBucket = match[2];
      dateString = match[3];
    } else if (match.length === 3) {
      // logs that match the default storage logs name
      loggedProject = null;
      loggedBucket = match[1];
      dateString = match[2];
    }
  } else if (usageLogRegExp.exec(objectId)) {
    // Move usage logs to the usage logs bucket
    return file.move(usageLogsBucket);
  }

  // Check if this file was already processed
  return fileProcessed.exists()
    .then((response) => {
      const exists = response[0];
      if (exists) {
        // The file was already uploaded and processed - finish the run
        console.log(`${file.name} was already procecseed.`);
        return file.delete();
      } else {
        return Promise.resolve()
          .then(() => {
            // Check that the file was parsed successfully and download
            if (!loggedBucket || !dateString) {
              const error = new Error('Invalid storage log file');
              throw error;
            }
            return file.download();
          })
          .then((response) => {
            // Prepare row for insertion
            const now = new Date();
            const content = response[0];
            const lines = content.toString().split('\n');
            const data = lines[1].replace(/"/g, '').split(',');
            const storageBytesHours = parseInt(data[1]);
            const bytes = Math.round(storageBytesHours / 24);
            const date = bigquery.date(dateString.replace(/_/g, '-'));
            const updateTime = bigquery.timestamp(now);
            
            const row = {
              project_id: loggedProject,
              bucket: loggedBucket,
              storage_byte_hours: storageBytesHours,
              bytes: bytes,
              date: date,
              update_time: updateTime,
              filename: objectId
            };
            return table.insert(row);
          })
          .then((response) => {
            // Move the file to the processed files bucket
            return file.move(processedBucket);
          })
          .catch((err) => {
            // Move the file to the errors bucket if there was an error
            console.error('ERROR:', err);
            return file.move(errorsBucket);
          });
      }
    })
};
