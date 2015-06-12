module.exports = function () {
    var Promise = require('bluebird');
    var AWS = require('aws-sdk');

    function setup() {
        // The AWS Credentials are loaded from the credential local file which is defined as best practice.
        AWS.config.region = 'eu-west-1';
    }

    function batchWriteItem(items) {
        return new Promise(function(resolve, reject) {
            // Define the basic params object.
            var params = {
                RequestItems: {
                    WordUrlRank: []
                }
             };

             // Fill the table with put requests.
             items.forEach(function(item) {
                 params.RequestItems.WordUrlRank.push({
                     PutRequest : item
                 });
             });

            var db = new AWS.DynamoDB();
            db.batchWriteItem(params, function(err, data) {
                if (err) {
                    // an error occurred
                    reject(err);
                }
                else {
                    // successful response
                    resolve(data);
                }
            });
        });
    }

    function queryItems(key) {
        return new Promise(function(resolve, reject) {
            // Define the basic params object.
            var params = {
                TableName : "WordUrlRank",
                IndexName : "Word-Rank-index",
                KeyConditions : {
                    Word : {
                        ComparisonOperator: "EQ",
                        AttributeValueList: [
                            { S: key }
                        ]
                    }
                },
                ScanIndexForward : false
            }

            var db = new AWS.DynamoDB();
            db.query(params, function(err, data) {
                if (err) {
                    // an error occurred
                    reject(err);
                }
                else {
                    // successful response
                    resolve(data);
                }
            });
        });
    }

    return {
        setup : setup,
        batchWriteItem : batchWriteItem,
        queryItems : queryItems
    };

};