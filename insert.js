/*
 The insert tool works as a command line utility that inserts the given inverted index results to DaynamoDB.

 Execution format: node insert.js -i INVERTED_INDEX_RESULTS_FILE_PATH
 Example: node insert.js -i /input/invertedindex.txt
 The -i parameter is used for specifying the inverted index results input file path.

 The inverted index results input file format is as follows:
 1. A file in the form of CSV, where the first column is the key and the rest are the values.
 2. If the first column is a word then the following columns are web sites the word resides in.
 3. If the first column is a URL the second column will contain the number of occurrences that the URL was found.

 Input example:
     about,http://www.iht.com,http://www.nytimes.com,http://espn.go.com,http://recode.net,http://www.cnn.com
     http://xgames.com/,1
     https://fivethirtyeight.com/datalab/what-kyrie-irvings-injury-could-mean-for-the-cavs-chances/,3
 */

var validator = require('validator');
var fs = require("fs");
var argv = require('minimist')(process.argv.slice(2));

var invertedIndexPath = argv["i"];
if (!invertedIndexPath) {
    console.log("Input params must include: -i INVERTED_INDEX_RESULTS_FILE_PATH");
    return;
}


