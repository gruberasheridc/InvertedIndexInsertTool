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

var db = require('AWSService')();
db.setup();

var fs = require('fileSystemLayer')();
fs.setup();

var validator = require('validator');
var Map = require("collections/map");
var argv = require('minimist')(process.argv.slice(2));

var invertedIndexPath = argv["i"];
if (!invertedIndexPath) {
    console.log("Input params must include: -i INVERTED_INDEX_RESULTS_FILE_PATH");
    return;
}

insertInvertedIndexToDynamoDB(invertedIndexPath);

function insertInvertedIndexToDynamoDB(invertedIndexPath) {
    // Read input file.
    fs.getFile(invertedIndexPath).then(function(data) {
        if (data) {
            // Prepare data for insert.
            var rankItems = prepareDataForInsert(data);
            db.batchWriteItem(rankItems).then(function(data) {
                res.send(data);
            });
        }
    }).catch(function(err) {
        console.log("Failed to open the inverted index input file." + "Error: " + err + ".");
    });
};

function prepareDataForInsert(data) {
    var lines = data.split('\n');

    // Generate a word/url => Sites/Number map. To be used in order to prepare the insert statement.
    var wordSitesMap = new Map();
    lines.forEach(function (line) {
        if (line) {
            var wordSites = spliteWordSites(line);
            var word = wordSites[0];
            var sites = wordSites[1]; // If the word is a URL sites will contain a number.
            wordSitesMap.set(word, sites);
        }
    });

    // Generate items for insert to the InvertedIndex Table (item contains Word, Sites).
    var invertedIndexItems = [];
    wordSitesMap.entries().forEach(function (entry) {
        var word = entry[0];
        var sites = entry[1];

        var item = {
            Item: {
                Word: {
                    S: word
                },
                sites: {
                    S: sites
                }
            }
        };

        invertedIndexItems.push(item);
    });

    // Generate items for insert to the WordUrlRank. Each item contains Word, Url, Rank.
    // We will access the inverted index table only for getting the rank of URLs (it is possible that some ranks
    // exist in the Inverted Index table that are not in the map (if this is a case of update).
    var rankItems = [];
    wordSitesMap.entries().forEach(function (entry) {
        var word = entry[0];
        if (validator.isURL(word)) {
            // Urls words are only inserted as keys to the InvertedIndex table.
            // We will perform a lookup to the InvertedIndex table for getting their rank.
            return;
        }

        var sites = entry[1]; // We get the sites from the map.
        var sitesList = sites.split(",");
        sitesList.forEach(function (site) {
            // Generate an item for every Word, Site, Site Rank.
            var siteRank = wordSitesMap.get(site)
            if (!siteRank) {
                // If site is not in map.
                siteRank = 0;
            }

            var item = {
                Item: {
                    Word: {
                        S: word
                    },
                    Url: {
                        S: site
                    },
                    Rank: {
                        N: siteRank.toString()
                    }
                }
            };

            rankItems.push(item);
        });

    });

    return rankItems;
}

function spliteWordSites(line) {
    var arr = new Array();
    arr[0] = line.substring(0, line.indexOf(","));
    arr[1] = line.substring(line.indexOf(",") + 1);

    return arr;
};


