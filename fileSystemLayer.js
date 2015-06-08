/**
 * Created by asher on 07/04/15.
 */
module.exports = function () {
    var Promise = require('bluebird');
    var fs;
    const encoding = 'utf8';

    function setup() {
        fs = Promise.promisifyAll(require('fs'));
    }

    function getFile(path) {
        return fs.readFileAsync(path, encoding);
    }

    function delFile(path) {
        return fs.unlinkAsync(path, encoding);
    }

    return {
        setup : setup,
        getFile : getFile,
        delFile : delFile
    };

};