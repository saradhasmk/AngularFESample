/**
 * Created by saradhasmk on 3/19/2016.
 */
'use strict';

var app = require('./app.js');
var port = 3232;

var server = app.listen(port, function () {
    console.log('Feedback Forum server listening on port ', server.address().port);
});