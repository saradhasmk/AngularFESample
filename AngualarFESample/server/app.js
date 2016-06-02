/**
 * Created by saradhasmk on 3/19/2016.
 */
'use strict';
var http = require('http');
var path = require('path');
var express=require('express');
var app=express();
var bodyparser=require("body-parser");
var mongojs=require("mongojs");
var db=mongojs("eventmanagement",['eventmanagement']);

console.log(path.join(__dirname, '../')+"client/index_nodejs.html");
app.use(express.static(path.join(__dirname, '../')+"client"));
app.use(bodyparser.json());
app.get('/volunteerlist',function(req,res){
    db.eventmanagement.find().sort({_id:-1}).toArray(function(err,docs){
        console.log("Docs ::: "+JSON.stringify(docs));
        console.log("Docs ::: "+JSON.stringify(Object.keys(docs).length));
        res.json(docs);
    });
});

app.post('/addvolunteer',function(req,res){
    console.log("from add");
    console.log(JSON.stringify(req.body));
   db.eventmanagement.insert(req.body,function(err,docs){
        res.json(docs);
        console.log("sho ::: "+JSON.stringify(docs));
    });
});

module.exports=app;