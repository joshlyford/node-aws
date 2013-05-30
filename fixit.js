var kue = require('kue'), express = require('express');
var mysql = require('mysql');
var fs = require('fs');
var path = require('path');
var mkdirp = require('mkdirp');
var AWS = require('aws-sdk');

var config = require('./config.json');


var jobs = kue.createQueue();
// start the UI
var express = require("express");
var app = express();

console.log( config.aws);
// Instead, do this:
AWS.config.update( config.aws );

var s3 = new AWS.S3();
var glc = new AWS.Glacier();

//Create first job on start
jobs.create('fix_bucket', config.s3 ).save();

var file_path = config.local_path_fix;
if(config.add_local_directory){
	file_path = __dirname + "/storage" + file_path;
}

fs.unlink(file_path, function (err) {
		if (err) { console.log(err); }
		console.log('Successfully reset ' + file_path );
		});


jobs.process('fix_bucket', 10, function(job, done){
	job.progress(0, 100);
	var info = job.data;
	if(config.s3fixit.Prefix === ""){
		done( 'Must Set a Preifx in the config file' );
	} else {

	if(info.Bucket === undefined || info.Bucket === null ){
		info = config.s3;
	}
	info.Preifx = config.s3fixit.Prefix;
	job.progress(50, 100);
	s3.client.listObjects( info , process_list_objects);
	//job.progress(100, 100);
	setTimeout(done, Math.random() * 1000);
	}
});


function process_list_objects(err,data) {
	var next_list = config.s3;
	for (var index in data.Contents) {
			var item = data.Contents[index];
			item.Bucket = config.s3.Bucket;
			item.title	= item.Key;
			next_list.Marker = item.Key;
			jobs.create("fix_file_info", item).save();
			//console.log("Item: ", item);
	}
	if(data.IsTruncated){
		jobs.create('fix_bucket', next_list).priority('high').save();
	} else {
		//jobs.create('crawl_bucket', config.s3 ).delay( (minute * 1)  ).save();
	}

}



jobs.process('fix_file_info', config.max_files_at_once , function(job, done){
	var info = job.data;
	jobs.create('fix_get_from_s3', info).save();
	done();
});



jobs.process('fix_get_from_s3',  config.max_files_at_once , function(job, done){

	var info = job.data;
		var file_path = config.local_path_fix + info.Key;
		if(config.add_local_directory){
			file_path = __dirname + "/storage" + file_path;
		}
		var create_dir = path.dirname( file_path );
		info.file = file_path;
		mkdirp(create_dir, function (err) {
			if (err) {
				done(err);
				//console.error(err);
			}
		});

		var download_file = fs.createWriteStream( file_path );

		download_file.on('error', function(err){
			if (err) return done(err);
		});

		download_file.on('close', function () {
			done();
			/*
			if(config.move_to_glacier){
				jobs.create('move_to_glacier', info ).save();
			}
			*/
		});

		var s = {
			Bucket: info.Bucket,
			Key: info.Key
		};
		s3.client.getObject( s ).createReadStream().pipe( download_file );
});






app.use(kue.app);
app.listen( parseInt( config.port + 1 ) );
console.log('UI started on port '+ parseInt(config.port + 1));