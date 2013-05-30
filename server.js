var kue = require('kue'), express = require('express');
var mysql = require('mysql');
var fs = require('fs');
var path = require('path');
var mkdirp = require('mkdirp');
var AWS = require('aws-sdk');

var config = require('./config.json');

var minute = 60000;

var pool = mysql.createPool( config.database );


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
//jobs.create('crawl_bucket', config.s3 ).save();

jobs.process('crawl_bucket', 10, function(job, done){
	job.progress(0, 100);
	var info = job.data;
	if(info.Bucket === undefined || info.Bucket === null ){
		info = config.s3;
	}
	job.progress(50, 100);
	s3.client.listObjects( info , process_list_objects);
	//job.progress(100, 100);
	setTimeout(done, Math.random() * 1000);
});


function process_list_objects(err,data) {
	var next_list = config.s3;
	for (var index in data.Contents) {
			var item = data.Contents[index];
			item.Bucket = config.s3.Bucket;
			item.title	= item.Key;
			next_list.Marker = item.Key;
			//jobs.create("process_file_info", item).save();
			//console.log("Item: ", item);
	}
	if(data.IsTruncated){
		jobs.create('crawl_bucket', next_list).priority('high').save();
	} else {
		//jobs.create('crawl_bucket', config.s3 ).delay( (minute * 1)  ).save();
	}

}

jobs.promote();


jobs.process('process_file_info', config.max_files_at_once , function(job, done){

	var info = job.data;
	var new_file = false;
	pool.getConnection(function(err, connection) {
		if (err) throw done(err);
		connection.query('SELECT * FROM aws_status WHERE s3 = ?', info.Key, function(err, rows, fields) {
			if (err) throw done(err);

			if(!rows[0]){
				connection.query("INSERT INTO aws_status SET ? ", { s3: info.Key });
				jobs.create('get_from_s3', info).save();
			} else {
				if(!rows[0].glacier){
					jobs.create('get_from_s3', info).save();
				}
			}
		});
		connection.end();
	});
	done();
});


jobs.process('get_from_s3',  config.max_files_at_once , function(job, done){

	var info = job.data;
		var file_path = config.local_path + info.Key;
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
			if(config.move_to_glacier){
				jobs.create('move_to_glacier', info ).save();
			}
		});

		var s = {
			Bucket: info.Bucket,
			Key: info.Key
		};
		s3.client.getObject( s ).createReadStream().pipe( download_file );
});

jobs.process('move_to_glacier',  config.max_files_at_once, function(job, done){
	var info = job.data;
	job.log('Moving file to Glacier');
	fs.readFile(info.file, function (err, data) {
	if (err) return done(err);
	job.log('Loaded file from disk');
	glc.client.uploadArchive({
	vaultName: config.glacier.vault,
	archiveDescription: info.Key,
	body: data
	} , function (res, data) {
		if(config.delete_after_move){
		fs.unlink(info.file, function (err) {
		if (err) throw done(err);
		console.log('Successfully deleted ' + info.file );
		});
		}
		pool.getConnection(function(err, connection) {
			if (err) throw done(err);
			connection.query("UPDATE aws_status SET ? WHERE s3 = ? ", [ { glacier: info.Key } , info.Key ]);
			connection.end();
		});
		done();

	});
	});
});

app.use(kue.app);
app.listen(config.port);
console.log('UI started on port '+ config.port);