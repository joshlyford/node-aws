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


jobs.process('crawl_dir', 1, function(job, done){
	var info = job.data;
	fs.readdir(info.dir, function(err, list) {
		var pending = list.length;
		list.forEach(function(file) {
		file = info.dir + '/' + file;
		//console.log(file);
		fs.stat(file, function(err, stat) {
			if (stat && stat.isDirectory()) {
				jobs.create('crawl_dir', { dir: file, title: file } ).save();
			} else {
				jobs.create('process_local_file', { file: file , title: file } ).save();
			}
		});
		});
		done();
	});
});



jobs.process('process_local_file', 10, function(job, done){ 
	//console.log(results.length);
	var info = job.data;
	pool.getConnection(function(err, connection) {
		if (err){  console.log(err); }
		connection.query('SELECT * FROM ftp_aws_index WHERE ftp LIKE ?', info.file, function(err, rows, fields) {

			if (err){  console.log(err); }
			if(!rows[0]){
			job.log("add");
			var ftpcluster = "34";
			connection.query("INSERT INTO ftp_aws_index SET ?", { ftp: info.file , cluster: ftpcluster });
			//console.log('File', file, 'has been added');
			done();
			} else {
			job.log("add");
			done();
			}
			connection.end();
		});
	});	
	
	
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
			file_path = __dirname + file_path;
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

jobs.process('build_backup_jobs',  config.max_files_at_once, function(job, done){
	var info = job.data;
	job.log('Load info from db');
	pool.getConnection(function(err, connection) {
			if (err) throw done(err);
			connection.query("select * from ftp_aws_index WHERE s3 is NULL ",'',  function(err, rows, fields) {
			if (err){  console.log(err); }
			console.log(rows.length);
			job.progress(0, rows.length);

			for (var i = 0; i < rows.length; i++) {
				console.log('File', rows[i].ftp, 'has been added');
				job.progress(i, rows.length);
				jobs.create('ftp_move_to_s3', rows[i] ).save();
			};

	
		});
			connection.end();
			done();
		});
});

//jobs.create('build_backup_jobs').save();




jobs.process('ftp_move_to_s3', config.max_files_at_once, function(job, done){
  	var info = job.data;
  	var n=info.ftp.split("cluster"+info.cluster+"/");
	if(n[1]){
		fs.readFile(info.ftp, function (err, data) {

			console.log('attempt?');
    if (err) { throw err; }
    console.log({

    	Bucket:"brewlabs-node",
    	Key: n[1],
    	Body: data
    });
    s3.client.putObject({

    	Bucket:"brewlabs-node",
    	Key: n[1],
    	Body: data
    } , function (res, data) {
    		done();
            console.log( res +'Successfully uploaded file.' + data);
        });
});


	} else {
		done('Could not find Cluster');
	}
// done();
});

app.use(kue.app);
app.listen(config.port);
console.log('UI started on port '+ config.port);


