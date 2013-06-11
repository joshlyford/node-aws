/*
*	Make one file per Cluster to be Synced.
*/
var fs = require('fs');
var chokidar = require('chokidar');
var mysql = require('mysql');

var config = require('./config.json');
var ftpcluster = "34";
var pool = mysql.createPool( config.database );
var dirs = ["/Volumes/cluster34/"];
/*
for (var i = 0; i < 200; i++) {
	dirs.push("/Volumes/cluster34/" + i +"/");
}
//console.log(dirs);


fs.readdir("/Volumes/cluster34/", function(err, files){
	console.log(files);

});
*/
var walk = function(dir, done) {
  var results = [];
  fs.readdir(dir, function(err, list) {
    if (err) return done(err);
    var pending = list.length;
    if (!pending) return done(null, results);
    list.forEach(function(file) {
      file = dir + '/' + file;
      //console.log(file);
      fs.stat(file, function(err, stat) {
        if (stat && stat.isDirectory()) {
          walk(file, function(err, res) {
            results = results.concat(res);
            if (!--pending) done(null, results);
          });
        } else {


          results.push(file);
          //console.log(results.length);
          pool.getConnection(function(err, connection) {
			if (err){  console.log(err); }
			connection.query('SELECT * FROM ftp_aws_index WHERE ftp LIKE ?', file, function(err, rows, fields) {
				
				if (err){  console.log(err); }
				if(!rows[0]){
					console.log("add "+ file);
					connection.query("INSERT INTO ftp_aws_index SET ?", { ftp: file , cluster: ftpcluster });
					//console.log('File', file, 'has been added');
				
				} else {
					console.log("skip "+ file);
				}
			});
		connection.end();
	});


          if (!--pending) done(null, results);
        }
      });
    });
  });
};

/*
walk("/Volumes/cluster34/1", function(err, results) {
  if (err) throw err;
  console.log(results);
});
*/
walk("/Volumes/cluster34", function(err, results) {
  if (err) throw err;
  console.log(results.length);

});
/*


var watcher = chokidar.watch( dirs , {ignoreInitial: false, persistent: true});

watcher
  .on('add', function(path) {
  	console.log(path);
  	pool.getConnection(function(err, connection) {
		if (err){  console.log(err); }
		connection.query('SELECT * FROM ftp_aws_index WHERE ftp = ?', path, function(err, rows, fields) {
			if (err){  console.log(err); }

			if(!rows[0]){
				connection.query("INSERT INTO ftp_aws_index SET ?", { ftp: path , cluster: ftpcluster });
				console.log('File', path, 'has been added');
			}
		});
		connection.end();
	});

  })
.on('change', function(path) {console.log('File', path, 'has been changed');})
.on('unlink', function(path) {console.log('File', path, 'has been removed');})
.on('error', function(error) {console.error('Error happened', error);})
watcher.close();
*/
