# Amazon s3 to Glacier 

  Built for [node.js](http://nodejs.org) using [Kue](https://github.com/learnboost/kue) a priority job queue backed by [redis](http://redis.io).

## Installation
	
	$ cd /node-aws
    $ npm install

## Installing redis on OS X

    $ curl -O http://download.redis.io/redis-stable.tar.gz 
    $ tar -xvzf redis-stable.tar.gz 
    $ rm redis-stable.tar.gz
    $ cd redis-stable
    $ make 
    $ sudo make install