# s3 to Glacier

  Move files to Glacier from S3

## Installation

    $ npm install

## Start Backup

$ curl -H "Content-Type: application/json" -X POST -d \
    '{
       "type": "build_backup_jobs",
       "data": {
       },
       "options" : {}
     }' http://localhost:3000/job


curl -H "Content-Type: application/json" -X POST -d \
    '{
       "type": "crawl_dir",
       "data": {
       	"dir":"/Volumes/cluster34/1"
       },
       "options" : {}
     }'