#!/bin/bash

set -eux

# Create database
psql -v ON_ERROR_STOP=1 <<-EOSQL
  CREATE DATABASE citibike;
EOSQL

# Create table
psql -v ON_ERROR_STOP=1 citibike <<-EOSQL
  CREATE TABLE IF NOT EXISTS tripdata (
    tripduration            INTEGER,
    starttime               TIMESTAMP,
    stoptime                TIMESTAMP,
    start_station_id        INTEGER,
    start_station_name      VARCHAR(50),
    start_station_latitude  FLOAT8,
    start_station_longitude FLOAT8,
    end_station_id          INTEGER,
    end_station_name        VARCHAR(50),
    end_station_latitude    FLOAT8,
    end_station_longitude   FLOAT8
  );
EOSQL

# Load data
urls="
https://s3.amazonaws.com/tripdata/201901-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201902-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201903-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201904-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201905-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201906-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201907-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201908-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201909-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201910-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201911-citibike-tripdata.csv.zip
https://s3.amazonaws.com/tripdata/201912-citibike-tripdata.csv.zip
"

for url in ${urls}
do
  wget "${url}" -O /tmp/citibike-tripdata.csv.zip # Download data
  unzip /tmp/citibike-tripdata.csv.zip -d /tmp # Unzip
  filename=$(echo ${url} | sed 's:.*/::' | sed 's/\.zip$//') # Determine filename of unzipped CSV (this is the same as the .zip file)
  # Filter lines otherwise full Docker image is 4.18GB, every 8th line results in 890MB
  time awk -F',' 'NR == 1 || NR % 8 == 0 {print $1","$2","$3","$4","$5","$6","$7","$8","$9","$10","$11}' /tmp/${filename} | grep -v "NULL" > /tmp/citibike-tripdata.csv # Extract specific columns, write result to new file
  time psql -v ON_ERROR_STOP=1 citibike <<-EOSQL
    COPY tripdata
    FROM '/tmp/citibike-tripdata.csv' DELIMITER ',' CSV HEADER;
EOSQL
done

psql -v ON_ERROR_STOP=1 <<-EOSQL
  CREATE USER citi WITH PASSWORD 'cycling';
  GRANT ALL PRIVILEGES ON DATABASE citibike TO citi;
  \c citibike;
  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO citi;
  GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO citi;
EOSQL

pg_ctl stop
