#!/bin/bash

# Inspired by https://github.com/mrts/docker-postgresql-multiple-databases/blob/master/create-multiple-postgresql-databases.sh
# DB names hardcoded, script is created for demo purposes.

set -euxo pipefail

function create_user_and_database() {
	local database=$1
	echo "Creating user '$database' with database '$database'."
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER $database WITH PASSWORD '$database';
    CREATE DATABASE $database;
    GRANT ALL PRIVILEGES ON DATABASE $database TO $database;
EOSQL
}

# 1. Create databases
create_user_and_database "insideairbnb"

# 2. Create table for insideairbnb listings
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" insideairbnb <<-EOSQL
CREATE TABLE IF NOT EXISTS listings(
  id                             INTEGER,
  name                           TEXT,
  host_id                        INTEGER,
  host_name                      VARCHAR(100),
  neighbourhood_group            VARCHAR(100),
  neighbourhood                  VARCHAR(100),
  latitude                       NUMERIC(18,16),
  longitude                      NUMERIC(18,16),
  room_type                      VARCHAR(100),
  price                          INTEGER,
  minimum_nights                 INTEGER,
  number_of_reviews              INTEGER,
  last_review                    DATE,
  reviews_per_month              NUMERIC(5,2),
  calculated_host_listings_count INTEGER,
  availability_365               INTEGER,
  download_date                  DATE NOT NULL
);
EOSQL

# 3. Download Inside Airbnb Amsterdam listings data (http://insideairbnb.com/get-the-data.html)
listing_url="http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/{DATE}/visualisations/listings.csv"
listing_dates="
2019-12-07
2019-11-03
2019-10-15
2019-09-14
2019-08-08
2019-07-08
2019-06-04
2019-05-06
2019-04-08
2019-03-07
2019-02-04
2019-01-13
2018-12-06
2018-11-04
2018-10-05
2018-09-08
2018-08-07
2018-07-06
2018-06-06
2018-05-10
2018-04-07
2017-12-04
2017-11-03
2017-10-03
2017-09-04
2017-08-03
2017-07-03
2017-06-02
2017-05-03
2017-04-02
2017-03-02
2017-02-03
2017-01-02
2016-12-04
2016-11-03
2016-09-03
2016-08-04
2016-07-04
2016-06-03
2016-05-03
2016-04-04
2016-02-03
2016-01-03
2015-12-03
2015-11-06
2015-10-02
2015-09-03
2015-08-21
2015-04-05
"

mkdir -p /tmp/insideairbnb
for d in ${listing_dates}
do
  url=${listing_url/\{DATE\}/$d}
  wget $url -O /tmp/insideairbnb/listing-$d.csv

  # Hacky way to add the "download_date", by appending the date to all rows in the downloaded file
  sed -i "1 s/$/,download_date/" /tmp/insideairbnb/listing-$d.csv
  sed -i "2,$ s/$/,$d/" /tmp/insideairbnb/listing-$d.csv

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" insideairbnb <<-EOSQL
    COPY listings FROM '/tmp/insideairbnb/listing-$d.csv' DELIMITER ',' CSV HEADER QUOTE '"';
EOSQL
done

function grant_all() {
	local database=$1
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" $database <<-EOSQL
    ALTER SCHEMA public OWNER TO $database;
    GRANT USAGE ON SCHEMA public TO $database;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $database;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $database;
    GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO $database;
EOSQL
}

# Somehow the database-specific privileges must be set AFTERWARDS
grant_all "insideairbnb"

pg_ctl stop
