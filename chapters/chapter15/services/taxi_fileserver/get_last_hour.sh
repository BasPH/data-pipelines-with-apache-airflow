#!/bin/sh

year_offset=$(($(date +%Y)-${DATA_YEAR}))
output_filename="$(date +%m-%d-%Y-%H-%M-%S).csv"

# Extract taxi rides finished in last 15 minutes
PGPASSWORD=${POSTGRES_PASSWORD} psql \
-v ON_ERROR_STOP=1 \
-h ${POSTGRES_HOST} \
-d ${POSTGRES_DATABASE} \
-U ${POSTGRES_USERNAME} \
<<-EOSQL
  \copy (SELECT pickup_datetime + INTERVAL '${year_offset} YEARS' AS pickup_datetime, dropoff_datetime + INTERVAL '${year_offset} YEARS' AS dropoff_datetime, pickup_locationid, dropoff_locationid, trip_distance FROM triprecords WHERE dropoff_datetime + interval '${year_offset} YEARS' <= NOW() AND dropoff_datetime + interval '${year_offset} YEARS' >= NOW() - interval '15 minutes') TO '/data/${output_filename}' WITH CSV HEADER;
EOSQL

echo "Deleting files older than 1 hour..."
# 59 because the last minute is inclusive
find /data -mmin +59 -print -delete
