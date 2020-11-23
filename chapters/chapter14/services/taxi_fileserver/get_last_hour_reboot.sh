#!/bin/sh

year_offset=$(($(date +%Y)-${DATA_YEAR}))

current_epoch=$(date "+%s")
floor_quarter=$((${current_epoch} / (15 * 60) * (15 * 60)))

for i in $(seq 0 3); do
  interval_epoch_end=$((${floor_quarter} - (15 * 60 * ${i})))
  output_filename="$(date -d @${interval_epoch_end} "+%m-%d-%Y-%H-%M-%S").csv"

  PGPASSWORD=${POSTGRES_PASSWORD} psql \
  -v ON_ERROR_STOP=1 \
  -h ${POSTGRES_HOST} \
  -d ${POSTGRES_DATABASE} \
  -U ${POSTGRES_USERNAME} \
  <<-EOSQL
  \copy (SELECT pickup_datetime + INTERVAL '${year_offset} YEARS' AS pickup_datetime, dropoff_datetime + INTERVAL '${year_offset} YEARS' AS dropoff_datetime, pickup_locationid, dropoff_locationid, trip_distance FROM triprecords WHERE dropoff_datetime + interval '${year_offset} YEARS' <= to_timestamp(${floor_quarter}) - interval '$((${i} * 15)) minutes' AND dropoff_datetime + interval '${year_offset} YEARS' >= NOW() - interval '($(((${i} + 1) * 15)) minutes') TO '/data/${output_filename}' WITH CSV HEADER;
EOSQL
done
