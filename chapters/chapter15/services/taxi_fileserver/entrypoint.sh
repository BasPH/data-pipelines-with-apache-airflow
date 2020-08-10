#!/bin/sh

echo "Starting cron..."
crond -b

echo "Started cron. Starting nginx..."
exec nginx -g 'daemon off;'
