#!/usr/bin/env bash

wget https://dumps.wikimedia.org/other/pageviews/2019/2019-07/pageviews-20190707-110000.gz
gunzip pageviews-20190707-110000.gz
awk -F' ' '{print $1}' pageviews-20190707-110000 | sort | uniq -c | sort -nr | head
