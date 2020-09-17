#!/usr/bin/env bash

wget https://dumps.wikimedia.org/other/pageviews/2019/2019-07/pageviews-20190701-010000.gz
gunzip pageviews-20190701-010000.gz
head pageviews-20190701-010000
