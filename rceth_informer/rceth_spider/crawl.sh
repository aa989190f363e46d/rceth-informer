#!/usr/bin/env sh

DIR=data/`date +%Y-%m`
mkdir -p $DIR

F_NAME=$DIR/`date +%Y-%m-%d-%H-%M`

scrapy crawl -O$F_NAME.json --logfile=$F_NAME.log drugs_spider