#!/bin/sh
# run_daily_trends.sh
#
# Shell script for running daily trend estimation.
#
# Usage:
#
# Replace the input paths and the desired range
# then:
#
# $ bash trendingtopics/lib/scripts/run_daily_trends.sh MYBUCKET
#
# Produces a tab delimited trend output file "/mnt/daily_trends.txt" 
# ready to bulk load into the Rails app daily_trends table.
#
# To clean the output directory before running again:
#
# $ hadoop dfs -rmr finaltrendoutput
#

# echo $DAYS
# 20090603 20090604 20090605 20090606 20090607 20090608 20090609 20090610 20090611 20090612



#D0=`date --date "now -1 day" +"%Y%m%d"`
#D1=`date --date "now -2 day" +"%Y%m%d"`
#D2=`date --date "now -3 day" +"%Y%m%d"`
#D3=`date --date "now -4 day" +"%Y%m%d"`
#D4=`date --date "now -5 day" +"%Y%m%d"`
#D5=`date --date "now -6 day" +"%Y%m%d"`
#D6=`date --date "now -7 day" +"%Y%m%d"`
#D7=`date --date "now -8 day" +"%Y%m%d"`
#D8=`date --date "now -9 day" +"%Y%m%d"`
#D9=`date --date "now -10 day" +"%Y%m%d"`

# Run the streaming job
./bin/hadoop jar /usr/local/hadoop-0.20.2/contrib/streaming/hadoop-*-streaming.jar \
  -input dumps/pagecounts-* \
  -output finaltrendoutput \
  -mapper "daily_trends.py mapper" \
  -reducer "daily_trends.py reducer 1" \
  -file 'daily_trends.py' \

# Clear the logs
./bin/hadoop fs -rmr finaltrendoutput/_logs
