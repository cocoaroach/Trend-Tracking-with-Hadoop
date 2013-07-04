#!/bin/sh
# run_daily_timelines.sh
#
# Shell script for running daily timeline aggregation
# and monthly trend estimation.
# 
#
# Usage:
#
# Replace the input paths and the desired range
# 
# The streaming command has a cutoff that requires a minimum of 
# 45 days of data. But for testing, has been hardcoded 1day.
#
# Produces a tab delimited trend output file "daily_trends.txt" 
# and a "pages.txt" in /mnt/trendsdb.tar.gz
# to load into the Rails app MySQL db.
#
# To clean the output directories before running again:
#
# $ hadoop dfs -rmr stage1-output
# $ hadoop dfs -rmr finaloutput

# start the hadoop streaming jobs
./bin/hadoop jar /usr/local/hadoop-0.20.2/contrib/streaming/hadoop-*-streaming.jar \
  -input dumps/pagecounts-20* \
  -output stage1-output \
  -mapper "daily_timelines.py mapper1" \
  -reducer "daily_timelines.py reducer1" \
  -file 'daily_timelines.py' \

./bin/hadoop jar /usr/local/hadoop-0.20.2/contrib/streaming/hadoop-*-streaming.jar \
  -input stage1-output \
  -output finaloutput \
  -mapper "daily_timelines.py mapper2" \
  -reducer "daily_timelines.py reducer2 1" \
  -file 'daily_timelines.py' \
