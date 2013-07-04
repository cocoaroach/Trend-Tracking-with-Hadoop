Trend-Tracking-with-Hadoop
==========================
daily_timelines.py and daily_trends.py contains the map() and reduce() functions required for the job.

1) Obtain Wikipedia dump files from http://dumps.wikimedia.org/other/pagecounts-raw/
2) Transfer them to your HDFS volume.
3) Place daily_timelines.py, daily_trends.py, run_daily_timelines.sh and run_daily_trends.sh to your Hadoop root directory.
4) Make necessary changes to the shell scripts and execute them. Results will be saved in HDFS

This project is in a very crude form. There are plans to refine it along with adding much more thorough documentation.

