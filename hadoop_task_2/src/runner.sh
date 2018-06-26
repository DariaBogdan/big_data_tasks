#!/bin/bash
python3.4 logs_stats.py -r hadoop hdfs:///user/raj_ops/input_logs.txt --hadoop-streaming-jar=/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar --python-bin=python3.4 --compress=false --output_format=csv
