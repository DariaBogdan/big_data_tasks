#!/bin/bash
python3.4 longest_words.py -r hadoop hdfs:///user/raj_ops/war_and_peace.txt hdfs:///user/raj_ops/jerom.txt --hadoop-streaming-jar=/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar --python-bin=python3.4 --top=30
