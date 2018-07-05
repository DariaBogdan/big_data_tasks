#!/bin/bash
hdfs dfs -mkdir /user/raj_ops/hivetask
hdfs dfs -put carriers.csv /user/raj_ops/hivetask/
hdfs dfs -put airports.csv /user/raj_ops/hivetask/
hdfs dfs -put 2007.csv /user/raj_ops/hivetask/

