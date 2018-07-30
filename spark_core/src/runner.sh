#!/usr/bin/env bash
MY_HDFS_DIR="/user/root/spark_core/"
case $1 in    
"" | "-h" | "--help")
        echo "Usage: $0 "
        echo "Available subcommands: 
		to_hdfs -- copy files from local to hdfs, 
		run -- run
		rm -- remove results from hdfs"
        ;;    
to_hdfs)
        hdfs dfs -mkdir $MY_HDFS_DIR
	hdfs dfs -put spark_core/bids.txt $MY_HDFS_DIR
	hdfs dfs -put spark_core/exchange_rate.txt $MY_HDFS_DIR
	hdfs dfs -put spark_core/motels.txt $MY_HDFS_DIR
        ;;    
run)
        export SPARK_HOME=/usr/hdp/2.6.5.0-292/spark2/
	export PYSPARK_PYTHON=/home/raj_ops/myvenv/bin/python3.4
	spark-submit --py-files spark_core/classes.py spark_core/hw.py --master yarn
        ;;    
query)
        hive -f query.hql
        ;;    
rm)
	hdfs dfs -rm /user/root/spark_core/result.txt/aggregated/*
	hdfs dfs -rm /user/root/spark_core/result.txt/erroneous/*
	hdfs dfs -rmdir /user/root/spark_core/result.txt/aggregated
	hdfs dfs -rmdir /user/root/spark_core/result.txt/erroneous
	hdfs dfs -rmdir /user/root/spark_core/result.txt
	;;
ls)
	hdfs dfs -ls /user/root/spark_core/result.txt
	hdfs dfs -ls /user/root/spark_core/result.txt/aggregated
	hdfs dfs -ls /user/root/spark_core/result.txt/erroneous
	;;
*)     
echo "Error: '$subcommand' is not a known subcommand." >&2        
echo "       Run '$0 --help' for a list of known subcommands." >&2        
exit 1        
;;
esac
