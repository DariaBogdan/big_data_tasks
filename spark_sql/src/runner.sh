#!/usr/bin/env bash
MY_HDFS_DIR="/user/root/spark_sql/"
case $1 in    
"" | "-h" | "--help")
        echo "Usage: $0 "
        echo "Available subcommands: 
		to_hdfs -- copy files from local to hdfs, 
		run -- run
		rm -- remove results from hdfs
		cat_err -- show results for errors
		car_agg -- show results for aggregation"
        ;;    
to_hdfs)
        hdfs dfs -mkdir $MY_HDFS_DIR
	hdfs dfs -put bids.snappy.parquet $MY_HDFS_DIR
	hdfs dfs -put exchange_rate.snappy.parquet $MY_HDFS_DIR
	hdfs dfs -put motels.snappy.parquet $MY_HDFS_DIR
        ;;    
run)
        export SPARK_HOME=/usr/hdp/2.6.5.0-292/spark2/
	export PYSPARK_PYTHON=/home/raj_ops/myvenv/bin/python3.4
	spark-submit --master yarn hw.py
        ;;        
rm)
	hdfs dfs -rm /user/root/spark_sql/result/aggregated/*
	hdfs dfs -rm /user/root/spark_sql/result/erroneous/*
	hdfs dfs -rmdir /user/root/spark_sql/result/aggregated
	hdfs dfs -rmdir /user/root/spark_sql/result/erroneous
	hdfs dfs -rmdir /user/root/spark_sql/result
	;;
cat_agg)
	hdfs dfs -ls /user/root/spark_sql/result
	hdfs dfs -ls /user/root/spark_sql/result/aggregated
	hdfs dfs -cat /user/root/spark_sql/result/aggregated/* | head -n 1000
	;;
cat_err)
	hdfs dfs -ls /user/root/spark_sql/result
	hdfs dfs -ls /user/root/spark_sql/result/erroneous
	hdfs dfs -cat /user/root/spark_sql/result/erroneous/* | head -n 1000
	;;

*)     
echo "Error: '$subcommand' is not a known subcommand." >&2        
echo "       Run '$0 --help' for a list of known subcommands." >&2        
exit 1        
;;
esac
