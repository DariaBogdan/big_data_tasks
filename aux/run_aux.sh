#!/usr/bin/env bash
MY_HDFS_DIR="/user/raj_ops/auxtask/"
DATABASE="auxtask"
MYSQL_CREDS=".my.cnf"
PATH_TO_FILES='./auxfiles/*'
case $1 in    
"" | "-h" | "--help")
        echo "Usage: $0 "
        echo "Available subcommands: 
		to_hdfs -- copy files from local to hdfs, 
		prepare_table -- drop and create DB '$DATABASE', drop and create table 'weather', 
		export -- export files content from HDFS to MySQL table 'weather'
		results -- make two sql-queries"
        ;;    
prepare_table)
	mysql --defaults-file=$MYSQL_CREDS -e "drop database if exists $DATABASE";
	mysql --defaults-file=$MYSQL_CREDS -e "create database $DATABASE";
	mysql --defaults-file=$MYSQL_CREDS $DATABASE -e "drop table if exists weather";
	mysql --defaults-file=$MYSQL_CREDS $DATABASE -e "create table weather (stationid varchar(20), date date, tmin int, tmax int, snow int, snwd int, prcp int)";
	mysql --defaults-file=$MYSQL_CREDS $DATABASE -e "drop table if exists weather_stage";
	mysql --defaults-file=$MYSQL_CREDS $DATABASE -e "create table weather_stage like weather";

        ;;    
to_hdfs)
        hadoop fs -mkdir $MY_HDFS_DIR
	hadoop fs -put $PATH_TO_FILES $MY_HDFS_DIR
        ;;    
export)
        sqoop export --connect jdbc:mysql://localhost/$DATABASE --table weather --staging-table weather_stage --export-dir $MY_HDFS_DIR --username root --password hadoop
        ;;    
results)
        mysql --defaults-file=$MYSQL_CREDS $DATABASE -e "SELECT count(*) FROM weather"
	mysql --defaults-file=$MYSQL_CREDS $DATABASE -e "SELECT * FROM weather ORDER BY stationid, date LIMIT 10"
        ;;    
*)     
echo "Error: '$subcommand' is not a known subcommand." >&2        
echo "       Run '$0 --help' for a list of known subcommands." >&2        
exit 1        
;;
esac
