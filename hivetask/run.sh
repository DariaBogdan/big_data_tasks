#!/usr/bin/env bash
MY_HDFS_DIR="/user/raj_ops/hivetask/"
case $1 in    
"" | "-h" | "--help")
        echo "Usage: $0 "
        echo "Available subcommands: 
		to_hdfs -- copy files from local to hdfs, 
		create_tables -- create tables and fill them with values in files, 
		q02 -- run task01_question02.hql, 
		q03 -- run task01_question03.hql, 
		q04 -- run task01_question04.hql, 
		q05 -- run task01_question05.hql"
        ;;    
to_hdfs)
        hdfs dfs -mkdir $MY_HDFS_DIR
	hdfs dfs -put carriers.csv $MY_HDFS_DIR
	hdfs dfs -put airports.csv $MY_HDFS_DIR
	hdfs dfs -put 2007.csv $MY_HDFS_DIR
        ;;    
create_tables)
        beeline -u jdbc:hive2://localhost:10000/default -n root -f create_tables.hql --hivevar "MY_HDFS_DIR=$MY_HDFS_DIR"
        ;;    
q02)
        beeline -u jdbc:hive2://localhost:10000/default -n root -f task01_question02.hql
        ;;    
q03)
        beeline -u jdbc:hive2://localhost:10000/default -n root -f task01_question03.hql
        ;;    
q04)
        beeline -u jdbc:hive2://localhost:10000/default -n root -f task01_question04.hql
        ;;    
q05)
        beeline -u jdbc:hive2://localhost:10000/default -n root -f task01_question05.hql
        ;;    

*)     
echo "Error: '$subcommand' is not a known subcommand." >&2        
echo "       Run '$0 --help' for a list of known subcommands." >&2        
exit 1        
;;
esac
