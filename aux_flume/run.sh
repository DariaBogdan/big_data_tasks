#!/usr/bin/env bash
MY_HDFS_FILES="/user/raj_ops/flumetask/*"
AGENT="a1"
CONF="my.conf"
case $1 in    
"" | "-h" | "--help")
        echo "Usage: $0 "
        echo "Available subcommands: 
		cat -- run cat file, 
		run_flume_agent -- run flume agent, 
		results -- cat resulted HDFS files"
        ;;    
cat)
	cat linux_messages_3000lines.txt | while read line ; do echo "$line" ; sleep 0.2 ; done > output.txt
        ;;    
run_flume_agent)
        /usr/bin/flume-ng agent -n $AGENT -c conf -f $CONF
        ;;    
results)
        hdfs dfs -cat $MY_HDFS_FILES
        ;;    
*)     
echo "Error: '$subcommand' is not a known subcommand." >&2        
echo "       Run '$0 --help' for a list of known subcommands." >&2        
exit 1        
;;
esac
