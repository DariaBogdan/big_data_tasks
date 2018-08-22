#!/usr/bin/env bash
case $1 in    
"" | "-h" | "--help")
        echo "Usage: $0 "
        echo "Available subcommands: 
		start_zeppelin -- start zeppelin, 
		run_python_gateway -- run code to take predictions from java, 
		run_producer -- run producer,
		run_streaming -- run streaming"
        ;;    
start_zeppelin)
        sudo /usr/hdp/2.6.5.0-292/zeppelin/bin/zeppelin-daemon.sh start
        ;;    
run_python_gateway)
	sh ./sh/build_java.sh
	sh ./sh/run_python_gateway.sh
        ;;    
run_producer)
        python3 kafka_producer.py
        ;;    
run_streaming)
	PYSPARK_PYTHON=python3 SPARK_HOME=/usr/hdp/current/spark2-client spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 script_with_kafka.py
	;;
*)     
echo "Error: '$subcommand' is not a known subcommand." >&2        
echo "       Run '$0 --help' for a list of known subcommands." >&2        
exit 1        
;;
esac
