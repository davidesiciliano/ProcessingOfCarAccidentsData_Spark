# ProcessingOfCarAccidentsData_Spark

## Dataset
https://drive.google.com/open?id=1hhd9TmIvRSk_ZLmhAINAMa7uPz_SZqln

## Instruction
You can either run the examples in local mode or on a cluster

### Start a Spark cluster

Download spark (compiled version) and from the main folder run the following commands
- Start a master: ./sbin/start-master.sh
- Start a slave (executor): ./sbin/start-slave.sh <master-URL>

In the case you want to record events on the history server
- Add "spark.eventLog.enabled true" in the file conf/spark-defaults.conf (before starting the master)
- Add "spark.eventLog.dir /path/to/your/log/dir" in the file conf/spark-defaults.conf (before starting the master)
- Start a history server: ./sbin/start-history-server.sh

### Compile the project and obtain a jar

From the main folder of the project (where the pom.xml file is located)
- mvn package

### Submit a job

From the Spark main folder
- ./bin/spark-submit --class main.class.you.want.to.run /path/to/generated/jar <args>

### Check the execution

From you browser
- 127.0.0.1:8080
- 127.0.0.1:18080 (history server)

### Stop Workers
for pid in $(jps | grep Worker | awk '{print $1}'); do kill -9 $pid; done

### Start Workers
SPARK_WORKER_INSTANCES=4 SPARK_WORKER_CORES=1 ./sbin/start-slave.sh spark://DavideS:7077