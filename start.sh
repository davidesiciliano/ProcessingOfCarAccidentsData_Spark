~/spark-2.4.5-bin-hadoop2.7/bin/spark-submit \
  --class start.Start  \
  --master spark://192.168.1.7:7077 \
  /home/davide/GitHub/ProcessingOfCarAccidentsData_Spark/target/ProcessingOfCarAccidentsData_Spark-1.0.jar \
  spark://192.168.1.7:7077 \
  /home/davide/GitHub/ProcessingOfCarAccidentsData_Spark/