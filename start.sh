~/spark-2.4.5-bin-hadoop2.7/bin/spark-submit \
  --class start.Start  \
  --master spark://DavideS:7077 \
  /home/davide/GitHub/ProcessingOfCarAccidentsData_Spark/target/ProcessingOfCarAccidentsData_Spark-1.0.jar \
  spark://DavideS:7077 \
  /home/davide/GitHub/ProcessingOfCarAccidentsData_Spark/