package start;

import database.Database;
import database.DatabaseCache;
import database.DatabaseNoCache;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.LogUtils;

import java.time.Duration;
import java.time.LocalDateTime;

public class Start {
    public static void main(String[] args) {
        LogUtils.setLogLevel();

        // master is "local" for the local execution and "yarn" for the execution on HDInsight
        final String master = args.length > 0 ? args[0] : "local";
        final String filePath = args.length > 1 ? args[1] : "./";

        System.out.println(">>>> " + master);
        System.out.println(">>>> " + filePath);

        // creates the SparkSession
        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("CarAccidents")
                .getOrCreate();

        Database database = new DatabaseCache(spark);
        //Database database = new DatabaseNoCache(spark);

        //region: Initial Time
        LocalDateTime init = LocalDateTime.now();
        System.out.println(">>>>>> INIT TIME: " + init);
        //endregion

        database.loadDataset(filePath);

        //region: Reading Time
        LocalDateTime afterReading = LocalDateTime.now();
        Duration readingTime = Duration.between(init, afterReading);
        System.out.println(">>>>>> READING TIME: " + readingTime);
        //endregion

        final Dataset<Row> query3 = database.executeQuery3();
        query3.show();
        //query3.coalesce(1).write().option("header", "true").csv("./query3");

        //region: Query 3 Time
        LocalDateTime endQuery3 = LocalDateTime.now();
        System.out.println(">>>>>> END TIME QUERY 3: " + endQuery3);
        Duration executionTimeQuery3 = Duration.between(afterReading, endQuery3);
        System.out.println(">>>>>> EXECUTION TIME QUERY 3: " + executionTimeQuery3);
        //endregion

        final Dataset<Row> query1 = database.executeQuery1();
        query1.show();
        //query1.coalesce(1).write().option("header", "true").csv("./query1");

        //region: Query 1 Time
        LocalDateTime endQuery1 = LocalDateTime.now();
        System.out.println(">>>>>> END TIME QUERY 1: " + endQuery1);
        Duration executionTimeQuery1 = Duration.between(endQuery3, endQuery1);
        System.out.println(">>>>>> EXECUTION TIME QUERY 1: " + executionTimeQuery1);
        //endregion

        final Dataset<Row> query2 = database.executeQuery2();
        query2.show();
        //query2.coalesce(1).write().option("header", "true").csv("./query2");

        //region: Query 2 Time
        LocalDateTime endQuery2 = LocalDateTime.now();
        System.out.println(">>>>>> END TIME QUERY 2: " + endQuery2);
        Duration executionTimeQuery2 = Duration.between(endQuery1, endQuery2);
        System.out.println(">>>>>> EXECUTION TIME QUERY 2: " + executionTimeQuery2 + "\n");
        //endregion

        //region: Execution Time
        LocalDateTime end = LocalDateTime.now();
        Duration totalTime = Duration.between(init, end);
        System.out.println(">>>>>> TOTAL TIME: " + totalTime);
        System.out.println(">>> Reading Time: " + readingTime);
        System.out.println(">>> Execution Time Query 1: " + executionTimeQuery1);
        System.out.println(">>> Execution Time Query 2: " + executionTimeQuery2);
        System.out.println(">>> Execution Time Query 3: " + executionTimeQuery3);
        //endregion

        // close the spark session
        spark.close();
    }
}
