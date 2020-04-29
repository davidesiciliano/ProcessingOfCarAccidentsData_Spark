package start;

import database.Database;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.LogUtils;

import java.time.Duration;
import java.time.LocalDateTime;

public class Start {
    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        System.out.println(">>>> " + master);
        System.out.println(">>>> " + filePath);

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("CarAccidents")
                .getOrCreate();

        //INIT TIME
        LocalDateTime init = LocalDateTime.now();
        System.out.println(">>>>>>INIT TIME: " + init);

        Database.initializeDatabase(spark);
        Database.getDB().loadDataset(filePath);

        //READING TIME
        LocalDateTime afterReading = LocalDateTime.now();
        Duration readingTime = Duration.between(init, afterReading);
        System.out.println(">>>>>>READING TIME: " + readingTime);

        final Dataset<Row> query3 = Database.getDB().executeQuery3();
        query3.show();

        //QUERY 3 TIME
        LocalDateTime endQuery3 = LocalDateTime.now();
        System.out.println(">>>>>>END TIME QUERY 3: " + endQuery3);
        Duration executionTimeQuery3 = Duration.between(afterReading, endQuery3);
        System.out.println(">>>>>>EXECUTION TIME QUERY 3: " + executionTimeQuery3);

        final Dataset<Row> query1 = Database.getDB().executeQuery1();
        query1.show();

        //QUERY 1 TIME
        LocalDateTime endQuery1 = LocalDateTime.now();
        System.out.println(">>>>>>END TIME QUERY 1: " + endQuery1);
        Duration executionTimeQuery1 = Duration.between(endQuery3, endQuery1);
        System.out.println(">>>>>>EXECUTION TIME QUERY 1: " + executionTimeQuery1);

        //final Dataset<Row> query2 = Database.getDB().executeQuery2();
        //query2.show();

        //QUERY 2 TIME
        LocalDateTime endQuery2 = LocalDateTime.now();
        System.out.println(">>>>>>END TIME QUERY 2: " + endQuery2);
        Duration executionTimeQuery2 = Duration.between(endQuery1, endQuery2);
        System.out.println(">>>>>>EXECUTION TIME QUERY 2: " + executionTimeQuery2);

        //TOTAL TIME
        LocalDateTime end = LocalDateTime.now();
        Duration totalTime = Duration.between(init, end);
        System.out.println(">>>>>>TOTAL TIME: " + totalTime);

        spark.close();
    }
}
