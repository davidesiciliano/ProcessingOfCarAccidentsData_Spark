package start;

import database.Constants;
import database.Database;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.LogUtils;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.apache.spark.sql.functions.*;

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

        Database.initializeDatabase(spark);
        Database.getDB().constructSchema();
        Database.getDB().loadDataset(filePath);

        LocalDateTime init = LocalDateTime.now();
        System.out.println(">>>>>>INIT TIME: " + init);

        final Dataset<Row> completeDataset = Database.getDB().getDataset();

        System.out.println(">>>>> DIM INIZIALE: " + completeDataset.count());

        //QUERY 1: Number of lethal accidents per week throughout the entire dataset
        //QUERY 2: Number of accidents and percentage of number of deaths per contributing factor in the dataset
        //QUERY 3: Number of accidents and average number of lethal accidents per week per borough

        LocalDateTime afterReading = LocalDateTime.now();
        Duration readingTime = Duration.between(init, afterReading);
        System.out.println(">>>>>>READING TIME: " + readingTime);

        final Dataset<Row> cleanDataset = completeDataset
                .where(col(Constants.BOROUGH).isNotNull()
                        .and(col(Constants.DATE).isNotNull()));

        final Dataset<Row> d1 = cleanDataset
                .withColumn(Constants.WEEK, weekofyear(col(Constants.DATE)))
                .withColumn(Constants.YEAR, date_format(col(Constants.DATE), "yyyy"))
                .withColumn(Constants.NUMBER_INJURED, col(Constants.NUMBER_OF_PERSONS_INJURED)
                        .plus(col(Constants.NUMBER_OF_PEDESTRIANS_INJURED))
                        .plus(col(Constants.NUMBER_OF_CYCLIST_INJURED))
                        .plus(col(Constants.NUMBER_OF_MOTORIST_INJURED)))
                .withColumn(Constants.NUMBER_KILLED, col(Constants.NUMBER_OF_PERSONS_KILLED)
                        .plus(col(Constants.NUMBER_OF_PEDESTRIANS_KILLED))
                        .plus(col(Constants.NUMBER_OF_CYCLIST_KILLED))
                        .plus(col(Constants.NUMBER_OF_MOTORIST_KILLED)))
                .select(Constants.BOROUGH,
                        Constants.YEAR,
                        Constants.WEEK,
                        Constants.NUMBER_INJURED,
                        Constants.NUMBER_KILLED)
                .cache();

        LocalDateTime initQuery3 = LocalDateTime.now();
        System.out.println(">>>>>>INIT TIME QUERY 3: " + initQuery3);
        Duration cleaningTime = Duration.between(afterReading, initQuery3);
        System.out.println(">>>>>>EXECUTION TIME CLEANING: " + cleaningTime);

        final Dataset<Row> query3 = d1
                .groupBy(Constants.BOROUGH, Constants.YEAR, Constants.WEEK)
                .agg(count("*").as(Constants.NUMBER_ACCIDENTS),
                        count(when(col(Constants.NUMBER_KILLED).$greater(0), true)).as(Constants.NUMBER_LETHAL_ACCIDENTS),
                        sum(col(Constants.NUMBER_INJURED)).as(Constants.SUM_NUMBER_INJURED),
                        sum(col(Constants.NUMBER_KILLED)).as(Constants.SUM_NUMBER_KILLED))
                .withColumn(Constants.AVERAGE_NUMBER_LETHAL_ACCIDENTS, col(Constants.NUMBER_LETHAL_ACCIDENTS).divide(col(Constants.NUMBER_ACCIDENTS)))
                .sort(Constants.YEAR, Constants.WEEK, Constants.BOROUGH);
        query3.show();

        LocalDateTime endQuery3 = LocalDateTime.now();
        System.out.println(">>>>>>END TIME QUERY 3: " + endQuery3);
        Duration executionTimeQuery3 = Duration.between(initQuery3, endQuery3);
        System.out.println(">>>>>>EXECUTION TIME QUERY 3: " + executionTimeQuery3);

        final Dataset<Row> query1 = query3
                .groupBy(Constants.YEAR, Constants.WEEK)
                .agg(sum(col(Constants.NUMBER_LETHAL_ACCIDENTS)).as(Constants.SUM_NUMBER_LETHAL_ACCIDENTS))
                .select(Constants.YEAR, Constants.WEEK, Constants.SUM_NUMBER_LETHAL_ACCIDENTS)
                .sort(Constants.YEAR, Constants.WEEK);
        query1.show();

        LocalDateTime endQuery1 = LocalDateTime.now();
        System.out.println(">>>>>>END TIME QUERY 1: " + endQuery1);
        Duration executionTimeQuery1 = Duration.between(endQuery3, endQuery1);
        System.out.println(">>>>>>EXECUTION TIME QUERY 1: " + executionTimeQuery1);

        LocalDateTime end = LocalDateTime.now();
        Duration totalTime = Duration.between(init, end);
        System.out.println(">>>>>>TOTAL TIME: " + totalTime);

        spark.close();
    }
}
