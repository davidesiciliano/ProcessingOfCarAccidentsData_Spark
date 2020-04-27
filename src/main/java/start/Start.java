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
                        .and(col(Constants.DATE).isNotNull()))
                .cache();

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
                        Constants.NUMBER_KILLED);
        d1.show();

        // --> i risultati comunque sono sbagliati rispetto alle foto
        final Dataset<Row> query3 = d1
                .groupBy(Constants.BOROUGH, Constants.YEAR, Constants.WEEK)
                .agg(count("*").as(Constants.NUMBER_ACCIDENTS),
                        count(when(col(Constants.NUMBER_KILLED).$greater(0), true)).as(Constants.NUMBER_LETHAL_ACCIDENTS),
                        sum(col(Constants.NUMBER_INJURED)).as("sum(NumberInjured)"),
                        sum(col(Constants.NUMBER_KILLED)).as("sum(NumberKilled)"))
                .withColumn("avgLethal", col(Constants.NUMBER_LETHAL_ACCIDENTS).divide(col(Constants.NUMBER_ACCIDENTS)))
                .sort(Constants.YEAR, Constants.WEEK, Constants.BOROUGH);
        query3.show(100);

        LocalDateTime end = LocalDateTime.now();
        System.out.println(">>>>>>END TIME: " + end);
        Duration executionTime = Duration.between(afterReading, end);
        System.out.println(">>>>>>EXECUTION TIME: " + executionTime);
        Duration totalTime = Duration.between(init, end);
        System.out.println(">>>>>>TOTAL TIME: " + totalTime);

        spark.close();
    }
}
