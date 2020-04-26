package start;

import database.Constants;
import database.Database;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import utils.LogUtils;

import java.util.ArrayList;
import java.util.List;

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

        final Dataset<Row> completeDataset = Database.getDB().getDataset();

        //QUERY 1: Number of lethal accidents per week throughout the entire dataset
        //QUERY 2: Number of accidents and percentage of number of deaths per contributing factor in the dataset
        //QUERY 3: Number of accidents and average number of lethal accidents per week per borough

        final Dataset<Row> cleanDataset = completeDataset
                .where(col(Constants.BOROUGH).isNotNull()
                        .and(col(Constants.DATE).isNotNull()));

        final Dataset<Row> d1 = cleanDataset
                .withColumn(Constants.WEEK, date_format(col(Constants.DATE), "w"))
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

        // con dataset corretto parte da 2014
        // mentre con dataset sbagliato parte da 2012
        // --> i risultati comunque sono sbagliati rispetto alle foto
        final Dataset<Row> d2 = d1
                .groupBy(Constants.BOROUGH, Constants.YEAR, Constants.WEEK)
                .agg(count("*").as(Constants.NUMBER_ACCIDENTS),
                        count(when(col(Constants.NUMBER_KILLED).$greater(0), true)).as(Constants.NUMBER_LETHAL_ACCIDENTS),
                        sum(col(Constants.NUMBER_INJURED)).as("sum(NumberInjured)"),
                        sum(col(Constants.NUMBER_KILLED)).as("sum(NumberKilled)"))
                .withColumn("avgLethal", col(Constants.NUMBER_LETHAL_ACCIDENTS).divide(col(Constants.NUMBER_ACCIDENTS)))
                .sort(Constants.YEAR, Constants.WEEK, Constants.BOROUGH);
        d2.show(100);

        spark.close();
    }
}
