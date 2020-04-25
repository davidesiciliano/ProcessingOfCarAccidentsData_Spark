package start;

import database.Constants;
import database.Database;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.LogUtils;

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

        final Dataset<Row> dataset = Database.getDB().getDataset();

        final Dataset<Row> d1 = dataset
                .withColumn(Constants.WEEK, date_format(col(Constants.DATE), "w"))
                .withColumn(Constants.YEAR, date_format(col(Constants.DATE), "yyyy"))
                .select(Constants.WEEK, Constants.YEAR, Constants.BOROUGH, Constants.NUMBER_OF_PERSONS_KILLED, Constants.NUMBER_OF_PERSONS_INJURED);
        d1.show();

        spark.close();
    }
}
