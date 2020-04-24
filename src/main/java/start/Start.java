package start;

import database.Constants;
import database.Database;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.LogUtils;

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

        final Dataset<Row> prova = Database.getDB().loadData(filePath);

        prova.show();

        final Dataset<Row> query = prova
                .select(Constants.DATE);

        query.show();

        spark.close();
    }
}
