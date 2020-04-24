package database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Database {
    private static Database databaseInstance;
    private final SparkSession spark;
    private StructType mySchema;

    private Database(SparkSession spark) {
        this.spark = spark;
        this.mySchema = null;
    }

    public static void initializeDatabase(SparkSession spark) {
        if (databaseInstance == null)
            databaseInstance = new Database(spark);
        else
            System.out.println("ERRORE"); //TODO: add exception
    }

    public static Database getDB() {
        if (databaseInstance == null)
            System.out.println("ERRORE"); //TODO: add exception
        return databaseInstance;
    }

    public void constructSchema() {
        if (mySchema != null)
            System.out.println("ERRORE"); //TODO: add exception
        // DATE, TIME, BOROUGH, ZIP CODE, LATITUDE, LONGITUDE, LOCATION, ON STREET NAME,
        // CROSS STREET NAME, OFF STREET NAME, NUMBER OF PERSONS INJURED, NUMBER OF PERSONS KILLED,
        // NUMBER OF PEDESTRIANS INJURED, NUMBER OF PEDESTRIANS KILLED, NUMBER OF CYCLIST INJURED,
        // NUMBER OF CYCLIST KILLED, NUMBER OF MOTORIST INJURED, NUMBER OF MOTORIST KILLED,
        // CONTRIBUTING FACTOR VEHICLE 1, CONTRIBUTING FACTOR VEHICLE 2, CONTRIBUTING FACTOR VEHICLE 3,
        // CONTRIBUTING FACTOR VEHICLE 4, CONTRIBUTING FACTOR VEHICLE 5, UNIQUE KEY, VEHICLE TYPE CODE 1,
        // VEHICLE TYPE CODE 2, VEHICLE TYPE CODE 3, VEHICLE TYPE CODE 4,VEHICLE TYPE CODE 5
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(Constants.DATE, DataTypes.DateType, false));
        fields.add(DataTypes.createStructField(Constants.TIME, DataTypes.TimestampType, false));
        fields.add(DataTypes.createStructField(Constants.BOROUGH, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.ZIPCODE, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.LATITUDE, DataTypes.FloatType, true)); //magari double se da problemi
        fields.add(DataTypes.createStructField(Constants.LONGITUDE, DataTypes.FloatType, true)); //magari double se da problemi
        fields.add(DataTypes.createStructField(Constants.LOCATION, DataTypes.StringType, true)); //sarebbe un vettore di 2 componenti
        fields.add(DataTypes.createStructField(Constants.ON_STREET_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CROSS_STREET_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.OFF_STREET_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.NUMBER_OF_PERSONS_INJURED, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.NUMBER_OF_PERSONS_KILLED, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.NUMBER_OF_PEDESTRIANS_INJURED, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.NUMBER_OF_PEDESTRIANS_KILLED, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.NUMBER_OF_CYCLIST_INJURED, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.NUMBER_OF_CYCLIST_KILLED, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.NUMBER_OF_MOTORIST_INJURED, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.NUMBER_OF_MOTORIST_KILLED, DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField(Constants.CONTRIBUTING_FACTOR_VEHICLE_1, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CONTRIBUTING_FACTOR_VEHICLE_2, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CONTRIBUTING_FACTOR_VEHICLE_2, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CONTRIBUTING_FACTOR_VEHICLE_3, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CONTRIBUTING_FACTOR_VEHICLE_4, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CONTRIBUTING_FACTOR_VEHICLE_5, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.UNIQUE_KEY, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_1, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_2, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_3, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_4, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_5, DataTypes.StringType, true));
        this.mySchema = DataTypes.createStructType(fields);
    }

    public Dataset<Row> loadData(String filePath) {
        return spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(this.mySchema)
                //.csv(filePath + "src/main/resources/prova.csv");
                .csv(filePath + "src/main/resources/NYPD_Motor_Vehicle_Collisions.csv");
    }

}
