package database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Database {
    private static Database databaseInstance;
    private final SparkSession spark;
    private StructType mySchema;
    private Dataset<Row> dataset;

    private Dataset<Row> query1;
    private Dataset<Row> query2;
    private Dataset<Row> query3;

    private Database(SparkSession spark) {
        this.spark = spark;
        this.mySchema = null;
        this.dataset = null;
        this.query1 = null;
        this.query2 = null;
        this.query3 = null;
    }

    public static void initializeDatabase(SparkSession spark) {
        if (databaseInstance == null)
            databaseInstance = new Database(spark);
        else
            System.err.println("ERRORE"); //TODO: add exception
    }

    public static Database getDB() {
        if (databaseInstance == null)
            System.err.println("ERRORE"); //TODO: add exception
        return databaseInstance;
    }

    private void constructSchema() {
        if (mySchema != null)
            System.err.println("ERRORE"); //TODO: add exception
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
        fields.add(DataTypes.createStructField(Constants.LATITUDE, DataTypes.FloatType, true));
        fields.add(DataTypes.createStructField(Constants.LONGITUDE, DataTypes.FloatType, true));
        fields.add(DataTypes.createStructField(Constants.LOCATION, DataTypes.StringType, true));
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
        fields.add(DataTypes.createStructField(Constants.CONTRIBUTING_FACTOR_VEHICLE_3, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CONTRIBUTING_FACTOR_VEHICLE_4, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.CONTRIBUTING_FACTOR_VEHICLE_5, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.UNIQUE_KEY, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_1, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_2, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_3, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_4, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(Constants.VEHICLE_TYPE_CODE_5, DataTypes.StringType, true));
        this.mySchema = DataTypes.createStructType(fields);
    }

    public void loadDataset(String filePath) {
        if (this.dataset != null)
            System.err.println("Dataset already loaded");
        this.constructSchema();
        final Dataset<Row> completeDataset = spark
                .read()
                .option("header", "true")
                .option("dateFormat", "MM/dd/yyyy")
                .option("timestampFormat", "hh:mm")
                .option("delimiter", ",")
                //.option("inferSchema", "true")
                .schema(this.mySchema)
                .csv(filePath + "data/NYPD_Motor_Vehicle_Collisions.csv");
                //.csv(filePath + "data/DatasetProva.csv");
        this.dataset = completeDataset //clean dataset without Null values and useless columns
                .where(col(Constants.BOROUGH).isNotNull()
                        .and(col(Constants.DATE).isNotNull()))
                .select(col(Constants.DATE),
                        col(Constants.BOROUGH),
                        col(Constants.NUMBER_OF_PERSONS_INJURED),
                        col(Constants.NUMBER_OF_PERSONS_KILLED),
                        col(Constants.NUMBER_OF_PEDESTRIANS_INJURED),
                        col(Constants.NUMBER_OF_PEDESTRIANS_KILLED),
                        col(Constants.NUMBER_OF_CYCLIST_INJURED),
                        col(Constants.NUMBER_OF_CYCLIST_KILLED),
                        col(Constants.NUMBER_OF_MOTORIST_INJURED),
                        col(Constants.NUMBER_OF_MOTORIST_KILLED),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_1),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_2),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_3),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_4),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_5),
                        col(Constants.UNIQUE_KEY))
                .cache();
    }

    public Dataset<Row> executeQuery1() {
        //QUERY 1: Number of lethal accidents per week throughout the entire dataset
        if (this.query3 == null)
            this.executeQuery3();
        if (this.query1 != null)
            return this.query1;

        this.query1 = this.query3
                .groupBy(Constants.YEAR, Constants.WEEK)
                .agg(sum(col(Constants.NUMBER_LETHAL_ACCIDENTS)).as(Constants.SUM_NUMBER_LETHAL_ACCIDENTS))
                .select(Constants.YEAR, Constants.WEEK, Constants.SUM_NUMBER_LETHAL_ACCIDENTS)
                .sort(Constants.YEAR, Constants.WEEK);

        return this.query1;
    }

    public Dataset<Row> executeQuery2() {
        //QUERY 2: Number of accidents and percentage of number of deaths per contributing factor in the dataset
        return null;
    }

    public Dataset<Row> executeQuery3() {
        //QUERY 3: Number of accidents and average number of lethal accidents per week per borough
        if (this.query3 != null)
            return this.query3;

        final Dataset<Row> d1 = this.dataset
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
        this.query3 = d1
                .groupBy(Constants.BOROUGH, Constants.YEAR, Constants.WEEK)
                .agg(count("*").as(Constants.NUMBER_ACCIDENTS),
                        count(when(col(Constants.NUMBER_KILLED).$greater(0), true)).as(Constants.NUMBER_LETHAL_ACCIDENTS),
                        sum(col(Constants.NUMBER_INJURED)).as(Constants.SUM_NUMBER_INJURED),
                        sum(col(Constants.NUMBER_KILLED)).as(Constants.SUM_NUMBER_KILLED))
                .withColumn(Constants.AVERAGE_NUMBER_LETHAL_ACCIDENTS, col(Constants.NUMBER_LETHAL_ACCIDENTS).divide(col(Constants.NUMBER_ACCIDENTS)))
                .sort(Constants.YEAR, Constants.WEEK, Constants.BOROUGH)
                .cache();

        return this.query3;
    }
}
