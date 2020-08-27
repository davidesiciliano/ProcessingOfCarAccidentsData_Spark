package database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import utils.UnexpectedBehaviourException;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DatabaseCache implements Database {
    private final SparkSession spark;
    private StructType mySchema;
    /**
     * Contains the following columns:
     * <ul>
     *     <li>DATE</li>
     *     <li>BOROUGH</li>
     *     <li>NUMBER INJURED</li>
     *     <li>NUMBER KILLED</li>
     *     <li>CONTRIBUTING FACTOR VEHICLE 1</li>
     *     <li>CONTRIBUTING FACTOR VEHICLE 2</li>
     *     <li>CONTRIBUTING FACTOR VEHICLE 3</li>
     *     <li>CONTRIBUTING FACTOR VEHICLE 4</li>
     *     <li>CONTRIBUTING FACTOR VEHICLE 5</li>
     *     <li>UNIQUE_KEY</li>
     * </ul>
     */
    private Dataset<Row> dataset;
    /**
     * Contains the following columns:
     * <ul>
     *     <li>YEAR</li>
     *     <li>WEEK</li>
     *     <li>SUM(NUMBER LETHAL ACCIDENTS)</li>
     * </ul>
     */
    private Dataset<Row> query1;
    /**
     * Contains the following columns:
     * <ul>
     *     <li>CONTRIBUTING FACTOR</li>
     *     <li>NUMBER ACCIDENTS</li>
     *     <li>PERCENTAGE NUMBER DEATHS</li>
     * </ul>
     */
    private Dataset<Row> query2;
    /**
     * Contains the following columns:
     * <ul>
     *     <li>BOROUGH</li>
     *     <li>YEAR</li>
     *     <li>WEEK</li>
     *     <li>NUMBER ACCIDENTS</li>
     *     <li>NUMBER LETHAL ACCIDENTS</li>
     *     <li>SUM(NUMBER INJURED)</li>
     *     <li>SUM(NUMBER KILLED)</li>
     *     <li>AVG(NUMBER LETHAL ACCIDENTS)</li>
     * </ul>
     */
    private Dataset<Row> query3;

    public DatabaseCache(SparkSession spark) {
        this.spark = spark;
        this.mySchema = null;
        this.dataset = null;
        this.query1 = null;
        this.query2 = null;
        this.query3 = null;
    }

    private void constructSchema() {
        if (mySchema != null)
            throw new UnexpectedBehaviourException("Schema should not be already created");
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
            throw new UnexpectedBehaviourException("Dataset already loaded");
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
                //.csv(filePath + "data/NYPD_Motor_Vehicle_Collisions(double).csv");
                //.csv("wasbs:///NYPD_Motor_Vehicle_Collisions.csv"); //Azure
                //.csv("wasbs:///NYPD_Motor_Vehicle_Collisions(double).csv"); //Azure

        this.dataset = completeDataset //clean dataset without Null values and useless columns
                .where(col(Constants.DATE).isNotNull())
                .withColumn(Constants.BOROUGH, when(col(Constants.BOROUGH).isNull(), Constants.BOROUGH_NOT_SPECIFIED)
                        .otherwise(col(Constants.BOROUGH)))
                .withColumn(Constants.NUMBER_INJURED, col(Constants.NUMBER_OF_PERSONS_INJURED)
                        .plus(col(Constants.NUMBER_OF_PEDESTRIANS_INJURED))
                        .plus(col(Constants.NUMBER_OF_CYCLIST_INJURED))
                        .plus(col(Constants.NUMBER_OF_MOTORIST_INJURED)))
                .withColumn(Constants.NUMBER_KILLED, col(Constants.NUMBER_OF_PERSONS_KILLED)
                        .plus(col(Constants.NUMBER_OF_PEDESTRIANS_KILLED))
                        .plus(col(Constants.NUMBER_OF_CYCLIST_KILLED))
                        .plus(col(Constants.NUMBER_OF_MOTORIST_KILLED)))
                .drop(Constants.TIME,
                        Constants.ZIPCODE,
                        Constants.LATITUDE,
                        Constants.LONGITUDE,
                        Constants.ON_STREET_NAME,
                        Constants.CROSS_STREET_NAME,
                        Constants.OFF_STREET_NAME,
                        Constants.VEHICLE_TYPE_CODE_1,
                        Constants.VEHICLE_TYPE_CODE_2,
                        Constants.VEHICLE_TYPE_CODE_3,
                        Constants.VEHICLE_TYPE_CODE_4)
                .cache();
        this.dataset.count();
    }

    @Override
    public Dataset<Row> executeQuery1() {
        //QUERY 1: Number of lethal accidents per week throughout the entire dataset
        if (this.query3 == null) {
            this.executeQuery3();
        }
        if (this.query1 != null)
            return this.query1;

        this.query1 = this.query3
                .groupBy(Constants.YEAR, Constants.WEEK)
                .agg(sum(col(Constants.NUMBER_LETHAL_ACCIDENTS)).as(Constants.SUM_NUMBER_LETHAL_ACCIDENTS))
                .drop(Constants.BOROUGH,
                        Constants.NUMBER_ACCIDENTS,
                        Constants.NUMBER_LETHAL_ACCIDENTS,
                        Constants.SUM_NUMBER_INJURED,
                        Constants.SUM_NUMBER_KILLED,
                        Constants.AVERAGE_NUMBER_LETHAL_ACCIDENTS)
                .cache();
                //.sort(Constants.YEAR, Constants.WEEK);

        return this.query1;
    }

    @Override
    public Dataset<Row> executeQuery2() {
        //QUERY 2: Number of accidents and percentage of number of deaths per contributing factor in the dataset
        if (this.query2 != null)
            return this.query2;

        final Dataset<Row> initQuery2 = this.dataset
                .drop(Constants.DATE,
                        Constants.BOROUGH)
                .cache();

        final Dataset<Row> contributingFactor1 = initQuery2
                .drop(Constants.CONTRIBUTING_FACTOR_VEHICLE_2,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_3,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_4,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_5)
                .where(col(Constants.CONTRIBUTING_FACTOR_VEHICLE_1).isNotNull())
                .select(col(Constants.UNIQUE_KEY),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_1).as(Constants.CONTRIBUTING_FACTOR),
                        col(Constants.NUMBER_INJURED),
                        col(Constants.NUMBER_KILLED))
                .cache();
        final Dataset<Row> contributingFactor2 = initQuery2
                .drop(Constants.CONTRIBUTING_FACTOR_VEHICLE_1,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_3,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_4,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_5)
                .where(col(Constants.CONTRIBUTING_FACTOR_VEHICLE_2).isNotNull())
                .select(col(Constants.UNIQUE_KEY),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_2).as(Constants.CONTRIBUTING_FACTOR),
                        col(Constants.NUMBER_INJURED),
                        col(Constants.NUMBER_KILLED))
                .cache();
        final Dataset<Row> contributingFactor3 = initQuery2
                .drop(Constants.CONTRIBUTING_FACTOR_VEHICLE_1,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_2,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_4,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_5)
                .where(col(Constants.CONTRIBUTING_FACTOR_VEHICLE_3).isNotNull())
                .select(col(Constants.UNIQUE_KEY),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_3).as(Constants.CONTRIBUTING_FACTOR),
                        col(Constants.NUMBER_INJURED),
                        col(Constants.NUMBER_KILLED))
                .cache();
        final Dataset<Row> contributingFactor4 = initQuery2
                .drop(Constants.CONTRIBUTING_FACTOR_VEHICLE_1,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_2,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_3,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_5)
                .where(col(Constants.CONTRIBUTING_FACTOR_VEHICLE_4).isNotNull())
                .select(col(Constants.UNIQUE_KEY),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_4).as(Constants.CONTRIBUTING_FACTOR),
                        col(Constants.NUMBER_INJURED),
                        col(Constants.NUMBER_KILLED))
                .cache();
        final Dataset<Row> contributingFactor5 = initQuery2
                .drop(Constants.CONTRIBUTING_FACTOR_VEHICLE_1,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_2,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_3,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_4)
                .where(col(Constants.CONTRIBUTING_FACTOR_VEHICLE_5).isNotNull())
                .select(col(Constants.UNIQUE_KEY),
                        col(Constants.CONTRIBUTING_FACTOR_VEHICLE_5).as(Constants.CONTRIBUTING_FACTOR),
                        col(Constants.NUMBER_INJURED),
                        col(Constants.NUMBER_KILLED))
                .cache();
        final Dataset<Row> unionTable = contributingFactor1
                .union(contributingFactor2)
                .union(contributingFactor3)
                .union(contributingFactor4)
                .union(contributingFactor5)
                .dropDuplicates(Constants.UNIQUE_KEY, Constants.CONTRIBUTING_FACTOR)
                .drop(Constants.UNIQUE_KEY)
                .cache();
        this.query2 = unionTable
                .groupBy(Constants.CONTRIBUTING_FACTOR)
                .agg(count("*").as(Constants.NUMBER_ACCIDENTS),
                        sum(col(Constants.NUMBER_INJURED)).as(Constants.SUM_NUMBER_INJURED),
                        sum(col(Constants.NUMBER_KILLED)).as(Constants.SUM_NUMBER_KILLED))
                .withColumn(Constants.PERCENTAGE_NUMBER_DEATHS, (col(Constants.SUM_NUMBER_KILLED)
                        .divide(when((col(Constants.SUM_NUMBER_INJURED).plus(col(Constants.SUM_NUMBER_KILLED))).notEqual(0),
                                col(Constants.SUM_NUMBER_INJURED).plus(col(Constants.SUM_NUMBER_KILLED)))
                                .otherwise(1))).multiply(100))
                .select(col(Constants.CONTRIBUTING_FACTOR),
                        col(Constants.NUMBER_ACCIDENTS),
                        col(Constants.PERCENTAGE_NUMBER_DEATHS))
                .cache();

        return this.query2;
    }

    @Override
    public Dataset<Row> executeQuery3() {
        //QUERY 3: Number of accidents and average number of lethal accidents per week per borough
        if (this.query3 != null)
            return this.query3;

        final Dataset<Row> d1 = this.dataset
                .withColumn(Constants.WEEK, weekofyear(col(Constants.DATE)))
                .withColumn(Constants.YEAR, date_format(col(Constants.DATE), "yyyy"))
                .drop(Constants.CONTRIBUTING_FACTOR_VEHICLE_1,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_2,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_3,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_4,
                        Constants.CONTRIBUTING_FACTOR_VEHICLE_5,
                        Constants.UNIQUE_KEY)
                .cache();

        final Dataset<Row> d2 = d1
                .groupBy(Constants.BOROUGH, Constants.YEAR, Constants.WEEK)
                .agg(count("*").as(Constants.NUMBER_ACCIDENTS),
                        count(when(col(Constants.NUMBER_KILLED).$greater(0), true)).as(Constants.NUMBER_LETHAL_ACCIDENTS),
                        sum(col(Constants.NUMBER_INJURED)).as(Constants.SUM_NUMBER_INJURED),
                        sum(col(Constants.NUMBER_KILLED)).as(Constants.SUM_NUMBER_KILLED))
                .cache();

        this.query3 = d2
                .withColumn(Constants.AVERAGE_NUMBER_LETHAL_ACCIDENTS, col(Constants.NUMBER_LETHAL_ACCIDENTS).divide(col(Constants.NUMBER_ACCIDENTS)))
                .sort(Constants.YEAR, Constants.WEEK, Constants.BOROUGH)
                .cache();

        return this.query3;
    }
}
