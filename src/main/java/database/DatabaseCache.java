package database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.UnexpectedBehaviourException;

import static org.apache.spark.sql.functions.*;

public class DatabaseCache extends Database {

    public DatabaseCache(SparkSession spark) {
        super(spark);
    }

    @Override
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
                .withColumn(Constants.NUMBER_INJURED, col(Constants.NUMBER_OF_PERSONS_INJURED))
                .withColumn(Constants.NUMBER_KILLED, col(Constants.NUMBER_OF_PERSONS_KILLED))
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
                //.sort(Constants.YEAR, Constants.WEEK)
                .cache();

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
                        count(when(col(Constants.NUMBER_KILLED).$greater(0), true)).as(Constants.NUMBER_LETHAL_ACCIDENTS),
                        sum(col(Constants.NUMBER_INJURED)).as(Constants.SUM_NUMBER_INJURED),
                        sum(col(Constants.NUMBER_KILLED)).as(Constants.SUM_NUMBER_KILLED))
                .withColumn(Constants.PERCENTAGE_NUMBER_DEATHS, (col(Constants.SUM_NUMBER_KILLED)
                        .divide(when((col(Constants.SUM_NUMBER_INJURED).plus(col(Constants.SUM_NUMBER_KILLED))).notEqual(0),
                                col(Constants.SUM_NUMBER_INJURED).plus(col(Constants.SUM_NUMBER_KILLED)))
                                .otherwise(1))).multiply(100))
                .withColumn(Constants.PERCENTAGE_NUMBER_LETHAL_ACCIDENTS, (col(Constants.NUMBER_LETHAL_ACCIDENTS)
                        .divide(when((col(Constants.NUMBER_ACCIDENTS).plus(col(Constants.NUMBER_LETHAL_ACCIDENTS))).notEqual(0),
                                col(Constants.NUMBER_ACCIDENTS).plus(col(Constants.NUMBER_LETHAL_ACCIDENTS)))
                                .otherwise(1))).multiply(100))
                .select(col(Constants.CONTRIBUTING_FACTOR),
                        col(Constants.NUMBER_ACCIDENTS),
                        col(Constants.NUMBER_LETHAL_ACCIDENTS),
                        col(Constants.PERCENTAGE_NUMBER_DEATHS),
                        col(Constants.PERCENTAGE_NUMBER_LETHAL_ACCIDENTS))
                //.sort(col(Constants.CONTRIBUTING_FACTOR))
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
                //.sort(Constants.YEAR, Constants.WEEK, Constants.BOROUGH)
                .cache();

        return this.query3;
    }
}
