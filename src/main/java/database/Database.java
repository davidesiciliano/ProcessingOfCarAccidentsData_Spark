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

public abstract class Database {
    final SparkSession spark;

    StructType mySchema;
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
    Dataset<Row> dataset;
    /**
     * Contains the following columns:
     * <ul>
     *     <li>YEAR</li>
     *     <li>WEEK</li>
     *     <li>SUM(NUMBER LETHAL ACCIDENTS)</li>
     * </ul>
     */
    Dataset<Row> query1;
    /**
     * Contains the following columns:
     * <ul>
     *     <li>CONTRIBUTING FACTOR</li>
     *     <li>NUMBER ACCIDENTS</li>
     *     <li>PERCENTAGE NUMBER DEATHS</li>
     * </ul>
     */
    Dataset<Row> query2;
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
    Dataset<Row> query3;

    public Database(SparkSession spark) {
        this.spark = spark;
        this.mySchema = null;
        this.dataset = null;
        this.query1 = null;
        this.query2 = null;
        this.query3 = null;
    }

    void constructSchema() {
        if (mySchema != null)
            throw new UnexpectedBehaviourException("Schema should not be already created");
        // DATE, TIME, BOROUGH, ZIP CODE, LATITUDE, LONGITUDE, LOCATION, ON STREET NAME,
        // CROSS STREET NAME, OFF STREET NAME, NUMBER OF PERSONS INJURED, NUMBER OF PERSONS KILLED,
        // NUMBER OF PEDESTRIANS INJURED, NUMBER OF PEDESTRIANS KILLED, NUMBER OF CYCLIST INJURED,
        // NUMBER OF CYCLIST KILLED, NUMBER OF MOTORIST INJURED, NUMBER OF MOTORIST KILLED,
        // CONTRIBUTING FACTOR VEHICLE 1, CONTRIBUTING FACTOR VEHICLE 2, CONTRIBUTING FACTOR VEHICLE 3,
        // CONTRIBUTING FACTOR VEHICLE 4, CONTRIBUTING FACTOR VEHICLE 5, UNIQUE KEY, VEHICLE TYPE CODE 1,
        // VEHICLE TYPE CODE 2, VEHICLE TYPE CODE 3, VEHICLE TYPE CODE 4,VEHICLE TYPE CODE 5
        List<StructField> fields = new ArrayList<>(); //column name, data type to read, if the value can be null
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

    public abstract void loadDataset(String filePath);

    public abstract Dataset<Row> executeQuery1();

    public abstract Dataset<Row> executeQuery2();

    public abstract Dataset<Row> executeQuery3();
}
