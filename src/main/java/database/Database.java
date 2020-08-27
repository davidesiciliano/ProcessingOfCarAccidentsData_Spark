package database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Database {
    void loadDataset(String filePath);

    Dataset<Row> executeQuery1();

    Dataset<Row> executeQuery2();

    Dataset<Row> executeQuery3();
}
