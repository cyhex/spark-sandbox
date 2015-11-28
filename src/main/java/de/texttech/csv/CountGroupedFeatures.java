package de.texttech.csv;

import de.texttech.csv.utils.HdfsHelpers;
import de.texttech.csv.utils.SparkTask;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.net.URISyntaxException;

public class CountGroupedFeatures extends SparkTask {
    private static final Logger log = LogManager.getLogger(CountGroupedFeatures.class);
    private final SQLContext sqlContext;

    protected CountGroupedFeatures(JavaSparkContext sc) {
        super(sc);
        sqlContext = new SQLContext(sc);
    }

    public static void main(String[] args) throws URISyntaxException, IOException {

        SparkConf conf = new SparkConf()
                .setAppName("Count unique DF")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        CountGroupedFeatures app = new CountGroupedFeatures(sc);
        app.run();

    }

    @Override
    public void run() {
        DataFrame csv = sqlContext.read().format("com.databricks.spark.csv")
                .option("header", "false")
                .option("inferSchema", "true") // Automatically infer data types
                .load("data/d1.csv");

        csv.groupBy("C1").count()
                .coalesce(1).write()
                .format("com.databricks.spark.csv")
                .save("data/d2.hdfs");
        try {
            HdfsHelpers.copyMerge("data/d2.hdfs", "data/d2.csv");
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

    }
}
