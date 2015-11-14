package de.texttech.csv;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.net.URISyntaxException;

public class CountGroupedFeatures {
    private static final Logger log = LogManager.getLogger(CountGroupedFeatures.class);

    public static void main(String[] args) throws URISyntaxException, IOException {

        SparkConf conf = new SparkConf().setAppName("Count unique DF").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame csv = sqlContext.read().format("com.databricks.spark.csv")
                .option("header", "false")
                .option("inferSchema", "true") // Automatically infer data types
                .load("data/d1.csv");

        csv.groupBy("C1").count()
                .coalesce(1).write()
                .format("com.databricks.spark.csv")
                .save("data/d2.hdfs");
        HdfsHelpers.copyMerge("data/d2.hdfs", "data/d2.csv");



    }

}
