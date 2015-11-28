package de.texttech.csv.sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

/**
 * Created by gx on 11/15/15.
 */
public class Merge {

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setAppName("Count unique DF").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame csv = sqlContext.read().format("com.databricks.spark.csv")
                .option("header", "false")
                .option("inferSchema", "true") // Automatically infer data types
                .load("data/d1.csv");

        DataFrame csv2 = sqlContext.read().format("com.databricks.spark.csv")
                .option("header", "false")
                .option("inferSchema", "true") // Automatically infer data types
                .load("data/d2.csv");

        //DataFrame mergedDF = csv.unionAll(csv2);
        csv.groupBy("C0").avg("C1")
                .show();
    }


}
