package de.texttech.csv;

import de.texttech.csv.utils.SparkTask;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class CountUnique extends SparkTask {
    private static final Logger log = LogManager.getLogger(CountUnique.class);
    private final SQLContext sqlContext;

    protected CountUnique(JavaSparkContext sc) {
        super(sc);
        sqlContext = new SQLContext(sc);
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Count unique DF")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        CountUnique app = new CountUnique(sc);
        app.run();
    }

    @Override
    public void run() {
        DataFrame csv = sqlContext.read().format("com.databricks.spark.csv")
                .option("header", "false")
                .option("inferSchema", "true") // Automatically infer data types
                .load("data/d1.csv");

        long count = csv.select("C0").distinct().count();
        log.info("Unique count: " + count);

    }
}
