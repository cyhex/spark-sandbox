package de.cms;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkApp {

    private static final Logger log = LogManager.getLogger(SparkApp.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Log Counter");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile("/home/gx/Documents/projects/spark-test/data/access.log");
        long count = applyWork(logData);
        log.info("Total responces: " + logData.count());
        log.info("200 responces: " + count);
    }

    public static long applyWork(JavaRDD<String> logData){
        return logData.filter(s -> s.contains("\"GET / HTTP/1.1\" 200")).count();
    }
}
