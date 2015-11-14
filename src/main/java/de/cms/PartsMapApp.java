package de.cms;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

public class PartsMapApp {

    private static final Logger log = LogManager.getLogger(PartsMapApp.class);

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Log Counter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        FileUtils.deleteDirectory(new File("/home/gx/Desktop/html.rdd"));

        JavaRDD<String> seedsRdd = sc.textFile("/home/gx/Desktop/seeds.rdd");


        int pCount = (int) (seedsRdd.count() / 5) + 1;

        seedsRdd.repartition(pCount)
                .filter(r -> r != null)
                .saveAsTextFile("/home/gx/Desktop/html.rdd");

    }

}
