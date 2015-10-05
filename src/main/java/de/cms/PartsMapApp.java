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

        FileUtils.deleteDirectory(new File("/media/gx/Storage/crawl/html.rdd"));

        JavaRDD<String> seedsRdd = sc.textFile("/media/gx/Storage/crawl/seeds.rdd");
        Fetcher fetcher = new Fetcher();
        seedsRdd.repartition((int) (seedsRdd.count() / 5))
                .map(fetcher::fetch)
                .filter(s -> s != null)
                .saveAsTextFile("/media/gx/Storage/crawl/html.rdd");

    }

}
