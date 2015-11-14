package de.texttech.csv;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Function1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DataframesTest {
    private static final Logger log = LogManager.getLogger(DataframesTest.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Count unique DF").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> javaRDD = sc.textFile("data/d1.csv")
                .map(s -> s.split(",")[1])
                .map(Integer::parseInt);

        float avg = (float) javaRDD.reduce((i1, i2) -> i1 + i2) / javaRDD.count();
        log.info("avg: " +  avg);

    }

    private static class Payload implements Serializable {
        String k;
        Integer v;

        public Payload(String k, Integer v) {
            this.k = k;
            this.v = v;
        }
    }

}
