package de.texttech.csv.sandbox;

import de.texttech.csv.utils.HdfsHelpers;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by gx on 11/15/15.
 */
public class MergeRdd {

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setAppName("Count unique DF").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> d1Rdd = sc.textFile("data/d1.csv")
                .mapToPair(s -> new Tuple2<>(s.split(",")[0], s.split(",")[1]));

        JavaPairRDD<String, String> d2Rdd = sc.textFile("data/d2.csv")
                .mapToPair(s -> new Tuple2<>(s.split(",")[0], s.split(",")[1]));

        d1Rdd.union(d2Rdd).sortByKey(false).map(t -> t._1() + "," + t._2()).saveAsTextFile("data/merged.rdd");
        HdfsHelpers.copyMerge("data/merged.rdd", "data/merged.csv");


    }


}
