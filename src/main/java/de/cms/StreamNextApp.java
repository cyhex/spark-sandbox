package de.cms;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;


public class StreamNextApp {

    private static final Logger log = LogManager.getLogger(StreamNextApp.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Network World Next Counter");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 9998);
        JavaDStream<String> stringJavaDStream = lines.flatMap(o -> Arrays.asList(o.split(" ")));
        stringJavaDStream
                .mapToPair(s -> new Tuple2<>("xxx:" + s , 1))
                .reduceByKey((i, i2) -> i + i2).print();
        sc.start();
        sc.awaitTermination();




    }
}
