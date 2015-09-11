package de.cms;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;


public class StreamApp {

    private static final Logger log = LogManager.getLogger(StreamApp.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Network World Counter");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 9999);
        applyWork(lines);

        sc.start();
        sc.awaitTermination();

    }

    public static JavaDStream<String> applyWork(JavaReceiverInputDStream<String> lines) {
        JavaDStream<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")));
        return words;
    }
}
