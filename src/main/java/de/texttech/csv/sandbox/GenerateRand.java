package de.texttech.csv.sandbox;

import de.texttech.csv.utils.HdfsHelpers;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateRand {
    private static final Logger log = LogManager.getLogger(GenerateRand.class);

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Count unique DF").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Map<Integer, Double>> list = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            Map<Integer, Double> m = new HashMap<>();
            m.put(i, Math.sin(i));
            list.add(m);
        }

        sc.parallelize(list);
        HdfsHelpers.copyMerge("data/rand.rdd", "data/rand.txt");


    }

}
