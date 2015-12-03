package de.texttech.csv.utils;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by gx on 11/28/15.
 */
public abstract class SparkTask implements Runnable {

    protected final JavaSparkContext sc;

    public SparkTask(JavaSparkContext sc) {
        this.sc = sc;
    }

    public JavaSparkContext getSc() {
        return sc;
    }

}
