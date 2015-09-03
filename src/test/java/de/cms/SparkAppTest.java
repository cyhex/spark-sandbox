package de.cms;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkAppTest extends TestCase {

    public void testApplyWork() throws Exception {

        SparkConf conf = new SparkConf().setAppName("Log Counter").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello Unit test", "hello Unit test2"));
        long work = SparkApp.applyWork(rdd);
        Assert.assertEquals(0, work);

        rdd = sc.parallelize(Arrays.asList(" sdf sdf sdf sdf \"GET / HTTP/1.1\" 200\" asfsdf sdf sdf sdf ", "hello Unit test2"));
        work = SparkApp.applyWork(rdd);
        Assert.assertEquals(1, work);

    }
}