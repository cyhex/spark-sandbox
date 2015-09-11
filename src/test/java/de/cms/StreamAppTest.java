package de.cms;


import de.cms.util.StreamingTestCase;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;


/**
 * https://github.com/apache/spark/blob/master/streaming/src/test/java/org/apache/spark/streaming/JavaReceiverAPISuite.java
 */

public class StreamAppTest extends StreamingTestCase {

    private static final Logger LOG = LogManager.getLogger(StreamApp.class);

    @Test
    public void testApplyWork() throws Exception {
        final AtomicLong c = new AtomicLong(0);
        JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", testServer.getPort());
        JavaDStream<String> mapped = StreamApp.applyWork(lines);
        mapped.foreachRDD(longJavaRDD -> {
            c.addAndGet(longJavaRDD.count());
            return null;
        });

        startServers();

        for (int i = 0; i < 10; i++) {
            testServer.write("hello hello \n");
            Thread.sleep(100);
        }
        Thread.sleep(200);
        Assert.assertEquals(20, c.get());

    }


}