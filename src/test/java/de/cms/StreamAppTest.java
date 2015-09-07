package de.cms;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;


/**
 * https://github.com/apache/spark/blob/master/streaming/src/test/java/org/apache/spark/streaming/JavaReceiverAPISuite.java
 */

public class StreamAppTest extends TestCase {

    public void testApplyWork() throws Exception {
        final AtomicLong c = new AtomicLong(0);
        SparkConf conf = new SparkConf().setAppName("Log Counter").setMaster("local[2]");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

        //org.apache.spark.streaming.TestServer

        TestServer server = new TestServer(9999);
        server.start();

        JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 9999);
        JavaDStream<Long> mapped = StreamApp.applyWork(lines);
        mapped.foreachRDD(longJavaRDD -> {
            c.addAndGet(longJavaRDD.count());
            return null;
        });
        sc.start();
        Thread.sleep(500);

        server.getOut().println("hello");
        server.getOut().println("socket");
        server.setRun(false);
        Assert.assertEquals(2, c.get());

    }


    class TestServer extends Thread {
        private int port;
        private PrintWriter out;

        private boolean run = true;

        public TestServer(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            ServerSocket serverSocket;
            try {
                serverSocket = new ServerSocket(port);
                Socket clientSocket = serverSocket.accept();
                out = new PrintWriter(clientSocket.getOutputStream(), true);
                while (run){
                    Thread.sleep(1);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        public int getPort() {
            return port;
        }

        public PrintWriter getOut() {
            return out;
        }

        public void setRun(boolean run) {
            this.run = run;
        }
    }

}