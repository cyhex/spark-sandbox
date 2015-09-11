package de.cms.util;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestServer extends Thread {
    static Logger LOG = Logger.getLogger(TestServer.class);

    private final ServerSocket serverSocket;
    private BlockingQueue<String> writeQueue = new ArrayBlockingQueue<>(100);
    private boolean run = true;

    public TestServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public TestServer() throws IOException {
        serverSocket = new ServerSocket(0);
    }

    @Override
    public void run() {

        while (run) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                while (clientSocket.isConnected()) {
                    String v = writeQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (v != null) {
                        out.write(v);
                        out.flush();
                        LOG.info("sent: " + v);
                    }
                }
            } catch (IOException | InterruptedException e) {
                LOG.warn(e.getMessage(), e);
            }
        }

    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void stopServer() throws InterruptedException {
        run = false;
        interrupt();
    }

    public void write(String str) throws InterruptedException {
        writeQueue.put(str);
    }
}
