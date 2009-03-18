/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.server.socket;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.protocol.ServerWireFormat;

/**
 * A simple socket-based server for serving voldemort requests
 * 
 * @author jay
 * 
 */
public class SocketServer extends Thread {

    static final Logger logger = Logger.getLogger(SocketServer.class.getName());

    private final ExecutorService threadPool;
    private final Random random = new Random();
    private final int port;
    private final ThreadGroup threadGroup;
    private final CountDownLatch isStarted = new CountDownLatch(1);
    private final int socketBufferSize;
    private final ServerWireFormat wireFormat;
    private ServerSocket serverSocket = null;

    public SocketServer(int port,
                        int defaultThreads,
                        int maxThreads,
                        int socketBufferSize,
                        ServerWireFormat wireFormat) {
        this.port = port;
        this.socketBufferSize = socketBufferSize;
        this.threadGroup = new ThreadGroup("voldemort-socket-server");
        this.wireFormat = wireFormat;
        this.threadPool = new ThreadPoolExecutor(defaultThreads,
                                                 maxThreads,
                                                 1,
                                                 TimeUnit.SECONDS,
                                                 new SynchronousQueue<Runnable>(),
                                                 threadFactory,
                                                 rejectedExecutionHandler);
    }

    private final ThreadFactory threadFactory = new ThreadFactory() {

        public Thread newThread(Runnable r) {
            String name = getThreadName("handler");
            Thread t = new Thread(threadGroup, r, name);
            t.setDaemon(true);
            return t;
        }
    };

    private final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {

        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            SocketServerSession session = (SocketServerSession) r;
            logger.error("Too many open connections, " + executor.getActiveCount() + " of "
                         + executor.getLargestPoolSize()
                         + " threads in use, denying connection from "
                         + session.getSocket().getRemoteSocketAddress());
            try {
                session.getSocket().close();
            } catch(IOException e) {
                logger.error("Could not close socket.", e);
            }
        }
    };

    @Override
    public void run() {
        logger.info("Starting voldemort socket server on port " + port + ".");
        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(port));
            serverSocket.setReceiveBufferSize(this.socketBufferSize);
            isStarted.countDown();
            while(!isInterrupted() && !serverSocket.isClosed()) {
                final Socket socket = serverSocket.accept();
                configureSocket(socket);
                this.threadPool.execute(new SocketServerSession(socket, wireFormat));
            }
        } catch(BindException e) {
            logger.error("Could not bind to port " + port + ".");
            throw new VoldemortException(e);
        } catch(SocketException e) {
            // If we have been manually shutdown, ignore
            if(!isInterrupted())
                logger.error("Error in server: ", e);
        } catch(IOException e) {
            throw new VoldemortException(e);
        } finally {
            if(serverSocket != null) {
                try {
                    serverSocket.close();
                } catch(IOException e) {
                    logger.warn("Error while shutting down server.", e);
                }
            }

        }
    }

    private void configureSocket(Socket socket) throws SocketException {
        socket.setTcpNoDelay(true);
        socket.setSendBufferSize(this.socketBufferSize);
        if(socket.getReceiveBufferSize() != this.socketBufferSize)
            logger.debug("Requested socket receive buffer size was " + this.socketBufferSize
                         + " bytes but actual size is " + socket.getReceiveBufferSize() + " bytes.");
        if(socket.getSendBufferSize() != this.socketBufferSize)
            logger.debug("Requested socket send buffer size was " + this.socketBufferSize
                         + " bytes but actual size is " + socket.getSendBufferSize() + " bytes.");
    }

    public void shutdown() {
        logger.info("Shutting down voldemort socket server on port " + port + ".");
        threadGroup.interrupt();
        interrupt();
        threadPool.shutdownNow();
        try {
            threadPool.awaitTermination(1, TimeUnit.SECONDS);
        } catch(InterruptedException e) {
            logger.warn("Interrupted while waiting for tasks to complete: ", e);
        }
        try {
            if(!serverSocket.isClosed())
                serverSocket.close();
        } catch(IOException e) {
            logger.warn("Exception while closing server socket: ", e);
        }
    }

    public int getPort() {
        return this.port;
    }

    public void awaitStartupCompletion() {
        try {
            isStarted.await();
        } catch(InterruptedException e) {
            // this is okay, if we are interrupted we can stop waiting
        }
    }

    private String getThreadName(String baseName) {
        return baseName + random.nextInt(1000000);
    }

}
