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

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.vold.VoldemortNativeRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;

/**
 * Tests for the socket pooling
 * 
 * @author jay
 * 
 */
public class SocketPoolTest extends TestCase {

    private int port1;
    private int port2;
    private int maxConnectionsPerNode = 3;
    private int maxTotalConnections = 2 * maxConnectionsPerNode + 1;
    private SocketPool pool;
    private SocketDestination dest1;
    private SocketDestination dest2;
    private SocketServer server1;
    private SocketServer server2;

    @Override
    public void setUp() {
        int[] ports = ServerTestUtils.findFreePorts(2);
        this.port1 = ports[0];
        this.port2 = ports[1];
        this.pool = new SocketPool(maxConnectionsPerNode,
                                   maxTotalConnections,
                                   1000,
                                   1000,
                                   32 * 1024);
        this.dest1 = new SocketDestination("localhost", port1);
        this.dest2 = new SocketDestination("localhost", port2);
        VoldemortNativeRequestHandler requestHandler = new VoldemortNativeRequestHandler(new ErrorCodeMapper(),
                                                                                         new StoreRepository());
        this.server1 = new SocketServer(port1,
                                        maxTotalConnections,
                                        maxTotalConnections + 3,
                                        10000,
                                        requestHandler);
        this.server2 = new SocketServer(port2,
                                        maxTotalConnections,
                                        maxTotalConnections + 3,
                                        10000,
                                        requestHandler);
        this.server1.start();
        this.server1.awaitStartupCompletion();
        this.server2.start();
        this.server2.awaitStartupCompletion();
    }

    @Override
    public void tearDown() {
        this.pool.close();
        this.server1.shutdown();
        this.server2.shutdown();
    }

    public void testTwoCheckoutsGetTheSameSocket() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest1);
        pool.checkin(dest1, sas1);
        SocketAndStreams sas2 = pool.checkout(dest1);
        assertTrue(sas1 == sas2);
    }

    public void testClosingSocketDeactivates() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest1);
        sas1.getSocket().close();
        pool.checkin(dest1, sas1);
        SocketAndStreams sas2 = pool.checkout(dest1);
        assertTrue(sas1 != sas2);
    }

    public void testClosingStreamDeactivates() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest1);
        sas1.getOutputStream().close();
        pool.checkin(dest1, sas1);
        SocketAndStreams sas2 = pool.checkout(dest1);
        assertTrue(sas1 != sas2);
    }

}
