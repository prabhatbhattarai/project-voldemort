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

package voldemort.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;

import voldemort.ServerTestUtils;
import voldemort.client.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

/**
 * @author bbansal
 * 
 */
public class RebalancingTest extends TestCase {

    private static String TEMP_DIR = "test/unit/temp-output";
    private static String storeName = "test-replication-1";

    VoldemortServer server1;
    VoldemortServer server2;
    Cluster cluster;

    @Override
    public void setUp() throws IOException {
        // make a 2 node cluster with free ports
        int[] ports = ServerTestUtils.findFreePorts(3);
        Node node0 = new Node(0,
                              "localhost",
                              ports[0],
                              ports[1],
                              ports[2],
                              Arrays.asList(new Integer[] { 0, 1 }));

        ports = ServerTestUtils.findFreePorts(3);
        Node node1 = new Node(1,
                              "localhost",
                              ports[0],
                              ports[1],
                              ports[2],
                              Arrays.asList(new Integer[] { 2, 3 }));

        cluster = new Cluster("admin-service-test", Arrays.asList(new Node[] { node0, node1 }));

        VoldemortConfig config = createServerConfig(0);
        server1 = new VoldemortServer(config, cluster);
        server1.start();

        config = createServerConfig(1);
        server2 = new VoldemortServer(config, cluster);
        server2.start();
    }

    @Override
    public void tearDown() throws IOException, InterruptedException {
        server1.stop();
        server2.stop();
        FileDeleteStrategy.FORCE.delete(new File(TEMP_DIR));
    }

    private VoldemortConfig createServerConfig(int nodeId) throws IOException {
        Props props = new Props();
        props.put("node.id", nodeId);
        props.put("voldemort.home", TEMP_DIR + "/node-" + nodeId);
        props.put("bdb.cache.size", 1 * 1024 * 1024);
        props.put("jmx.enable", "false");
        VoldemortConfig config = new VoldemortConfig(props);

        // clean and reinit metadata dir.
        File tempDir = new File(config.getMetadataDirectory());
        tempDir.mkdirs();

        File tempDir2 = new File(config.getDataDirectory());
        tempDir2.mkdirs();

        // copy cluster.xml / stores.xml to temp metadata dir.
        FileUtils.copyFileToDirectory(new File("test/common/voldemort/config/cluster.xml"), tempDir);
        FileUtils.copyFileToDirectory(new File("test/common/voldemort/config/stores.xml"), tempDir);

        return config;
    }

    public void testStealPartitions() throws IOException {
        // enter data into server 1 & 2
        for(int i = 1; i <= 1000; i++) {
            byte[] key = ByteUtils.getBytes("" + i, "UTF-8");
            byte[] value = ByteUtils.getBytes("value-" + i, "UTF-8");

            loadEntry(new ByteArray(key), value, storeName);
        }

        // Add a new node to cluster config with blank partition List
        List<Node> nodes = new ArrayList<Node>(server1.getCluster().getNodes());
        nodes.add(new Node(2, "localhost", 8083, 6669, 7779, new ArrayList<Integer>()));
        Cluster updatedCluster = new Cluster("updated-cluster", nodes);

        VoldemortConfig config = createServerConfig(2);
        VoldemortServer server3 = new VoldemortServer(config, updatedCluster);
        server3.start();

        // do stealPartitions
        AdminClient client = new AdminClient(server3.getIdentityNode(),
                                             server3.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 10000));
        // persist updated Cluster to metadata here
        client.updateClusterMetaData(2, updatedCluster, MetadataStore.CLUSTER_KEY);

        client.stealPartitionsFromCluster(2, storeName);

        Store<ByteArray, byte[]> store3 = server3.getStoreMap().get(storeName);
        store3.put(new ByteArray("server3-test-key".getBytes()),
                   new Versioned<byte[]>("test-entry".getBytes()));
        assertEquals("put should pass for InMemoryStore",
                     "test-entry",
                     new String(store3.get(new ByteArray("server3-test-key".getBytes()))
                                      .get(0)
                                      .getValue()));

        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(server3.getCluster()
                                                                               .getNodes(), 1);

        // check all keys are present in new Store
        for(int i = 0; i <= 1000; i++) {
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + i, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + i, "UTF-8");

            Node node = routingStrategy.routeRequest(key.get()).get(0);
            if(node.getId() == 2) {
                assertEquals("key for partition belonging to new node should be present",
                             new String(value),
                             new String(store3.get(key).get(0).getValue()));
            }
        }

        // check all Store keys belong to new node partition only
        // TODO

        server3.stop();
    }

    public void testDonatePartitions() throws IOException {
        // Add a new node to cluster config with blank partition List
        List<Node> nodes = new ArrayList<Node>(server1.getCluster().getNodes());
        nodes.add(new Node(2, "localhost", 8083, 6669, 7779, new ArrayList<Integer>()));
        Cluster updatedCluster = new Cluster("updated-cluster", nodes);

        // enter data into server 1 & 2
        for(int i = 1; i <= 1000; i++) {
            byte[] key = ByteUtils.getBytes("" + i, "UTF-8");
            byte[] value = ByteUtils.getBytes("value-" + i, "UTF-8");

            loadEntry(new ByteArray(key), value, storeName);
        }

        VoldemortConfig config = createServerConfig(2);
        VoldemortServer server3 = new VoldemortServer(config, updatedCluster);
        server3.start();

        AdminClient client = new AdminClient(server1.getIdentityNode(),
                                             server1.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 10000));
        // persist updated Cluster to metadata for node 1
        client.updateClusterMetaData(0, updatedCluster, MetadataStore.CLUSTER_KEY);

        // do donatePartitions
        client.donatePartitionsToCluster(1, storeName, 2, false);

        // Assert server 3 got 2 partitions
        Store<ByteArray, byte[]> store3 = server3.getStoreMap().get(storeName);
        int matched = 0;
        for(int i = 0; i <= 1000; i++) {
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + i, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + i, "UTF-8");

            if(store3.get(key).size() > 0) {
                matched++;
            }
        }
        assertEquals("Atleast one key value should be returned", true, matched > 0);
        assertEquals("Aprox 1/2th of keys should be there", true, matched > 400 && matched < 600);

        server3.stop();
    }

    private void loadEntry(ByteArray key, byte[] value, String storeName) {
        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(server1.getCluster()
                                                                               .getNodes(), 1);
        Node node = routingStrategy.routeRequest(key.get()).get(0);

        switch(node.getId()) {
            case 0:
                Store<ByteArray, byte[]> store1 = server1.getStoreMap().get(storeName);
                store1.put(new ByteArray(key.get()), new Versioned<byte[]>(value));
                break;
            case 1:
                Store<ByteArray, byte[]> store2 = server2.getStoreMap().get(storeName);
                store2.put(new ByteArray(key.get()), new Versioned<byte[]>(value));
                break;
        }
    }

}
