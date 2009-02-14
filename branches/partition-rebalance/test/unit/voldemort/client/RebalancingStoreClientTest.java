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

package voldemort.client;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.ObjectSerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.server.RebalancingVoldemortServer;
import voldemort.server.VoldemortConfig;
import voldemort.store.FailingStore;
import voldemort.store.InvalidMetadataException;
import voldemort.store.StoreDefinition;
import voldemort.utils.Props;

/**
 * @author bbansal
 * 
 */
public class RebalancingStoreClientTest extends TestCase {

    private static String dir = "test/unit/temp-output";
    RebalancingVoldemortServer server;

    @Override
    public void setUp() throws IOException {
        startServer();
    }

    @Override
    public void tearDown() throws IOException {
        server.stop();
        FileDeleteStrategy.FORCE.delete(new File(dir));
    }

    private Cluster startServer() throws IOException {
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/unit/temp-output");
        props.put("bdb.cache.size", 1 * 1024 * 1024);
        VoldemortConfig config = new VoldemortConfig(props);

        // clean and reinit metadata dir.
        FileDeleteStrategy.FORCE.delete(new File(config.getMetadataDirectory()));
        File tempDir = new File(config.getMetadataDirectory());
        tempDir.mkdirs();

        // copy cluster.xml / stores.xml to temp metadata dir.
        FileUtils.copyFileToDirectory(new File("test/common/voldemort/config/cluster.xml"), tempDir);
        FileUtils.copyFileToDirectory(new File("test/common/voldemort/config/stores.xml"), tempDir);

        server = new RebalancingVoldemortServer(config);
        server.start();

        return server.getMetaDataStore().getCluster();
    }

    private void updateCluster(RebalancingVoldemortServer server) {
        Cluster cluster = server.getMetaDataStore().getCluster();

        // add node 3 and partition 4,5 to cluster.
        ArrayList<Integer> partitionList = new ArrayList<Integer>();
        ArrayList<Node> nodes = new ArrayList<Node>(cluster.getNodes());
        nodes.add(new Node(3, "localhost", 8883, 6668, partitionList));
        Cluster updatedCluster = new Cluster("new-cluster", nodes);

        // update VoldemortServer cluster.xml
        server.updateClusterMetadata(updatedCluster);
    }

    public void testBootstrapMetadata() {
        Collection<Node> nodes = server.getCluster().getNodes();
        URI[] uris = new URI[nodes.size()];
        int i = 0;
        for(Node node: nodes) {
            uris[i++] = node.getSocketUrl();
        }

        RebalancingStoreClient<byte[], byte[]> client = new RebalancingStoreClient<byte[], byte[]>(new FailingStore<byte[], byte[]>("users",
                                                                                                                                    new InvalidMetadataException("fail fail fail")),
                                                                                                   (Serializer<byte[]>) new CustomSerializerFactory().getSerializer(null),
                                                                                                   (Serializer<byte[]>) new CustomSerializerFactory().getSerializer(null),
                                                                                                   new ConsistentRoutingStrategy(server.getCluster()
                                                                                                                                       .getNodes(),
                                                                                                                                 1),
                                                                                                   uris);
        client.get("test".getBytes());

        int numRep = -1;
        for(StoreDefinition store: server.getMetaDataStore().getStores()) {
            if(store.getName().equals("users")) {
                numRep = store.getReplicationFactor();
            }
        }

        assertNotSame("Store should be present in metadata list.", numRep, -1);
        checkRoutingStrategy(client.getRoutingStrategy(),
                             new ConsistentRoutingStrategy(server.getCluster().getNodes(), numRep));

        updateCluster(server);
        client.get("test".getBytes());

        numRep = -1;
        for(StoreDefinition store: server.getMetaDataStore().getStores()) {
            if(store.getName().equals("users")) {
                numRep = store.getReplicationFactor();
            }
        }

        assertNotSame("Store should be present in metadata list.", numRep, -1);
        checkRoutingStrategy(client.getRoutingStrategy(),
                             new ConsistentRoutingStrategy(server.getCluster().getNodes(), numRep));

    }

    private class CustomSerializerFactory implements SerializerFactory {

        public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
            return new ObjectSerializer<Object>();
        }
    }

    private void checkRoutingStrategy(RoutingStrategy A, RoutingStrategy B) {
        assertEquals("num nodes do not match.", A.getNodes().size(), B.getNodes().size());

        ArrayList<Node> nodeAList = new ArrayList<Node>(A.getNodes());
        ArrayList<Node> nodeBList = new ArrayList<Node>(B.getNodes());

        for(int i = 0; i < A.getNodes().size(); i++) {
            Node nodeA = nodeAList.get(i);
            Node nodeB = nodeBList.get(i);
            assertEquals("NodeId do not match", nodeA.getId(), nodeB.getId());
            assertEquals("num partitions for Node:" + nodeA.getId() + " Do not match",
                         nodeA.getNumberOfPartitions(),
                         nodeB.getNumberOfPartitions());

            for(int j = 0; j < nodeA.getNumberOfPartitions(); j++) {
                assertEquals("partitionList do not match",
                             nodeA.getPartitionIds(),
                             nodeB.getPartitionIds());
            }
        }
    }
}
