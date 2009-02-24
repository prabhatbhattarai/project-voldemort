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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;

import voldemort.TestUtils;
import voldemort.client.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer.SERVER_STATE;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

/**
 * @author bbansal
 * 
 */
public class AdminServiceTest extends TestCase {

    VoldemortConfig config;
    VoldemortServer server;

    @Override
    public void setUp() throws IOException {
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/unit/temp-output");
        props.put("bdb.cache.size", 1 * 1024 * 1024);
        config = new VoldemortConfig(props);

        // clean and reinit metadata dir.
        FileDeleteStrategy.FORCE.delete(new File(config.getMetadataDirectory()));
        File tempDir = new File(config.getMetadataDirectory());
        tempDir.mkdirs();

        File tempDir2 = new File(config.getDataDirectory());
        tempDir2.mkdirs();

        // copy cluster.xml / stores.xml to temp metadata dir.
        FileUtils.copyFileToDirectory(new File("test/common/voldemort/config/cluster.xml"), tempDir);
        FileUtils.copyFileToDirectory(new File("test/common/voldemort/config/stores.xml"), tempDir);

        server = new VoldemortServer(config);
        server.start();
    }

    @Override
    public void tearDown() throws IOException, InterruptedException {
        server.stop();
        FileDeleteStrategy.FORCE.delete(new File(config.getMetadataDirectory()));
        FileDeleteStrategy.FORCE.delete(new File(config.getDataDirectory()));
    }

    public void testUpdateCluster() {

        Cluster cluster = server.getMetaDataStore().getCluster();

        // add node 3 and partition 4,5 to cluster.
        ArrayList<Integer> partitionList = new ArrayList<Integer>();
        partitionList.add(4);
        partitionList.add(5);
        ArrayList<Node> nodes = new ArrayList<Node>(cluster.getNodes());
        nodes.add(new Node(3, "localhost", 8883, 6668, 7778, partitionList));
        Cluster updatedCluster = new Cluster("new-cluster", nodes);

        // update VoldemortServer cluster.xml
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000));

        client.updateClusterMetaData(server.getIdentityNode().getId(),
                                     updatedCluster,
                                     MetadataStore.CLUSTER_KEY);

        TestUtils.checkClusterMatch(updatedCluster, server.getMetaDataStore().getCluster());
    }

    public void testUpdateOldCluster() {
        Cluster cluster = server.getMetaDataStore().getCluster();

        // add node 3 and partition 4,5 to cluster.
        ArrayList<Integer> partitionList = new ArrayList<Integer>();
        partitionList.add(4);
        partitionList.add(5);
        ArrayList<Node> nodes = new ArrayList<Node>(cluster.getNodes());
        nodes.add(new Node(3, "localhost", 8883, 6668, 7778, partitionList));
        Cluster updatedCluster = new Cluster("new-cluster", nodes);

        // update VoldemortServer cluster.xml
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000));

        client.updateClusterMetaData(server.getIdentityNode().getId(),
                                     updatedCluster,
                                     MetadataStore.OLD_CLUSTER_KEY);

        Cluster metaCluster = new ClusterMapper().readCluster(new StringReader(new String(server.getMetaDataStore()
                                                                                                .get(ByteUtils.getBytes(MetadataStore.OLD_CLUSTER_KEY,
                                                                                                                        "UTF-8"))
                                                                                                .get(0)
                                                                                                .getValue())));
        TestUtils.checkClusterMatch(updatedCluster, metaCluster);
    }

    public void testUpdateStores() {
        List<StoreDefinition> storesList = server.getMetaDataStore().getStores();

        // user store should be present
        assertNotSame("StoreDefinition for 'users' should not be nul ",
                      null,
                      server.getMetaDataStore().getStore("users"));

        // remove store users from storesList and update store info.
        for(StoreDefinition def: storesList) {
            if(def.getName().equals("users")) {
                storesList.remove(def);
            }
        }

        // update server stores info
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000));

        client.updateStoresMetaData(server.getIdentityNode().getId(), storesList);

        boolean foundUserStore = false;
        for(StoreDefinition def: server.getMetaDataStore().getStores()) {
            if(def.getName().equals("users")) {
                foundUserStore = true;
            }
        }
        assertEquals("Store users should no longer be available", false, foundUserStore);
    }

    public void testRedirectGet() {
        List<StoreDefinition> storesList = server.getMetaDataStore().getStores();

        // user store should be present
        Store<byte[], byte[]> store = server.getStoreMap().get("users");

        assertNotSame("Store 'users' should not be null", null, store);

        byte[] key = ByteUtils.getBytes("test_member_1", "UTF-8");
        byte[] value = "test-value-1".getBytes();

        store.put(key, new Versioned<byte[]>(value));

        // check direct get
        assertEquals("Direct Get should succeed", new String(value), new String(store.get(key)
                                                                                     .get(0)
                                                                                     .getValue()));

        // update server stores info
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000));

        assertEquals("RedirectGet should match put value",
                     new String(value),
                     new String(client.redirectGet(server.getIdentityNode().getId(), "users", key)
                                      .get(0)
                                      .getValue()));
    }

    public void testRestart() {
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000));
        client.restartServices(server.getIdentityNode().getId());
    }

    public void testStateTransitions() {
        // change to REBALANCING STATE
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000));
        client.setRebalancingStateAndRestart(server.getIdentityNode().getId());

        List<Versioned<byte[]>> values = server.getMetaDataStore()
                                               .get(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                       "UTF-8"));
        SERVER_STATE state = SERVER_STATE.valueOf(new String(values.get(0).getValue()));
        assertEquals("State should be changed correctly to rebalancing state",
                     SERVER_STATE.REBALANCING_STATE,
                     state);

        // change back to NORMAL state
        client.setNormalStateAndRestart(server.getIdentityNode().getId());

        values = server.getMetaDataStore().get(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                  "UTF-8"));
        state = SERVER_STATE.valueOf(new String(values.get(0).getValue()));
        assertEquals("State should be changed correctly to rebalancing state",
                     SERVER_STATE.NORMAL_STATE,
                     state);
    }
}
