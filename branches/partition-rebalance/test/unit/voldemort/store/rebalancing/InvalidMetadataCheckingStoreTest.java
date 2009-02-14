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

package voldemort.store.rebalancing;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.DoNothingStore;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;

/**
 * @author bbansal
 * 
 */
public class InvalidMetadataCheckingStoreTest extends TestCase {

    private static int LOOP_COUNT = 1000;

    public void testValidMetaData() {
        boolean sawException = false;
        RoutingStrategy routingStrategy = getRoutingStrategy(new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6, 7 }, { 8, 9, 10, 11 } }, 1);
        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<byte[], byte[]>("test"),
                                                                              routingStrategy);

        try {
            doOperations(0, store, store.getRoutingStrategy());
        } catch(InvalidMetadataException e) {
            sawException = true;
        }

        assertEquals("Should not see any InvalidMetaDataException", false, sawException);
    }

    /**
     * NOTE: the total number of partitions should remain same for hash
     * conistency
     */
    public void testAddingPartition() {
        boolean sawException = false;
        RoutingStrategy routingStrategy = getRoutingStrategy(new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6, 7 }, { 8, 9, 10, 11 } }, 1);

        RoutingStrategy updatedRoutingStrategy = getRoutingStrategy(new int[][] {
                { 0, 1, 2, 3, 11 }, { 4, 5, 6, 7 }, { 8, 9, 10 } }, 1);

        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<byte[], byte[]>("test"),
                                                                              updatedRoutingStrategy);
        try {
            doOperations(0, store, routingStrategy);
        } catch(InvalidMetadataException e) {
            sawException = true;
        }

        assertEquals("Should not see any InvalidMetaDataException", false, sawException);
    }

    public void testRemovingPartition() {
        boolean sawException = false;
        RoutingStrategy routingStrategy = getRoutingStrategy(new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6, 7 }, { 8, 9, 10, 11 } }, 1);

        RoutingStrategy updatedRoutingStrategy = getRoutingStrategy(new int[][] { { 0, 1, 2 },
                { 4, 5, 6, 7, 3 }, { 8, 9, 10, 11 } }, 1);

        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<byte[], byte[]>("test"),
                                                                              updatedRoutingStrategy);
        try {
            doOperations(0, store, routingStrategy);
        } catch(InvalidMetadataException e) {
            sawException = true;
        }

        assertEquals("Should see InvalidMetaDataException", true, sawException);
    }

    private RoutingStrategy getRoutingStrategy(int[][] partitionMap, int numReplication) {
        ArrayList<Node> nodes = new ArrayList<Node>(partitionMap.length);
        ArrayList<Integer> partitionList = new ArrayList<Integer>();

        for(int i = 0; i < partitionMap.length; i++) {
            partitionList.clear();
            for(int p = 0; p < partitionMap[i].length; p++) {
                partitionList.add(partitionMap[i][p]);
            }
            nodes.add(new Node(i, "localhost", 8880 + i, 6666 + i, partitionList));
        }

        return new ConsistentRoutingStrategy(nodes, numReplication);
    }

    private boolean containsNodeId(List<Node> nodes, int nodeId) {
        for(Node node: nodes) {
            if(nodeId == node.getId()) {
                return true;
            }
        }
        return false;
    }

    private void doOperations(int nodeId,
                              Store<byte[], byte[]> store,
                              RoutingStrategy routingStrategy) {
        for(int i = 0; i < LOOP_COUNT;) {
            byte[] key = ByteUtils.md5(new Integer((int) (Math.random() * Integer.MAX_VALUE)).toString()
                                                                                             .getBytes());
            byte[] value = "value".getBytes();

            if(containsNodeId(routingStrategy.routeRequest(key), nodeId)) {
                i++; // increment count
                switch(i % 3) {
                    case 0:
                        store.get(key);
                        break;
                    case 1:
                        store.delete(key, null);
                        break;
                    case 2:
                        store.put(key, new Versioned<byte[]>(value));
                        break;

                }
            }
        }

    }
}
