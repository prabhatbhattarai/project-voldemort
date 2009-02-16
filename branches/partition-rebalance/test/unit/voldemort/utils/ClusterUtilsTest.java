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

package voldemort.utils;

import java.util.ArrayList;
import java.util.Collection;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

public class ClusterUtilsTest extends TestCase {

    public void testUpdateClusterAddNode() {
        int[][] partitionMap = new int[][] { { 0, 1, 2 }, { 3, 4, 5 }, { 6, 7, 8 } };
        Collection<Node> nodes = TestUtils.createNodes(partitionMap);
        Cluster cluster = new Cluster("test-cluster", new ArrayList<Node>(nodes));

        // add a node and rebalance
        assertEquals("Num partitions moved do not match.",
                     2,
                     TestUtils.getPartitionsDiff(cluster,
                                                 ClusterUtils.updateClusterAddNode(cluster,
                                                                                   3,
                                                                                   "",
                                                                                   8084,
                                                                                   6669)));
    }

    public void testUpdateClusterAddNode2() {
        int[][] partitionMap = new int[][] { { 0, 1, 2 }, { 3, 4, 5 }, { 6, 7, 8, 9, 10, 11 } };
        Collection<Node> nodes = TestUtils.createNodes(partitionMap);
        Cluster cluster = new Cluster("test-cluster", new ArrayList<Node>(nodes));

        // add a node and rebalance
        assertEquals("Num partitions moved do not match.",
                     3,
                     TestUtils.getPartitionsDiff(cluster,
                                                 ClusterUtils.updateClusterAddNode(cluster,
                                                                                   4,
                                                                                   "",
                                                                                   8084,
                                                                                   6669)));
    }
}
