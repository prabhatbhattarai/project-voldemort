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

package voldemort;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.TestCase;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.utils.ClusterUtils;
import voldemort.versioning.VectorClock;

/**
 * Helper utilities for tests
 * 
 * @author jay
 * 
 */
public class TestUtils {

    public static final String DIGITS = "0123456789";
    public static final String LETTERS = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
    public static final String CHARACTERS = LETTERS + DIGITS + "~!@#$%^&*()____+-=[];',,,./>?:{}";
    private static final Random random = new Random(19873482374L);

    /**
     * Get a vector clock with events on the sequence of nodes given
     * 
     * @param nodes The sequence of nodes
     * @return A VectorClock initialized with the given sequence of events
     */
    public static VectorClock getClock(int... nodes) {
        VectorClock clock = new VectorClock();
        increment(clock, nodes);
        return clock;
    }

    /**
     * Record events for the given sequence of nodes
     * 
     * @param clock The VectorClock to record the events on
     * @param nodes The sequences of node events
     */
    public static void increment(VectorClock clock, int... nodes) {
        for(int n: nodes)
            clock.incrementVersion((short) n, System.currentTimeMillis());
    }

    /**
     * Test two byte arrays for (deep) equality. I think this exists in java 6
     * but not java 5
     * 
     * @param a1 Array 1
     * @param a2 Array 2
     * @return True iff a1.length == a2.length and a1[i] == a2[i] for 0 <= i <
     *         a1.length
     */
    public static boolean bytesEqual(byte[] a1, byte[] a2) {
        if(a1 == a2) {
            return true;
        } else if(a1 == null || a2 == null) {
            return false;
        } else if(a1.length != a2.length) {
            return false;
        } else {
            for(int i = 0; i < a1.length; i++)
                if(a1[i] != a2[i])
                    return false;
        }

        return true;
    }

    /**
     * Create a string with some random letters
     * 
     * @param random The Random number generator to use
     * @param length The length of the string to create
     * @return The string
     */
    public static String randomLetters(int length) {
        return randomString(LETTERS, length);
    }

    /**
     * Create a string that is a random sample (with replacement) from the given
     * string
     * 
     * @param sampler The string to sample from
     * @param length The length of the string to create
     * @return The created string
     */
    public static String randomString(String sampler, int length) {
        StringBuilder builder = new StringBuilder(length);
        for(int i = 0; i < length; i++)
            builder.append(sampler.charAt(random.nextInt(sampler.length())));
        return builder.toString();
    }

    public static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    /**
     * Compute the requested quantile of the given array
     * 
     * @param values The array of values
     * @param quantile The quantile requested (must be between 0.0 and 1.0
     *        inclusive)
     * @return The quantile
     */
    public static long quantile(long[] values, double quantile) {
        if(values == null)
            throw new IllegalArgumentException("Values cannot be null.");
        if(quantile < 0.0 || quantile > 1.0)
            throw new IllegalArgumentException("Quantile must be between 0.0 and 1.0");

        long[] copy = new long[values.length];
        System.arraycopy(values, 0, copy, 0, copy.length);
        Arrays.sort(copy);
        int index = (int) (copy.length * quantile);
        return copy[index];
    }

    public static File getTempDirectory() {
        String tempDir = System.getProperty("java.io.tmpdir") + File.separatorChar
                         + (Math.abs(random.nextInt()) % 1000000);
        File temp = new File(tempDir);
        temp.mkdir();
        return temp;
    }

    public static String str(String s) {
        return "\"" + s + "\"";
    }

    public static RoutingStrategy getRoutingStrategy(int[][] partitionMap, int numReplication) {
        return new ConsistentRoutingStrategy(createNodes(partitionMap), numReplication);
    }

    public static Collection<Node> createNodes(int[][] partitionMap) {
        ArrayList<Node> nodes = new ArrayList<Node>(partitionMap.length);
        ArrayList<Integer> partitionList = new ArrayList<Integer>();

        for(int i = 0; i < partitionMap.length; i++) {
            partitionList.clear();
            for(int p = 0; p < partitionMap[i].length; p++) {
                partitionList.add(partitionMap[i][p]);
            }
            nodes.add(new Node(i, "localhost", 8880 + i, 6666 + i, partitionList));
        }

        return nodes;
    }

    public static void checkClusterMatch(Cluster A, Cluster B) {
        System.out.println("cluster A:" + ClusterUtils.GetClusterAsString(A));
        System.out.println("cluster B:" + ClusterUtils.GetClusterAsString(B));

        TestCase.assertEquals("num nodes do not match.", A.getNodes().size(), B.getNodes().size());

        ArrayList<Node> nodeAList = new ArrayList<Node>(A.getNodes());

        for(int i = 0; i < A.getNodes().size(); i++) {
            Node nodeA = nodeAList.get(i);
            Node nodeB = B.getNodeById(nodeA.getId());
            TestCase.assertEquals("NodeId do not match", nodeA.getId(), nodeB.getId());
            TestCase.assertEquals("num partitions for Node:" + nodeA.getId() + " Do not match",
                                  nodeA.getNumberOfPartitions(),
                                  nodeB.getNumberOfPartitions());

            for(int j = 0; j < nodeA.getNumberOfPartitions(); j++) {
                TestCase.assertEquals("partitionList do not match",
                                      nodeA.getPartitionIds(),
                                      nodeB.getPartitionIds());
            }
        }

    }

    public static int getPartitionsDiff(Cluster orig, Cluster updated) {
        int diffPartition = 0;

        System.out.println("cluster A:" + ClusterUtils.GetClusterAsString(orig));
        System.out.println("cluster B:" + ClusterUtils.GetClusterAsString(updated));

        ArrayList<Node> nodeAList = new ArrayList<Node>(orig.getNodes());

        for(int i = 0; i < orig.getNodes().size(); i++) {
            Node nodeA = nodeAList.get(i);
            Node nodeB;
            try {
                nodeB = updated.getNodeById(nodeA.getId());
            } catch(VoldemortException e) {
                // add the partition in this node
                diffPartition += nodeA.getNumberOfPartitions();
                continue;
            }

            SortedSet<Integer> BpartitonSet = new TreeSet<Integer>(nodeB.getPartitionIds());
            for(int p: nodeA.getPartitionIds()) {
                if(!BpartitonSet.contains(new Integer(p))) {
                    diffPartition++;
                }

            }
        }
        return diffPartition;
    }
}
