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
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

/**
 * Helper functions to get updated cluster by deleting/adding a node keeping the
 * partitions changes to minimum.
 * 
 * @author bbansal
 * 
 */
public class ClusterUtils {

    /**
     * Helper function to create an updated cluster configuration given an
     * existing cluster configuration by adding a node.
     * <p>
     * assumes all nodes are equal steals partition using a priority queue
     * 
     * @param cluster
     * @param nodeId
     * @return
     */
    public static Cluster updateClusterStealPartitions(Cluster cluster, Node newNode) {
        int totalPartitons = 0;
        PriorityQueue<ComparableNode> queue = new PriorityQueue<ComparableNode>(cluster.getNumberOfNodes(),
                                                                                Collections.reverseOrder());
        for(Node node: cluster.getNodes()) {
            if(node.getId() != newNode.getId()) {
                queue.add(new ComparableNode(node, node.getPartitionIds()));
            }
            totalPartitons += node.getNumberOfPartitions();
        }

        int numStealPartitons = (int) (totalPartitons / (queue.size() + 1));

        // initialize stealList with existing partitions.
        ArrayList<Integer> stealList = new ArrayList<Integer>(newNode.getPartitionIds());

        while(stealList.size() < numStealPartitons) {
            ComparableNode node = queue.poll();
            // steal the last partition.
            stealList.add(node.getPartitions().remove(node.getPartitions().size() - 1));
            // add the node back into queue.
            queue.add(node);
        }

        // Lets make the new Cluster now !!
        ArrayList<Node> nodes = new ArrayList<Node>();
        nodes.add(new Node(newNode.getId(),
                           newNode.getHost(),
                           newNode.getHttpPort(),
                           newNode.getSocketPort(),
                           newNode.getAdminPort(),
                           stealList,
                           newNode.getStatus()));

        for(ComparableNode node: queue) {
            nodes.add(new Node(node.getNode().getId(),
                               node.getNode().getHost(),
                               node.getNode().getHttpPort(),
                               node.getNode().getSocketPort(),
                               node.getNode().getAdminPort(),
                               node.getPartitions()));
        }

        return new Cluster(cluster.getName(), nodes);
    }

    /**
     * 
     * @param cluster
     */
    public static String GetClusterAsString(Cluster cluster) {
        StringBuilder builder = new StringBuilder("[");
        for(Node node: cluster.getNodes()) {
            builder.append(node + "(");
            for(Integer p: node.getPartitionIds()) {
                builder.append(p);
                builder.append(",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(") ");
        }
        builder.append("]");
        return builder.toString().trim();
    }

    /**
     * Helper function to create an updated cluster configuration given an
     * existing cluster configuration by deleting a node.
     * <p>
     * assumes all nodes are equal steals partition using a priority queue
     * 
     * @param cluster
     * @param nodeId
     * @return
     */
    public static Cluster updateClusterDeleteNode(Cluster cluster, int nodeId) {
        Node deletedNode = null;
        PriorityQueue<ComparableNode> queue = new PriorityQueue<ComparableNode>();
        for(Node node: cluster.getNodes()) {
            if(node.getId() == nodeId) {
                deletedNode = node;
                continue;
            }
            queue.add(new ComparableNode(node, node.getPartitionIds()));
        }
        if(null == deletedNode) {
            throw new IllegalArgumentException("nodeID:" + nodeId + " is not present in  cluster.");
        }

        ArrayList<Integer> stealList = new ArrayList<Integer>(deletedNode.getPartitionIds());
        while(stealList.size() > 0) {
            ComparableNode node = queue.poll();

            // add the node back into queue
            node.getPartitions().add(stealList.remove(stealList.size() - 1));
            queue.add(node);
        }

        // Lets make the new Cluster now !!
        ArrayList<Node> nodes = new ArrayList<Node>();
        for(ComparableNode node: queue) {
            nodes.add(new Node(node.getNode().getId(),
                               node.getNode().getHost(),
                               node.getNode().getSocketPort(),
                               node.getNode().getAdminPort(),
                               node.getNode().getHttpPort(),
                               node.getPartitions()));
        }

        return new Cluster(cluster.getName(), nodes);
    }

    private static class ComparableNode implements Comparable<ComparableNode> {

        private Node _node;
        private ArrayList<Integer> _partitions;

        public ComparableNode(Node node, List<Integer> partitions) {
            this._node = node;
            this._partitions = new ArrayList<Integer>(partitions);
        }

        public int compareTo(ComparableNode o) {
            return new Integer(getPartitions().size()).compareTo(o.getPartitions().size());
        }

        public Node getNode() {
            return _node;
        }

        public ArrayList<Integer> getPartitions() {
            return _partitions;
        }
    }
}
