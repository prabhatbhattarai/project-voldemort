package voldemort.client.admin;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer.SERVER_STATE;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ClusterUtils;

public class PartitionRebalanceClient extends AdminClient {

    private static final Logger logger = Logger.getLogger(PartitionRebalanceClient.class);
    private final ErrorCodeMapper errorCodeMapper = new ErrorCodeMapper();

    public PartitionRebalanceClient(Node currentNode,
                                    MetadataStore metadataStore,
                                    SocketPool socketPool) {
        super(currentNode, metadataStore, socketPool);
    }

    /**
     * Rebalances the cluster by stealing partitions from current Cluster
     * configuration. <strong> Steps </strong>
     * <ul>
     * <li>Get Current Cluster configuration from {@link MetadataStore}</li>
     * <li>update current config as {@link MetadataStore#OLD_CLUSTER_KEY}</li>
     * <li>Set Current Server state as
     * {@link SERVER_STATE#REBALANCING_STEALER_STATE}</li>
     * <li>create a new cluster config by stealing partitions from all nodes</li>
     * <li>For All nodes do
     * <ul>
     * <li>identify steal list for this node and make a temp. cluster Config</li>
     * <li>Update ALL servers with temp. cluster Config</li>
     * <li>steal partitions</li>
     * </ul>
     * </li>
     * <li>Set Current Server state as {@link SERVER_STATE#NORMAL_STATE}</li>
     * </ul>
     * <p>
     * TODO: HIGH Failure Scenarios
     * <ul>
     * <li>StealerNode dies</li>
     * <li>DonorNode dies</li>
     * </ul>
     * 
     * @throws IOException
     */
    public void stealPartitionsFromCluster(int stealerNodeId, String storeName) throws IOException {
        logger.info("Node(" + getConnectedNode().getId() + ") Starting Steal Parttion Process");
        Cluster currentCluster = getMetaDataStore().getCluster();
        updateClusterMetaData(stealerNodeId, currentCluster, MetadataStore.OLD_CLUSTER_KEY);

        logger.info("Node(" + getConnectedNode().getId() + ") State changed to REBALANCING MODE");
        changeStateAndRestart(stealerNodeId, SERVER_STATE.REBALANCING_STEALER_STATE);

        Node stealerNode = currentCluster.getNodeById(stealerNodeId);
        if(stealerNode == null) {
            throw new VoldemortException("stealerNode id:" + stealerNodeId
                                         + " should be present in initial cluster");
        }

        Cluster updatedCluster = ClusterUtils.updateClusterStealPartitions(currentCluster,
                                                                           stealerNode);
        try {
            for(Node donorNode: currentCluster.getNodes()) {
                if(donorNode.getId() != stealerNodeId) {
                    List<Integer> stealList = getStealList(currentCluster,
                                                           updatedCluster,
                                                           donorNode.getId(),
                                                           stealerNodeId);
                    logger.info("Node(" + getConnectedNode().getId() + ") Stealing from node:"
                                + donorNode.getId() + " stealList:" + stealList);

                    if(stealList.size() > 0) {
                        Cluster tempCluster = getTempCluster(currentCluster,
                                                             donorNode,
                                                             stealerNode,
                                                             stealList);

                        logger.info("tempCluster:" + ClusterUtils.GetClusterAsString(tempCluster));

                        // set tempCluster on Donor node and stream partitions
                        updateClusterMetaData(donorNode.getId(),
                                              tempCluster,
                                              MetadataStore.CLUSTER_KEY);

                        changeStateAndRestart(donorNode.getId(),
                                              SERVER_STATE.REBALANCING_DONOR_STATE);
                        pipeGetAndPutStreams(donorNode.getId(), stealerNodeId, storeName, stealList);
                        changeStateAndRestart(donorNode.getId(), SERVER_STATE.NORMAL_STATE);
                    }
                }
            }

            for(Node node: currentCluster.getNodes()) {
                updateClusterMetaData(node.getId(), updatedCluster, MetadataStore.CLUSTER_KEY);
            }
            changeStateAndRestart(stealerNode.getId(), SERVER_STATE.NORMAL_STATE);
            logger.info("Node(" + getConnectedNode().getId()
                        + ") State changed back to NORMAL MODE");

            logger.info("Node(" + getConnectedNode().getId() + ") Steal process completed.");
        } catch(Exception e) {
            // undo all changes
            for(Node node: currentCluster.getNodes()) {
                updateClusterMetaData(node.getId(), updatedCluster, MetadataStore.OLD_CLUSTER_KEY);
            }
            throw new VoldemortException("Steal Partitions for " + stealerNodeId + " failed", e);
        }
    }

    /**
     * Rebalances the cluster by deleting current node and returning partitions
     * to other nodes in cluster. <strong> Steps </strong>
     * <ul>
     * <li>Get Current Cluster configuration from {@link MetadataStore}</li>
     * <li>Create new Cluster config by identifying partitions to return</li>
     * <li>For All nodes do
     * <ul>
     * <li>identify steal list for this node 'K'</li>
     * <li>update current config as {@link MetadataStore#OLD_CLUSTER_KEY} on
     * remote node 'K'</li>
     * <li>create a temp cluster config</li>
     * <li>Update ALL servers with temp cluster Config</li>
     * <li>Set remote node 'K' state as
     * {@link SERVER_STATE#REBALANCING_STEALER_STATE}</li>
     * <li>return partitions</li>
     * <li>Set remote node 'K' state as {@link SERVER_STATE#NORMAL_STATE}</li>
     * </ul>
     * </li>
     * </ul>
     * 
     * @throws IOException
     */
    public void donatePartitionsToCluster(int donorNodeId,
                                          String storeName,
                                          int numPartitions,
                                          boolean deleteNode) throws IOException {
        logger.info("Node(" + getConnectedNode().getId() + ") Starting Donate Partition Process");

        Cluster currentCluster = getMetaDataStore().getCluster();
        Cluster updatedCluster = ClusterUtils.updateClusterDonatePartitions(currentCluster,
                                                                            donorNodeId,
                                                                            numPartitions,
                                                                            deleteNode);
        Node donorNode = updatedCluster.getNodeById(donorNodeId);
        changeStateAndRestart(donorNodeId, SERVER_STATE.REBALANCING_DONOR_STATE);
        logger.info("originalCluster:" + ClusterUtils.GetClusterAsString(currentCluster));
        logger.info("updatedCluster:" + ClusterUtils.GetClusterAsString(updatedCluster));

        for(Node node: updatedCluster.getNodes()) {
            if(node.getId() != donorNode.getId()) {
                logger.info("Node(" + donorNodeId + ") Donating to node:" + node.getId());

                updateClusterMetaData(node.getId(), currentCluster, MetadataStore.OLD_CLUSTER_KEY);

                List<Integer> stealList = getStealList(currentCluster,
                                                       updatedCluster,
                                                       donorNode.getId(),
                                                       node.getId());
                if(stealList.size() == 0) {
                    continue;
                }
                Cluster tempCluster = getTempCluster(currentCluster, donorNode, node, stealList);
                logger.info("tempCluster:" + ClusterUtils.GetClusterAsString(tempCluster));

                for(Node tempNode: tempCluster.getNodes()) {
                    updateClusterMetaData(tempNode.getId(), tempCluster, MetadataStore.CLUSTER_KEY);
                }

                pipeGetAndPutStreams(donorNode.getId(), node.getId(), storeName, stealList);

                changeStateAndRestart(node.getId(), SERVER_STATE.NORMAL_STATE);
            }
        }
        logger.info("Node(" + getConnectedNode().getId() + ") Donate process completed ..");
    }
}