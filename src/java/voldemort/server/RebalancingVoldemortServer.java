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

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

/**
 * Extending {@link VoldemortServer} to add Online rebalancing capabilities
 * 
 * @author bbansal
 * 
 */
public class RebalancingVoldemortServer extends VoldemortServer implements
        RebalancingVoldemortService {

    private static final Logger logger = Logger.getLogger(RebalancingVoldemortServer.class.getName());

    public RebalancingVoldemortServer(VoldemortConfig config) {
        super(config);
    }

    public RebalancingVoldemortServer(Props props, Cluster cluster) {
        super(props, cluster);
    }

    public void updateClusterMetadata(Cluster updatedCluster) throws UnableUpdateMetadataException {
        logger.info("updating server Id(" + getIdentityNode().getId() + ") with cluster info("
                    + updatedCluster.toString() + ")");
        // get current ClusterInfo
        List<Versioned<byte[]>> clusterInfo = getMetaDataStore().get(ByteUtils.getBytes(MetadataStore.CLUSTER_KEY,
                                                                                        "UTF-8"));
        if(clusterInfo.size() > 1) {
            throw new UnableUpdateMetadataException("Inconistent Cluster Metdata found on Server:"
                                                    + getIdentityNode().getId());
        }

        // update version
        VectorClock updatedVersion = ((VectorClock) clusterInfo.get(0).getVersion());
        updatedVersion.incrementVersion(getIdentityNode().getId(), System.currentTimeMillis());

        // update cluster details in metaDataStore
        getMetaDataStore().put(ByteUtils.getBytes(MetadataStore.CLUSTER_KEY, "UTF-8"),
                               new Versioned<byte[]>(ByteUtils.getBytes(new ClusterMapper().writeCluster(updatedCluster),
                                                                        "UTF-8")));

        List<VoldemortService> updatedServices = createServices();

        for(VoldemortService service: getServices()) {
            try {
                service.stop();
            } catch(Exception e) {
                logger.warn("Trouble in stopping service", e);
            }
        }

        getServices().clear();
        this.setCluster(updatedCluster);

        for(VoldemortService service: updatedServices) {
            service.start();
            getServices().add(service);
        }
    }

    public void updateStoreMetadata(Map<String, Store<byte[], byte[]>> updatedStore)
            throws UnableUpdateMetadataException {
        throw new UnableUpdateMetadataException("Store Metdata update is not supported yet.");
    }
}
