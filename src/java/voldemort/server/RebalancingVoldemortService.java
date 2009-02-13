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

import java.util.Map;

import voldemort.cluster.Cluster;
import voldemort.store.Store;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Extends {@link RebalancingVoldemortService} to add rebalancing capabilities.
 * 
 * @author bbansal
 * 
 */
public interface RebalancingVoldemortService extends VoldemortService {

    /**
     * updates cluster description for current service as passed .
     * 
     * @param {@link ClusterMapper} : definition for the new Cluster.
     * @returns void
     * @throws UnableUpdateMetadataException
     */
    public void updateClusterMetadata(Cluster updatedCluster) throws UnableUpdateMetadataException;

    /**
     * updates Stores description for current service as passed in params.
     * 
     * @param : {@link StoreDefinitionsMapper} definition for the new Store.
     * @returns void
     * @throws UnableUpdateMetadataException
     */
    public void updateStoreMetadata(Map<String, Store<byte[], byte[]>> updatedStores)
            throws UnableUpdateMetadataException;
}
