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

import java.io.StringReader;
import java.net.URI;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.cluster.Cluster;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.serialization.StringSerializer;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * extends the {@link DefaultStoreClient} implementation to provide support for
 * InvalidMetadataException. the store should request new Metadata and update
 * itself on seeing any InvalidMetadataException
 * 
 * @author bbansal
 * 
 * @param <K> The key type
 * @param <V> The value type
 */
@Threadsafe
public class RebalancingStoreClient<K, V> extends DefaultStoreClient<K, V> {

    private static final Logger logger = Logger.getLogger(RebalancingStoreClient.class.getName());
    private final URI[] serverURLs;

    public RebalancingStoreClient(Store<K, V> store,
                                  Serializer<K> keySerializer,
                                  Serializer<V> valueSerializer,
                                  RoutingStrategy routingStrategy,
                                  URI[] serverURLs) {
        super(store, keySerializer, valueSerializer, routingStrategy);
        this.serverURLs = serverURLs;
    }

    public boolean delete(K key, Version version) {
        try {
            return super.delete(key, version);
        } catch(InvalidMetadataException e) {
            logger.info("Client delete failed with InvalidMetadataException updating Metadata information now.");
            this.setRoutingStrategy(updateRoutingStrategy());
            throw new InsufficientOperationalNodesException("delete failed because of temporary InvalidMetadataException try again !!");
        }
    }

    public Versioned<V> get(K key) {
        try {
            return super.get(key, null);
        } catch(InvalidMetadataException e) {
            logger.info("Client put failed with InvalidMetadataException updating Metadata information now.");
            this.setRoutingStrategy(updateRoutingStrategy());
            return null;
        }
    }

    public void put(K key, Versioned<V> versioned) throws ObsoleteVersionException {
        try {
            super.put(key, versioned);
        } catch(InvalidMetadataException e) {
            logger.info("Client put failed with InvalidMetadataException updating Metadata information now.");
            this.setRoutingStrategy(updateRoutingStrategy());
            throw new InsufficientOperationalNodesException("Put failed because of temporary InvalidMetadataException try again !!");
        }
    }

    private String bootstrapMetadata(String key, URI[] urls) {
        String value;
        for(URI url: urls) {
            try {
                Store<byte[], byte[]> remoteStore = new SocketStore(MetadataStore.METADATA_STORE_NAME,
                                                                    url.getHost(),
                                                                    url.getPort(),
                                                                    new SocketPool(1, 1, 2000));

                Store<String, String> store = new SerializingStore<String, String>(remoteStore,
                                                                                   new StringSerializer("UTF-8"),
                                                                                   new StringSerializer("UTF-8"));
                List<Versioned<String>> found = store.get(key);
                if(found.size() == 1)
                    return found.get(0).getValue();
            } catch(Exception e) {

            }
        }
        throw new BootstrapFailureException("No available boostrap servers found!");
    }

    private RoutingStrategy updateRoutingStrategy() {
        logger.info("client updating metadata from server.");
        String clusterXml = bootstrapMetadata(MetadataStore.CLUSTER_KEY, serverURLs);
        Cluster cluster = new ClusterMapper().readCluster(new StringReader(clusterXml));

        String storesXml = bootstrapMetadata(MetadataStore.STORES_KEY, serverURLs);
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml));

        StoreDefinition storeDef = null;
        for(StoreDefinition d: storeDefs)
            if(d.getName().equals(getStore().getName()))
                storeDef = d;
        if(storeDef == null)
            throw new BootstrapFailureException("Unknown store '" + getStore().getName() + "'.");

        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(((ConsistentRoutingStrategy) getRoutingStrategy()).getHashFunction(),
                                                                        cluster.getNodes(),
                                                                        storeDef.getReplicationFactor());
        return routingStrategy;
    }
}
