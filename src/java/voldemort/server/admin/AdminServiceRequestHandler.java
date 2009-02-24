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

package voldemort.server.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.UnableUpdateMetadataException;
import voldemort.server.VoldemortServer;
import voldemort.server.VoldemortService;
import voldemort.store.Entry;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Responsible for interpreting and handling a Admin Request Stream
 * 
 * @author bbansal
 * 
 */
public class AdminServiceRequestHandler {

    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;
    private final ConcurrentMap<String, ? extends StorageEngine<byte[], byte[]>> storeMap;
    private final MetadataStore metadataStore;
    private final List<VoldemortService> serviceList;
    private final int nodeId;

    private ErrorCodeMapper errorMapper = new ErrorCodeMapper();

    public AdminServiceRequestHandler(ConcurrentMap<String, ? extends StorageEngine<byte[], byte[]>> storeEngineMap,
                                      DataInputStream inputStream,
                                      DataOutputStream outputStream,
                                      MetadataStore metadataStore,
                                      List<VoldemortService> serviceList,
                                      int nodeId) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.storeMap = storeEngineMap;
        this.metadataStore = metadataStore;
        this.serviceList = serviceList;
        this.nodeId = nodeId;
    }

    public void handleRequest() throws IOException {
        byte opCode = inputStream.readByte();
        StorageEngine<byte[], byte[]> engine;
        switch(opCode) {
            case VoldemortOpCode.GET_PARTITION_AS_STREAM_OP_CODE:
                engine = readStorageEngine();
                if(engine != null)
                    handleGetPartitionsAsStream(engine);
                break;
            case VoldemortOpCode.PUT_PARTITION_AS_STREAM_OP_CODE:
                engine = readStorageEngine();
                if(engine != null)
                    handlePutPartitionsAsStream(engine);
                break;
            case VoldemortOpCode.UPDATE_CLUSTER_METADATA_OP_CODE:
                handleUpdateClusterMetadataRequest();
                break;
            case VoldemortOpCode.UPDATE_STORES_METADATA_OP_CODE:
                handleUpdateStoresMetadataRequest();
                break;
            case VoldemortOpCode.RESTART_SERVICES_OP_CODE:
                handleRestartServicesRequest();
                break;
            case VoldemortOpCode.REBALANCING_SERVER_MODE_OP_CODE:
                handleRebalancingServerModeRequest();
                break;
            case VoldemortOpCode.NORMAL_SERVER_MODE_OP_CODE:
                handleNormalServerModeRequest();
                break;
            case VoldemortOpCode.REDIRECT_GET_OP_CODE:
                engine = readStorageEngine();
                byte[] key = readKey(inputStream);
                handleRedirectGetRequest(engine, key);
                break;
            default:
                throw new IOException("Unknown op code: " + opCode);
        }

        outputStream.flush();
    }

    private byte[] readKey(DataInputStream inputStream) throws IOException {
        int keySize = inputStream.readInt();
        byte[] key = new byte[keySize];
        ByteUtils.read(inputStream, key);

        return key;
    }

    private StorageEngine<byte[], byte[]> readStorageEngine() throws IOException {
        String storeName = inputStream.readUTF();
        StorageEngine<byte[], byte[]> engine = storeMap.get(storeName);
        if(engine == null) {
            writeException(outputStream, new VoldemortException("No store named '" + storeName
                                                                + "'."));
        }

        return engine;
    }

    /**
     * provides a way to write a batch of entries to a storageEngine. expects
     * format as
     * <p>
     * <code>KeyLength(int32) bytes(keyLength) valueLength(int32)
     * bytes(valueLength)</code>
     * <p>
     * <strong> Reads entries unless see a keyLength value of -1</strong>.
     * <p>
     * Possible usecases
     * <ul>
     * <li>data grandfathering</li>
     * <li>cluster rebalancing</li>
     * <li>nodes replication</li>
     * </ul>
     * 
     * @param engine
     * @throws IOException
     */
    private void handlePutPartitionsAsStream(StorageEngine<byte[], byte[]> engine)
            throws IOException {
        try {
            int keySize = inputStream.readInt();
            while(keySize != -1) {
                byte[] key = new byte[keySize];
                ByteUtils.read(inputStream, key);

                int valueSize = inputStream.readInt();
                byte[] value = new byte[valueSize];
                ByteUtils.read(inputStream, value);
                VectorClock clock = new VectorClock(value);
                Versioned<byte[]> versionedValue = new Versioned<byte[]>(ByteUtils.copy(value,
                                                                                        clock.sizeInBytes(),
                                                                                        value.length),
                                                                         clock);
                engine.put(key, versionedValue);

                keySize = inputStream.readInt(); // read next KeySize
            }
            // all puts are handled.
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    /**
     * provides a way to read batch entries from a storageEngine. expects an
     * integer list of partitions requested. writes back to dataStream in format
     * <p>
     * <code>KeyLength(int32) bytes(keyLength) valueLength(int32)
     * bytes(valueLength)</code>
     * <p>
     * <strong>Stream end is indicated by writing a keyLength value of -1</strong>.
     * <p>
     * Possible usecases
     * <ul>
     * <li>data grandfathering</li>
     * <li>cluster rebalancing</li>
     * <li>nodes replication</li>
     * </ul>
     * 
     * @param engine
     * @throws IOException
     */
    private void handleGetPartitionsAsStream(StorageEngine<byte[], byte[]> engine)
            throws IOException {
        // read partition List
        int partitionSize = inputStream.readInt();
        int[] partitionList = new int[partitionSize];
        for(int i = 0; i < partitionSize; i++) {
            partitionList[i] = inputStream.readInt();
        }

        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(metadataStore.getCluster()
                                                                                     .getNodes(),
                                                                        metadataStore.getStore(engine.getName())
                                                                                     .getReplicationFactor());
        try {
            /**
             * TODO HIGH: This way to iterate over all keys is not optimal
             * stores should be made routing aware to fix this problem
             */
            ClosableIterator<Entry<byte[], Versioned<byte[]>>> iterator = engine.entries();
            outputStream.writeShort(0);

            while(iterator.hasNext()) {
                Entry<byte[], Versioned<byte[]>> entry = iterator.next();
                if(validPartition(entry.getKey(), partitionList, routingStrategy)) {
                    // write key
                    byte[] key = entry.getKey();
                    outputStream.writeInt(key.length);
                    outputStream.write(key);

                    // write value
                    outputStream.writeInt(1);
                    byte[] clock = ((VectorClock) entry.getValue().getVersion()).toBytes();
                    byte[] value = entry.getValue().getValue();
                    outputStream.writeInt(clock.length + value.length);
                    outputStream.write(clock);
                    outputStream.write(value);
                }
            }
            // close the iterator here
            iterator.close();
            outputStream.writeInt(-1); // indicate that all keys are done
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    private void handleUpdateClusterMetadataRequest() throws IOException {
        String cluster_key = inputStream.readUTF();
        // get current ClusterInfo
        List<Versioned<byte[]>> clusterInfo = metadataStore.get(ByteUtils.getBytes(cluster_key,
                                                                                   "UTF-8"));
        if(clusterInfo.size() > 1) {
            throw new UnableUpdateMetadataException("Inconistent Cluster Metdata found on Server:"
                                                    + nodeId + " for Key:" + cluster_key);
        }
        System.out.println("Cluster metadata  update called " + cluster_key);
        // update version
        VectorClock updatedVersion = new VectorClock();
        if(clusterInfo.size() > 0) {
            updatedVersion = ((VectorClock) clusterInfo.get(0).getVersion());
        }

        updatedVersion.incrementVersion(nodeId, System.currentTimeMillis());

        try {
            String clusterString = inputStream.readUTF();

            Cluster updatedCluster = new ClusterMapper().readCluster(new StringReader(clusterString));

            // update cluster details in metaDataStore
            metadataStore.put(ByteUtils.getBytes(cluster_key, "UTF-8"),
                              new Versioned<byte[]>(ByteUtils.getBytes(new ClusterMapper().writeCluster(updatedCluster),
                                                                       "UTF-8"),
                                                    updatedVersion));
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }

    }

    private void handleUpdateStoresMetadataRequest() throws IOException {
        // get current Store Info
        List<Versioned<byte[]>> storesInfo = metadataStore.get(ByteUtils.getBytes(MetadataStore.STORES_KEY,
                                                                                  "UTF-8"));
        if(storesInfo.size() > 1) {
            throw new UnableUpdateMetadataException("Inconistent Stores Metdata found on Server:"
                                                    + nodeId);
        }

        // update version
        VectorClock updatedVersion = ((VectorClock) storesInfo.get(0).getVersion());
        updatedVersion.incrementVersion(nodeId, System.currentTimeMillis());

        try {
            String storesString = inputStream.readUTF();

            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(new String(storesString)));

            // update cluster details in metaDataStore
            metadataStore.put(ByteUtils.getBytes(MetadataStore.STORES_KEY, "UTF-8"),
                              new Versioned<byte[]>(ByteUtils.getBytes(new StoreDefinitionsMapper().writeStoreList(storeDefs),
                                                                       "UTF-8"),
                                                    updatedVersion));
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
    }

    private void handleRestartServicesRequest() throws IOException {
        List<VoldemortException> exceptions = new ArrayList<VoldemortException>();

        try {
            for(VoldemortService service: serviceList) {
                service.stop();
                service.start();
            }
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
    }

    private void handleRebalancingServerModeRequest() throws IOException {
        try {
            List<Versioned<byte[]>> serverState = metadataStore.get(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                                       "UTF-8"));

            // update version
            VectorClock updatedVersion = new VectorClock();
            if(serverState.size() > 0) {
                updatedVersion = ((VectorClock) serverState.get(0).getVersion());
            }

            updatedVersion.incrementVersion(nodeId, System.currentTimeMillis());

            metadataStore.put(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY, "UTF-8"),
                              new Versioned<byte[]>(ByteUtils.getBytes(VoldemortServer.SERVER_STATE.REBALANCING_STATE.toString(),
                                                                       "UTF-8"),
                                                    updatedVersion));

            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }

    }

    private void handleNormalServerModeRequest() throws IOException {
        try {
            List<Versioned<byte[]>> serverState = metadataStore.get(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                                       "UTF-8"));

            // update version
            VectorClock updatedVersion = new VectorClock();
            if(serverState.size() > 0) {
                updatedVersion = ((VectorClock) serverState.get(0).getVersion());
            }
            updatedVersion.incrementVersion(nodeId, System.currentTimeMillis());

            metadataStore.put(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY, "UTF-8"),
                              new Versioned<byte[]>(ByteUtils.getBytes(VoldemortServer.SERVER_STATE.NORMAL_STATE.toString(),
                                                                       "UTF-8"),
                                                    updatedVersion));
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
    }

    private void handleRedirectGetRequest(StorageEngine engine, byte[] key) throws IOException {
        List<Versioned<byte[]>> results = null;
        try {
            results = engine.get(key);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
        outputStream.writeInt(results.size());
        for(Versioned<byte[]> v: results) {
            byte[] clock = ((VectorClock) v.getVersion()).toBytes();
            byte[] value = v.getValue();
            outputStream.writeInt(clock.length + value.length);
            outputStream.write(clock);
            outputStream.write(value);
        }
    }

    private void writeException(DataOutputStream stream, VoldemortException e) throws IOException {
        short code = errorMapper.getCode(e);
        stream.writeShort(code);
        stream.writeUTF(e.getMessage());
    }

    private boolean validPartition(byte[] key, int[] partitionList, RoutingStrategy routingStrategy) {
        List<Integer> keyPartitions = routingStrategy.getPartitionList(key);
        for(int p: partitionList) {
            if(keyPartitions.contains(new Integer(p))) {
                return true;
            }
        }
        return false;
    }
}
