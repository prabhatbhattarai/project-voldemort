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

package voldemort.server.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import voldemort.VoldemortException;
import voldemort.client.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.VoldemortServer;
import voldemort.server.VoldemortServer.SERVER_STATE;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

/**
 * Responsible for interpreting and handling a single request stream
 * 
 * @author jay
 * 
 */
public class StreamStoreRequestHandler {

    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;
    private final ConcurrentMap<String, ? extends Store<ByteArray, byte[]>> storeMap;
    private VoldemortServer.SERVER_STATE state = SERVER_STATE.NORMAL_STATE;
    private final MetadataStore metadataStore;
    private final Cluster oldCluster;
    private final Cluster updatedCluster;
    private final List<StoreDefinition> storeDefs;
    private final AdminClient adminClient;
    private final int nodeId;

    private ErrorCodeMapper errorMapper = new ErrorCodeMapper();

    public StreamStoreRequestHandler(ConcurrentMap<String, ? extends Store<ByteArray, byte[]>> storeMap,
                                     DataInputStream inputStream,
                                     DataOutputStream outputStream,
                                     int socketBufferSize,
                                     MetadataStore metadataStore,
                                     int nodeId) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.storeMap = storeMap;
        this.metadataStore = metadataStore;
        this.nodeId = nodeId;

        if(null != metadataStore) {
            List<Versioned<byte[]>> values = metadataStore.get(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                                  "UTF-8"));
            if(values.size() > 0) {
                String stateString = new String(values.get(0).getValue());
                this.state = SERVER_STATE.valueOf(stateString);
            }
            storeDefs = metadataStore.getStores();
            updatedCluster = metadataStore.getCluster();
            adminClient = new AdminClient(updatedCluster.getNodeById(nodeId),
                                          metadataStore,
                                          new SocketPool(10,
                                                         10 * updatedCluster.getNumberOfNodes(),
                                                         2000,
                                                         socketBufferSize));
        } else {
            updatedCluster = null;
            adminClient = null;
            storeDefs = null;
        }

        if(SERVER_STATE.REBALANCING_STATE.equals(state)) {
            List<Versioned<byte[]>> oldClusterInfo = metadataStore.get(ByteUtils.getBytes(MetadataStore.OLD_CLUSTER_KEY,
                                                                                          "UTF-8"));
            if(oldClusterInfo.size() != 1) {
                throw new VoldemortException("handleGetRebalance : Failed to read REBALANCED_CLUSTER_KEY correctly");
            }
            oldCluster = new ClusterMapper().readCluster(new StringReader(new String(oldClusterInfo.get(0)
                                                                                                   .getValue())));
        } else {
            // FOR NORMAL state no need for these
            oldCluster = null;
        }
    }

    public void handleRequest() throws IOException {
        byte opCode = inputStream.readByte();
        String storeName = inputStream.readUTF();
        Store<ByteArray, byte[]> store = storeMap.get(storeName);
        ByteArray key;
        if(store == null) {
            writeException(outputStream, new VoldemortException("No store named '" + storeName
                                                                + "'."));
        } else {
            switch(opCode) {
                case VoldemortOpCode.GET_OP_CODE:
                    key = readKey(inputStream);
                    handleGet(store, key);
                    break;
                case VoldemortOpCode.PUT_OP_CODE:
                    key = readKey(inputStream);
                    handlePut(store, key);
                    break;
                case VoldemortOpCode.DELETE_OP_CODE:
                    key = readKey(inputStream);
                    handleDelete(store, key);
                    break;
                default:
                    throw new IOException("Unknown op code: " + opCode);
            }
        }
        outputStream.flush();
    }

    private ByteArray readKey(DataInputStream inputStream) throws IOException {
        int keySize = inputStream.readInt();
        byte[] key = new byte[keySize];
        ByteUtils.read(inputStream, key);

        return new ByteArray(key);
    }

    /**
     * handle REBALANCING_STATE need to redirect get to original cluster
     * configuration
     * 
     * @param store
     * @param key
     * @throws IOException
     */
    private void handleGet(Store<ByteArray, byte[]> store, ByteArray key) throws IOException {
        List<Versioned<byte[]>> results = null;
        try {
            results = doGet(store, key);
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

    private List<Versioned<byte[]>> doGet(Store<ByteArray, byte[]> store, ByteArray key)
            throws IOException {
        if(VoldemortServer.SERVER_STATE.REBALANCING_STATE.equals(state)) {
            return doGetRebalancingState(store, key);
        }
        return doGetNormalState(store, key);
    }

    private List<Versioned<byte[]>> doGetNormalState(Store<ByteArray, byte[]> store, ByteArray key) {
        return store.get(key);
    }

    /**
     * 
     * @param store
     * @param key
     * @return
     */
    private List<Versioned<byte[]>> doGetRebalancingState(Store<ByteArray, byte[]> store,
                                                          ByteArray key) {
        if(null == metadataStore) {
            throw new VoldemortException("doGetRebalancingStore needs a non null MetaDataStore.");
        }
        int repFactor = getReplicationFactor(store.getName());
        List<Node> originalNodeList = new ConsistentRoutingStrategy(oldCluster.getNodes(),
                                                                    repFactor).routeRequest(key.get());
        List<Node> updatedNodeList = new ConsistentRoutingStrategy(updatedCluster.getNodes(),
                                                                   repFactor).routeRequest(key.get());
        // find the nodeId present in original and missing in updated list
        int redirectedNodeId = -1;
        for(Node node: originalNodeList) {
            if(!updatedNodeList.contains(node)) {
                redirectedNodeId = node.getId();
                break;
            }
        }

        // redirect request to redirected Node now
        if(-1 != redirectedNodeId) {
            return adminClient.redirectGet(redirectedNodeId, store.getName(), key);
        }
        return new ArrayList<Versioned<byte[]>>();
    }

    private int getReplicationFactor(String storeName) {
        for(StoreDefinition def: storeDefs) {
            if(storeName.equals(def.getName())) {
                return def.getReplicationFactor();
            }
        }
        throw new VoldemortException("Store not found in saved storeDefs");
    }

    private void handlePut(Store<ByteArray, byte[]> store, ByteArray key) throws IOException {
        int valueSize = inputStream.readInt();
        byte[] bytes = new byte[valueSize];
        ByteUtils.read(inputStream, bytes);
        VectorClock clock = new VectorClock(bytes);
        byte[] value = ByteUtils.copy(bytes, clock.sizeInBytes(), bytes.length);
        try {
            store.put(key, new Versioned<byte[]>(value, clock));
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    private void handleDelete(Store<ByteArray, byte[]> store, ByteArray key) throws IOException {
        int versionSize = inputStream.readShort();
        byte[] versionBytes = new byte[versionSize];
        ByteUtils.read(inputStream, versionBytes);
        VectorClock version = new VectorClock(versionBytes);
        try {
            boolean succeeded = store.delete(key, version);
            outputStream.writeShort(0);
            outputStream.writeBoolean(succeeded);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    private void writeException(DataOutputStream stream, VoldemortException e) throws IOException {
        short code = errorMapper.getCode(e);
        stream.writeShort(code);
        stream.writeUTF(e.getMessage());
    }
}
