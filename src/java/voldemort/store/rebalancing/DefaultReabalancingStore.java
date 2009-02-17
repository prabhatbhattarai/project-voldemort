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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.store.DelegatingStore;
import voldemort.store.Entry;
import voldemort.store.RebalancingStore;
import voldemort.store.StorageEngine;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * A {@link DefaultReabalancingStore} store is a store wrapper that implements
 * {@link RebalancingStore} to provide <strong>Default<strong> support for
 * partition rebalancing.
 * 
 * @author bbansal
 * 
 */
public class DefaultReabalancingStore extends DelegatingStore<byte[], byte[]> implements
        RebalancingStore {

    private final int node;
    private final RoutingStrategy routingStratgey;
    private final StorageEngine<byte[], byte[]> innerEngine;
    private final int bufferSize;
    private final ExecutorService executor;

    /**
     * BufferSize is used to set buffer size for all input/output streams total
     * memory consumption can be as high as
     * <p>
     * <strong>numParallelStreams * 2 * bufferSize<strong>
     * <p>
     * 
     * @param node
     * @param innerStore
     * @param routingStrategy
     * @param bufferSize
     */
    public DefaultReabalancingStore(int node,
                                    StorageEngine<byte[], byte[]> innerStore,
                                    RoutingStrategy routingStrategy,
                                    int bufferSize,
                                    int numParallelStreams) {
        super(innerStore);
        this.innerEngine = innerStore;
        this.node = node;
        this.routingStratgey = routingStrategy;
        this.bufferSize = bufferSize;
        this.executor = Executors.newFixedThreadPool(numParallelStreams);
    }

    public RoutingStrategy getRoutingStrategy() {
        return routingStratgey;
    }

    /**
     * provides default implementation for getPartitionsAsStream. Iterate over
     * all keys in the store and filters out keys for partition requested.
     * Individual StorageEngines are encouraged to provide better ways to get
     * partitions.
     */
    public DataInputStream getPartitionsAsStream(final int[] partitionList) {
        DataInputStream din = null;
        try {
            PipedInputStream pin = new PipedInputStream();
            PipedOutputStream pos = new PipedOutputStream(pin);
            din = new DataInputStream(new BufferedInputStream(pin, bufferSize));

            final ClosableIterator<Entry<byte[], Versioned<byte[]>>> iterator = innerEngine.entries();
            final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(pos,
                                                                                       bufferSize));
            executor.execute(new Runnable() {

                public void run() {
                    try {
                        while(iterator.hasNext()) {
                            Entry<byte[], Versioned<byte[]>> entry = iterator.next();
                            if(validPartition(entry.getKey(), partitionList)) {
                                // write key
                                byte[] key = entry.getKey();
                                dos.writeInt(key.length);
                                dos.write(key);

                                // write value
                                /**
                                 * TODO bbansal: why iterator retuns a single
                                 * value and get a List ??
                                 */
                                dos.writeInt(1); // TODO : check this again
                                byte[] clock = ((VectorClock) entry.getValue().getVersion()).toBytes();
                                byte[] value = entry.getValue().getValue();
                                dos.writeInt(clock.length + value.length);
                                dos.write(clock);
                                dos.write(value);
                            }
                        }
                        dos.writeInt(-1); // indicate that all keys are done
                    } catch(IOException e) {
                        throw new VoldemortException("Exception while getPartitionAsStream.", e);
                    }
                }

                private boolean validPartition(byte[] key, int[] partitionList) {
                    List<Integer> keyPartitions = routingStratgey.getPartitionList(key);
                    for(int p: partitionList) {
                        if(keyPartitions.contains(new Integer(p))) {
                            return true;
                        }
                    }
                    return false;
                }
            });
        } catch(IOException e) {
            throw new VoldemortException("IOException while using pipedInput/Output Stream");
        }
        return din;
    }

    /**
     * copies bunch of code from {@link SocketStore#get(byte[])} to read
     * streamed value data.
     */
    public void putPartitionsAsStream(DataInputStream inputStream) {
        try {
            final DataInputStream din = new DataInputStream(new BufferedInputStream(inputStream,
                                                                                    bufferSize));
            executor.execute(new Runnable() {

                public void run() {
                    try {
                        int keySize = din.readInt();
                        while(keySize != -1) {
                            byte[] key = new byte[keySize];
                            ByteUtils.read(din, key);

                            int valueSize = din.readInt();
                            byte[] value = new byte[valueSize];
                            ByteUtils.read(din, value);
                            VectorClock clock = new VectorClock(value);
                            Versioned<byte[]> versionedValue = new Versioned<byte[]>(ByteUtils.copy(value,
                                                                                                    clock.sizeInBytes(),
                                                                                                    value.length),
                                                                                     clock);
                            put(key, versionedValue);

                            keySize = din.readInt(); // read next KeySize
                        }
                    } catch(IOException e) {
                        throw new VoldemortException("Exception while putPartitionAsStream.", e);
                    }
                }
            });
        } catch(Exception e) {
            throw new VoldemortException("IOException while using pipedInput/Output Stream");
        }
    }

    public int getBufferSize() {
        return bufferSize;
    }
}
