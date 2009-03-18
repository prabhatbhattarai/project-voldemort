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

package voldemort.store.socket;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.protocol.ClientWireFormat;
import voldemort.protocol.ClientWireFormatFactory;
import voldemort.protocol.WireFormatType;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The client implementation of a socket store--tranlates each request into a
 * network operation to be handled by the socket server on the other side.
 * 
 * @author jay
 * 
 */
public class SocketStore implements Store<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(SocketStore.class);

    private final ClientWireFormatFactory wireFormatFactory = new ClientWireFormatFactory();

    private final String name;
    private final SocketPool pool;
    private final SocketDestination destination;
    private final ClientWireFormat wireFormat;
    private final boolean reroute;

    public SocketStore(String name,
                       String host,
                       int port,
                       SocketPool socketPool,
                       WireFormatType wireFormatType,
                       boolean reroute) {
        this.name = Utils.notNull(name);
        this.pool = Utils.notNull(socketPool);
        this.destination = new SocketDestination(Utils.notNull(host), port);
        this.wireFormat = wireFormatFactory.getWireFormat(wireFormatType);
        this.reroute = reroute;
    }

    public void close() throws VoldemortException {
    // don't close the socket pool, it is shared
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            wireFormat.writeDeleteRequest(sands.getOutputStream(),
                                          name,
                                          key,
                                          (VectorClock) version,
                                          reroute);
            sands.getOutputStream().flush();
            return wireFormat.readDeleteResponse(sands.getInputStream());
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            wireFormat.writeGetAllRequest(sands.getOutputStream(), name, keys, reroute);
            sands.getOutputStream().flush();
            return wireFormat.readGetAllResponse(sands.getInputStream());
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            wireFormat.writeGetRequest(sands.getOutputStream(), name, key, reroute);
            sands.getOutputStream().flush();
            return wireFormat.readGetResponse(sands.getInputStream());
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public void put(ByteArray key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            wireFormat.writePutRequest(sands.getOutputStream(),
                                       name,
                                       key,
                                       versioned.getValue(),
                                       (VectorClock) versioned.getVersion(),
                                       reroute);
            sands.getOutputStream().flush();
            wireFormat.readPutResponse(sands.getInputStream());
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public String getName() {
        return name;
    }

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }
}
