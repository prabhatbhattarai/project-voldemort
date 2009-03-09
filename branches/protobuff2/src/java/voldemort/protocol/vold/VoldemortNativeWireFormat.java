package voldemort.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.protocol.AbstractWireFormat;
import voldemort.serialization.VoldemortOpCode;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * A low-overhead custom binary protocol
 * 
 * @author jay
 * 
 */
public class VoldemortNativeWireFormat extends AbstractWireFormat {

    public VoldemortNativeWireFormat(Map<String, ? extends Store<byte[], byte[]>> localStores,
                                     Map<String, ? extends Store<byte[], byte[]>> routedStores) {
        super(new ErrorCodeMapper(), localStores, routedStores);
    }

    public boolean readDeleteResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        return inputStream.readBoolean();
    }

    public List<Versioned<byte[]>> readGetResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        int resultSize = inputStream.readInt();
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>(resultSize);
        for(int i = 0; i < resultSize; i++) {
            int valueSize = inputStream.readInt();
            byte[] bytes = new byte[valueSize];
            ByteUtils.read(inputStream, bytes);
            VectorClock clock = new VectorClock(bytes);
            results.add(new Versioned<byte[]>(ByteUtils.copy(bytes,
                                                             clock.sizeInBytes(),
                                                             bytes.length), clock));
        }
        return results;
    }

    public void readPutResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
    }

    public void writeDeleteRequest(DataOutputStream outputStream,
                                   String storeName,
                                   byte[] key,
                                   VectorClock version,
                                   boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        outputStream.writeByte(VoldemortOpCode.DELETE_OP_CODE);
        outputStream.writeUTF(storeName);
        outputStream.writeBoolean(shouldReroute);
        outputStream.writeInt(key.length);
        outputStream.write(key);
        VectorClock clock = (VectorClock) version;
        outputStream.writeShort(clock.sizeInBytes());
        outputStream.write(clock.toBytes());
    }

    public void writeGetRequest(DataOutputStream outputStream,
                                String storeName,
                                byte[] key,
                                boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        outputStream.writeByte(VoldemortOpCode.GET_OP_CODE);
        outputStream.writeUTF(storeName);
        outputStream.writeBoolean(shouldReroute);
        outputStream.writeInt(key.length);
        outputStream.write(key);
    }

    public void writePutRequest(DataOutputStream outputStream,
                                String storeName,
                                byte[] key,
                                byte[] value,
                                VectorClock version,
                                boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        outputStream.writeByte(VoldemortOpCode.PUT_OP_CODE);
        outputStream.writeUTF(storeName);
        outputStream.writeBoolean(shouldReroute);
        outputStream.writeInt(key.length);
        outputStream.write(key);
        outputStream.writeInt(value.length + version.sizeInBytes());
        outputStream.write(version.toBytes());
        outputStream.write(value);
    }

    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException {
        byte opCode = inputStream.readByte();
        String storeName = inputStream.readUTF();
        boolean isRouted = inputStream.readBoolean();
        int keySize = inputStream.readInt();
        byte[] key = new byte[keySize];
        ByteUtils.read(inputStream, key);
        Store<byte[], byte[]> store = getStore(storeName, isRouted);
        if(store == null) {
            writeException(outputStream, new VoldemortException("No store named '" + storeName
                                                                + "'."));
        } else {
            switch(opCode) {
                case VoldemortOpCode.GET_OP_CODE:
                    handleGet(inputStream, outputStream, store, key);
                    break;
                case VoldemortOpCode.PUT_OP_CODE:
                    handlePut(inputStream, outputStream, store, key);
                    break;
                case VoldemortOpCode.DELETE_OP_CODE:
                    handleDelete(inputStream, outputStream, store, key);
                    break;
                default:
                    throw new IOException("Unknown op code: " + opCode);
            }
        }
        outputStream.flush();
    }

    private void handleGet(DataInputStream inputStream,
                           DataOutputStream outputStream,
                           Store<byte[], byte[]> store,
                           byte[] key) throws IOException {
        List<Versioned<byte[]>> results = null;
        try {
            results = store.get(key);
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

    private void handlePut(DataInputStream inputStream,
                           DataOutputStream outputStream,
                           Store<byte[], byte[]> store,
                           byte[] key) throws IOException {
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

    private void handleDelete(DataInputStream inputStream,
                              DataOutputStream outputStream,
                              Store<byte[], byte[]> store,
                              byte[] key) throws IOException {
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
        short code = getErrorMapper().getCode(e);
        stream.writeShort(code);
        stream.writeUTF(e.getMessage());
    }

    private Store<byte[], byte[]> getStore(String name, boolean isRouted) {
        if(isRouted)
            return getRoutedStores().get(name);
        else
            return getLocalStores().get(name);
    }

    private void checkException(DataInputStream inputStream) throws IOException {
        short retCode = inputStream.readShort();
        if(retCode != 0) {
            String error = inputStream.readUTF();
            throw getErrorMapper().getError(retCode, error);
        }
    }

}
