package voldemort.server.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import voldemort.VoldemortException;
import voldemort.serialization.VoldemortOpCode;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * Responsible for interpreting and handling a single request stream
 * 
 * @author jay
 * 
 */
public class StreamStoreRequestHandler {

    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;
    private final ConcurrentMap<String, ? extends Store<byte[], byte[]>> storeMap;

    private ErrorCodeMapper errorMapper = new ErrorCodeMapper();

    public StreamStoreRequestHandler(ConcurrentMap<String, ? extends Store<byte[], byte[]>> storeMap,
                                     DataInputStream inputStream,
                                     DataOutputStream outputStream) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.storeMap = storeMap;
    }

    public void handleRequest() throws IOException {
        byte opCode = inputStream.readByte();
        String storeName = inputStream.readUTF();
        int keySize = inputStream.readInt();
        byte[] key = new byte[keySize];
        ByteUtils.read(inputStream, key);
        Store<byte[], byte[]> store = storeMap.get(storeName);
        if(store == null) {
            writeException(outputStream, new VoldemortException("No store named '" + storeName
                                                                + "'."));
        } else {
            switch(opCode) {
                case VoldemortOpCode.GET_OP_CODE:
                    handleGet(store, key);
                    break;
                case VoldemortOpCode.PUT_OP_CODE:
                    handlePut(store, key);
                    break;
                case VoldemortOpCode.DELETE_OP_CODE:
                    handleDelete(store, key);
                    break;
                default:
                    throw new IOException("Unknown op code: " + opCode);
            }
        }
        outputStream.flush();
    }

    private void handleGet(Store<byte[], byte[]> store, byte[] key) throws IOException {
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

    private void handlePut(Store<byte[], byte[]> store, byte[] key) throws IOException {
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

    private void handleDelete(Store<byte[], byte[]> store, byte[] key) throws IOException {
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