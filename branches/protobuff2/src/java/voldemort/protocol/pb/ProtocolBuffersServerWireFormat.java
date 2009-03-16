package voldemort.protocol.pb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.protocol.AbstractServerWireFormat;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.protobuf.ByteString;

/**
 * 
 * 
 * @author jay
 * 
 */
public class ProtocolBuffersServerWireFormat extends AbstractServerWireFormat {

    public ProtocolBuffersServerWireFormat(ErrorCodeMapper errorMapper,
                                           Map<String, ? extends Store<ByteArray, byte[]>> localStoreMap,
                                           Map<String, ? extends Store<ByteArray, byte[]>> routedStoreMap) {
        super(errorMapper, localStoreMap, routedStoreMap);
    }

    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException {
        VoldemortProtocol.VoldemortRequest request = VoldemortProtocol.VoldemortRequest.parseFrom(inputStream);
        boolean shouldRoute = request.getShouldRoute();
        String storeName = request.getStore();
        Store<ByteArray, byte[]> store = getStore(storeName, shouldRoute);
        switch(request.getType()) {
            case GET:
                handleGet(request.getGet(), store, inputStream, outputStream);
                break;
            case GET_ALL:
                handleGetAll(request.getGetAll(), store, inputStream, outputStream);
                break;
            case PUT:
                handlePut(request.getPut(), store, inputStream, outputStream);
                break;
            case DELETE:
                handleDelete(request.getDelete(), store, inputStream, outputStream);
                break;
        }
    }

    private void handleGet(VoldemortProtocol.GetRequest request,
                           Store<ByteArray, byte[]> store,
                           DataInputStream inputStream,
                           DataOutputStream outputStream) throws IOException {
        VoldemortProtocol.GetResponse.Builder response = VoldemortProtocol.GetResponse.newBuilder();
        try {
            List<Versioned<byte[]>> values = store.get(PbUtils.decodeBytes(request.getKey()));
            for(Versioned<byte[]> versioned: values)
                response.addVersioned(PbUtils.encodeVersioned(versioned));
        } catch(VoldemortException e) {
            response.setError(PbUtils.encodeError(getErrorMapper(), e));
        }
        response.build().writeTo(outputStream);
    }

    private void handleGetAll(VoldemortProtocol.GetAllRequest request,
                              Store<ByteArray, byte[]> store,
                              DataInputStream inputStream,
                              DataOutputStream outputStream) throws IOException {
        VoldemortProtocol.GetAllResponse.Builder response = VoldemortProtocol.GetAllResponse.newBuilder();
        try {
            List<ByteArray> keys = new ArrayList<ByteArray>(request.getKeysCount());
            for(ByteString string: request.getKeysList())
                keys.add(PbUtils.decodeBytes(string));
            Map<ByteArray, List<Versioned<byte[]>>> values = store.getAll(keys);
            for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: values.entrySet()) {
                VoldemortProtocol.KeyedVersions.Builder keyedVersion = VoldemortProtocol.KeyedVersions.newBuilder()
                                                                                                      .setKey(PbUtils.encodeBytes(entry.getKey()));
                for(Versioned<byte[]> version: entry.getValue())
                    keyedVersion.addVersions(PbUtils.encodeVersioned(version));
                response.addValues(keyedVersion);
            }
        } catch(VoldemortException e) {
            response.setError(PbUtils.encodeError(getErrorMapper(), e));
        }
        response.build().writeTo(outputStream);
    }

    private void handlePut(VoldemortProtocol.PutRequest request,
                           Store<ByteArray, byte[]> store,
                           DataInputStream inputStream,
                           DataOutputStream outputStream) throws IOException {
        VoldemortProtocol.PutResponse.Builder response = VoldemortProtocol.PutResponse.newBuilder();
        try {
            ByteArray key = PbUtils.decodeBytes(request.getKey());
            Versioned<byte[]> value = PbUtils.decodeVersioned(request.getVersioned());
            store.put(key, value);
        } catch(VoldemortException e) {
            response.setError(PbUtils.encodeError(getErrorMapper(), e));
        }
        response.build().writeTo(outputStream);
    }

    private void handleDelete(VoldemortProtocol.DeleteRequest request,
                              Store<ByteArray, byte[]> store,
                              DataInputStream inputStream,
                              DataOutputStream outputStream) throws IOException {
        VoldemortProtocol.DeleteResponse.Builder response = VoldemortProtocol.DeleteResponse.newBuilder();
        try {
            boolean success = store.delete(PbUtils.decodeBytes(request.getKey()),
                                           PbUtils.decodeClock(request.getVersion()));
            response.setSuccess(success);
        } catch(VoldemortException e) {
            response.setSuccess(false);
            response.setError(PbUtils.encodeError(getErrorMapper(), e));
        }
        response.build().writeTo(outputStream);
    }

}
