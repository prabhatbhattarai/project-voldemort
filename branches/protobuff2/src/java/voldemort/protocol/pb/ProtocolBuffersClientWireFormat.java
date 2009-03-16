package voldemort.protocol.pb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.protocol.ClientWireFormat;
import voldemort.protocol.pb.VoldemortProtocol.RequestType;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.protobuf.ByteString;

/**
 * Wire format for protocol buffers protocol
 * 
 * @author jkreps
 * 
 */
public class ProtocolBuffersClientWireFormat implements ClientWireFormat {

    public final ErrorCodeMapper mapper;

    public ProtocolBuffersClientWireFormat() {
        this.mapper = new ErrorCodeMapper();
    }

    public void writeDeleteRequest(DataOutputStream output,
                                   String storeName,
                                   ByteArray key,
                                   VectorClock version,
                                   boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        VoldemortProtocol.VoldemortRequest.newBuilder()
                                          .setType(RequestType.DELETE)
                                          .setStore(storeName)
                                          .setShouldRoute(shouldReroute)
                                          .setDelete(VoldemortProtocol.DeleteRequest.newBuilder()
                                                                                    .setKey(ByteString.copyFrom(key.get()))
                                                                                    .setVersion(PbUtils.encodeClock(version)))
                                          .build()
                                          .writeTo(output);
    }

    public boolean readDeleteResponse(DataInputStream input) throws IOException {
        VoldemortProtocol.DeleteResponse response = VoldemortProtocol.DeleteResponse.parseFrom(input);
        if(response.hasError())
            throwException(response.getError());
        return response.getSuccess();
    }

    public void writeGetRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        VoldemortProtocol.VoldemortRequest.newBuilder()
                                          .setType(RequestType.GET)
                                          .setStore(storeName)
                                          .setShouldRoute(shouldReroute)
                                          .setGet(VoldemortProtocol.GetRequest.newBuilder()
                                                                              .setKey(ByteString.copyFrom(key.get())))
                                          .build()
                                          .writeTo(output);
    }

    public List<Versioned<byte[]>> readGetResponse(DataInputStream input) throws IOException {
        VoldemortProtocol.GetResponse response = VoldemortProtocol.GetResponse.parseFrom(input);
        if(response.hasError())
            throwException(response.getError());
        return PbUtils.decodeVersions(response.getVersionedList());
    }

    public void writeGetAllRequest(DataOutputStream output,
                                   String storeName,
                                   Iterable<ByteArray> keys,
                                   boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKeys(keys);
        VoldemortProtocol.GetAllRequest.Builder req = VoldemortProtocol.GetAllRequest.newBuilder();
        for(ByteArray key: keys)
            req.addKeys(ByteString.copyFrom(key.get()));

        VoldemortProtocol.VoldemortRequest.newBuilder()
                                          .setType(RequestType.GET_ALL)
                                          .setStore(storeName)
                                          .setShouldRoute(shouldReroute)
                                          .setGetAll(req)
                                          .build()
                                          .writeTo(output);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> readGetAllResponse(DataInputStream input)
            throws IOException {
        VoldemortProtocol.GetAllResponse response = VoldemortProtocol.GetAllResponse.parseFrom(input);
        if(response.hasError())
            throwException(response.getError());
        Map<ByteArray, List<Versioned<byte[]>>> vals = new HashMap<ByteArray, List<Versioned<byte[]>>>(response.getValuesCount());
        for(VoldemortProtocol.KeyedVersions versions: response.getValuesList())
            vals.put(PbUtils.decodeBytes(versions.getKey()),
                     PbUtils.decodeVersions(versions.getVersionsList()));
        return vals;
    }

    public void writePutRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                byte[] value,
                                VectorClock version,
                                boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        VoldemortProtocol.PutRequest.Builder req = VoldemortProtocol.PutRequest.newBuilder()
                                                                               .setKey(ByteString.copyFrom(key.get()))
                                                                               .setVersioned(VoldemortProtocol.Versioned.newBuilder()
                                                                                                                        .setValue(ByteString.copyFrom(value))
                                                                                                                        .setVersion(PbUtils.encodeClock(version)));
        VoldemortProtocol.VoldemortRequest.newBuilder()
                                          .setType(RequestType.PUT)
                                          .setStore(storeName)
                                          .setShouldRoute(shouldReroute)
                                          .setPut(req)
                                          .build()
                                          .writeTo(output);
    }

    public void readPutResponse(DataInputStream input) throws IOException {
        VoldemortProtocol.PutResponse response = VoldemortProtocol.PutResponse.parseFrom(input);
        if(response.hasError())
            throwException(response.getError());
    }

    public void throwException(VoldemortProtocol.Error error) {
        throw mapper.getError((short) error.getErrorCode(), error.getErrorMessage());
    }

}
