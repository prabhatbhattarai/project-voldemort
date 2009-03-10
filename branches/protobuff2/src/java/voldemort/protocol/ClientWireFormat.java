package voldemort.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public interface ClientWireFormat {

    public void writeGetRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                boolean shouldReroute) throws IOException;

    public List<Versioned<byte[]>> readGetResponse(DataInputStream stream) throws IOException;

    public void writeGetAllRequest(DataOutputStream output,
                                   String storeName,
                                   Iterable<ByteArray> key,
                                   boolean shouldReroute) throws IOException;

    public Map<ByteArray, List<Versioned<byte[]>>> readGetAllResponse(DataInputStream stream)
            throws IOException;

    public void writePutRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                byte[] value,
                                VectorClock version,
                                boolean shouldReroute) throws IOException;

    public void readPutResponse(DataInputStream stream) throws IOException;

    public void writeDeleteRequest(DataOutputStream output,
                                   String storeName,
                                   ByteArray key,
                                   VectorClock version,
                                   boolean shouldReroute) throws IOException;

    public boolean readDeleteResponse(DataInputStream input) throws IOException;
}
