package voldemort.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public interface WireFormat {

    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException;

    public void writeGetRequest(DataOutputStream output,
                                String storeName,
                                byte[] key,
                                boolean shouldReroute) throws IOException;

    public List<Versioned<byte[]>> readGetResponse(DataInputStream stream) throws IOException;

    public void writePutRequest(DataOutputStream output,
                                String storeName,
                                byte[] key,
                                byte[] value,
                                VectorClock version,
                                boolean shouldReroute) throws IOException;

    public void readPutResponse(DataInputStream stream) throws IOException;

    public void writeDeleteRequest(DataOutputStream output,
                                   String storeName,
                                   byte[] key,
                                   VectorClock version,
                                   boolean shouldReroute) throws IOException;

    public boolean readDeleteResponse(DataInputStream input) throws IOException;
}
