package voldemort.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface ServerWireFormat {

    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException;

}
