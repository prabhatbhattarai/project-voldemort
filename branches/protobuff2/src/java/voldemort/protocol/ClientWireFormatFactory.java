package voldemort.protocol;

import voldemort.VoldemortException;
import voldemort.protocol.pb.ProtocolBuffersClientWireFormat;
import voldemort.protocol.vold.VoldemortNativeClientWireFormat;

public class ClientWireFormatFactory {

    public ClientWireFormat getWireFormat(WireFormatType type) {
        switch(type) {
            case VOLDEMORT:
                return new VoldemortNativeClientWireFormat();
            case PROTOCOL_BUFFERS:
                return new ProtocolBuffersClientWireFormat();
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }

}
