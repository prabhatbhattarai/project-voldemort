package voldemort.protocol;

import voldemort.VoldemortException;
import voldemort.protocol.vold.VoldemortNativeClientWireFormat;
import voldemort.store.ErrorCodeMapper;

public class ClientWireFormatFactory {

    public ClientWireFormat getWireFormat(WireFormatType type) {
        switch(type) {
            case VOLDEMORT:
                return new VoldemortNativeClientWireFormat(new ErrorCodeMapper());
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }

}
