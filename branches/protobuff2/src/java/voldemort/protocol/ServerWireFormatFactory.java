package voldemort.protocol;

import java.util.Map;

import voldemort.VoldemortException;
import voldemort.protocol.vold.VoldemortNativeServerWireFormat;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

public class ServerWireFormatFactory {

    private final Map<String, ? extends Store<ByteArray, byte[]>> localStores;
    private final Map<String, ? extends Store<ByteArray, byte[]>> routedStores;

    public ServerWireFormatFactory(Map<String, ? extends Store<ByteArray, byte[]>> localStores,
                                   Map<String, ? extends Store<ByteArray, byte[]>> routedStores) {
        this.localStores = localStores;
        this.routedStores = routedStores;
    }

    public ServerWireFormat getWireFormat(WireFormatType type) {
        switch(type) {
            case VOLDEMORT:
                return new VoldemortNativeServerWireFormat(new ErrorCodeMapper(),
                                                           localStores,
                                                           routedStores);
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }

}
