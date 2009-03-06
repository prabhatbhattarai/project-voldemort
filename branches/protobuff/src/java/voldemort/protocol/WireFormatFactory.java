package voldemort.protocol;

import java.util.Map;

import voldemort.VoldemortException;
import voldemort.protocol.vold.VoldemortNativeWireFormat;
import voldemort.store.Store;

public class WireFormatFactory {

    private final Map<String, ? extends Store<byte[], byte[]>> localStores;
    private final Map<String, ? extends Store<byte[], byte[]>> routedStores;

    public WireFormatFactory(Map<String, ? extends Store<byte[], byte[]>> localStores,
                             Map<String, ? extends Store<byte[], byte[]>> routedStores) {
        this.localStores = localStores;
        this.routedStores = routedStores;
    }

    public WireFormat getWireFormat(WireFormatType type) {
        switch(type) {
            case VOLDEMORT:
                return new VoldemortNativeWireFormat(localStores, routedStores);
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }

}
