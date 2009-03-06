package voldemort.protocol;

import java.util.Map;

import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;

public abstract class AbstractWireFormat implements WireFormat {

    private final ErrorCodeMapper errorMapper;
    private final Map<String, ? extends Store<byte[], byte[]>> localStores;
    private final Map<String, ? extends Store<byte[], byte[]>> routedStores;

    protected AbstractWireFormat(ErrorCodeMapper errorMapper,
                                 Map<String, ? extends Store<byte[], byte[]>> localStoreMap,
                                 Map<String, ? extends Store<byte[], byte[]>> routedStoreMap) {
        this.errorMapper = errorMapper;
        this.localStores = localStoreMap;
        this.routedStores = routedStoreMap;
    }

    protected ErrorCodeMapper getErrorMapper() {
        return errorMapper;
    }

    protected Map<String, ? extends Store<byte[], byte[]>> getLocalStores() {
        return localStores;
    }

    protected Map<String, ? extends Store<byte[], byte[]>> getRoutedStores() {
        return routedStores;
    }

}
