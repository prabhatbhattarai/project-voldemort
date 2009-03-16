package voldemort.protocol;

import java.util.Map;

import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

public abstract class AbstractServerWireFormat implements ServerWireFormat {

    private final ErrorCodeMapper errorMapper;
    private final Map<String, ? extends Store<ByteArray, byte[]>> localStores;
    private final Map<String, ? extends Store<ByteArray, byte[]>> routedStores;

    protected AbstractServerWireFormat(ErrorCodeMapper errorMapper,
                                       Map<String, ? extends Store<ByteArray, byte[]>> localStoreMap,
                                       Map<String, ? extends Store<ByteArray, byte[]>> routedStoreMap) {
        this.errorMapper = errorMapper;
        this.localStores = localStoreMap;
        this.routedStores = routedStoreMap;
    }

    protected ErrorCodeMapper getErrorMapper() {
        return errorMapper;
    }

    protected Map<String, ? extends Store<ByteArray, byte[]>> getLocalStores() {
        return localStores;
    }

    protected Map<String, ? extends Store<ByteArray, byte[]>> getRoutedStores() {
        return routedStores;
    }

    protected Store<ByteArray, byte[]> getStore(String name, boolean isRouted) {
        if(isRouted)
            return getRoutedStores().get(name);
        else
            return getLocalStores().get(name);
    }

}
