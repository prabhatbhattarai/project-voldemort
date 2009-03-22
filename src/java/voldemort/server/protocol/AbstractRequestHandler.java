package voldemort.server.protocol;

import java.util.Map;

import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

/**
 * A base class with a few helper fields for writing a
 * {@link voldemort.server.protocol.RequestHandler}
 * 
 * @author jay
 * 
 */
public abstract class AbstractRequestHandler implements RequestHandler {

    private final ErrorCodeMapper errorMapper;
    private final Map<String, ? extends Store<ByteArray, byte[]>> localStores;
    private final Map<String, ? extends Store<ByteArray, byte[]>> routedStores;

    protected AbstractRequestHandler(ErrorCodeMapper errorMapper,
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
