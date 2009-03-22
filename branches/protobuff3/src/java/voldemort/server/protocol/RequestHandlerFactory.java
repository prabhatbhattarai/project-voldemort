package voldemort.server.protocol;

import java.util.Map;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.protocol.pb.ProtoBuffRequestHandler;
import voldemort.server.protocol.vold.VoldemortNativeRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

/**
 * A factory that gets the appropriate request handler for a given
 * {@link voldemort.client.RequestFormatType}.
 * 
 * @author jay
 * 
 */
public class RequestHandlerFactory {

    private final Map<String, ? extends Store<ByteArray, byte[]>> localStores;
    private final Map<String, ? extends Store<ByteArray, byte[]>> routedStores;

    public RequestHandlerFactory(Map<String, ? extends Store<ByteArray, byte[]>> localStores,
                                 Map<String, ? extends Store<ByteArray, byte[]>> routedStores) {
        this.localStores = localStores;
        this.routedStores = routedStores;
    }

    public RequestHandler getRequestHandler(RequestFormatType type) {
        switch(type) {
            case VOLDEMORT:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(),
                                                         localStores,
                                                         routedStores);
            case PROTOCOL_BUFFERS:
                return new ProtoBuffRequestHandler(new ErrorCodeMapper(), localStores, routedStores);
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }

}
