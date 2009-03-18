package voldemort.server;

import java.util.concurrent.ConcurrentMap;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

/**
 * A wrapper class that holds all the server's stores--storage engines, routed
 * stores, and remote stores.
 * 
 * @author jay
 * 
 */
@Threadsafe
public class StoreRepository {

    /*
     * Unrouted stores, local to this machine
     */
    private final ConcurrentMap<String, Store<ByteArray, byte[]>> localStores;

    /*
     * Routed stores that write and read from multiple nodes
     */
    private final ConcurrentMap<String, Store<ByteArray, byte[]>> routedStores;

    /*
     * Stores that connect to a single node only and represent a direct
     * connection to the storage on that node
     */
    private final ConcurrentMap<String, Store<ByteArray, byte[]>>[] nodeStores;

    private StoreRepository(ConcurrentMap<String, Store<ByteArray, byte[]>> localStores,
                            ConcurrentMap<String, Store<ByteArray, byte[]>> routedStores,
                            ConcurrentMap<String, Store<ByteArray, byte[]>>[] nodeStores) {
        super();
        this.localStores = localStores;
        this.routedStores = routedStores;
        this.nodeStores = nodeStores;
    }

}
