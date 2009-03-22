package voldemort.server;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;

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
    private final ConcurrentHashMap<Pair<String, Integer>, Store<ByteArray, byte[]>> nodeStores;

    private StoreRepository() {
        super();
        this.localStores = new ConcurrentHashMap<String, Store<ByteArray, byte[]>>();
        this.routedStores = new ConcurrentHashMap<String, Store<ByteArray, byte[]>>();
        this.nodeStores = new ConcurrentHashMap<Pair<String, Integer>, Store<ByteArray, byte[]>>();
    }

    public Store<ByteArray, byte[]> getLocalStore(String storeName) {
        return localStores.get(storeName);
    }

    public Store<ByteArray, byte[]> getRoutedStore(String storeName) {
        return routedStores.get(storeName);
    }

    public Store<ByteArray, byte[]> getNodeStore(String storeName, Integer id) {
        return nodeStores.get(Pair.create(storeName, id));
    }
}
