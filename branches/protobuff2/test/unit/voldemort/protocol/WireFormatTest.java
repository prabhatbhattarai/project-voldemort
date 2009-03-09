package voldemort.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.store.StorageEngine;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public abstract class WireFormatTest extends TestCase {

    private final String storeName;
    private final WireFormat wireFormat;
    private final InMemoryStorageEngine<byte[], byte[]> store;

    public WireFormatTest(WireFormatType type) {
        this.storeName = "test";
        ConcurrentMap<String, StorageEngine<byte[], byte[]>> stores = new ConcurrentHashMap<String, StorageEngine<byte[], byte[]>>();
        this.store = new InMemoryStorageEngine<byte[], byte[]>(storeName);
        stores.put("test", store);
        this.wireFormat = new WireFormatFactory(stores, stores).getWireFormat(type);
    }

    public void testNullKeys() throws Exception {
        try {
            testGetRequest(null, null, null, false);
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }

        try {
            // testRequest(null, null, null, false);
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
    }

    public void testGetRequests() throws Exception {
        testGetRequest("hello".getBytes(), null, null, false);
        testGetRequest("hello".getBytes(), "".getBytes(), new VectorClock(), true);
        testGetRequest("hello".getBytes(), "abc".getBytes(), TestUtils.getClock(1, 2, 2, 3), true);
        testGetRequest("hello".getBytes(),
                       "abcasdf".getBytes(),
                       TestUtils.getClock(1, 3, 4, 5),
                       true);

    }

    public void testGetRequest(byte[] key, byte[] value, VectorClock version, boolean isPresent)
            throws Exception {
        try {
            if(isPresent)
                store.put(key, Versioned.value(value, version));
            ByteArrayOutputStream getRequest = new ByteArrayOutputStream();
            this.wireFormat.writeGetRequest(new DataOutputStream(getRequest), storeName, key, false);
            ByteArrayOutputStream getResponse = new ByteArrayOutputStream();
            this.wireFormat.handleRequest(inputStream(getRequest),
                                          new DataOutputStream(getResponse));
            List<Versioned<byte[]>> values = this.wireFormat.readGetResponse(inputStream(getResponse));
            if(isPresent) {
                assertEquals(1, values.size());
                Versioned<byte[]> v = values.get(0);
                assertEquals(version, v.getVersion());
                assertTrue(Arrays.equals(v.getValue(), value));
            } else {
                assertEquals(0, values.size());
            }
        } finally {
            this.store.deleteAll();
        }
    }

    public void testPutRequests() throws Exception {
        testPutRequest(new byte[0], new byte[0], new VectorClock(), null);
        testPutRequest("hello".getBytes(), "world".getBytes(), new VectorClock(), null);

        // test obsolete exception
        this.store.put("hello".getBytes(), new Versioned<byte[]>("world".getBytes(),
                                                                 new VectorClock()));
        testPutRequest("hello".getBytes(),
                         "world".getBytes(),
                         new VectorClock(),
                         ObsoleteVersionException.class);
    }

    public void testPutRequest(byte[] key,
                                 byte[] value,
                                 VectorClock version,
                                 Class<? extends VoldemortException> exception) throws Exception {
        try {
            ByteArrayOutputStream putRequest = new ByteArrayOutputStream();
            this.wireFormat.writePutRequest(new DataOutputStream(putRequest),
                                            storeName,
                                            key,
                                            value,
                                            version,
                                            false);
            ByteArrayOutputStream putResponse = new ByteArrayOutputStream();
            this.wireFormat.handleRequest(inputStream(putRequest),
                                          new DataOutputStream(putResponse));
            this.wireFormat.readPutResponse(inputStream(putResponse));
            TestUtils.assertContains(this.store, key, value);
        } catch(Exception e) {
            assertEquals("Unexpected exception " + e.getClass().getName(), e.getClass(), exception);
        } finally {
            this.store.deleteAll();
        }
    }

    public void testDeleteRequests() throws Exception {
        // test pre-existing are deleted
        testDeleteRequest(new byte[0],
                          new VectorClock(),
                          new Versioned<byte[]>("hello".getBytes()),
                          true);
        testDeleteRequest("hello".getBytes(),
                          new VectorClock(),
                          new Versioned<byte[]>("world".getBytes()),
                          true);

        // test non-existant aren't deleted
        testDeleteRequest("hello".getBytes(), new VectorClock(), null, false);
    }

    public void testDeleteRequest(byte[] key,
                                  VectorClock version,
                                  Versioned<byte[]> existingValue,
                                  boolean isDeleted) throws Exception {
        try {
            if(existingValue != null)
                this.store.put(key, existingValue);
            ByteArrayOutputStream delRequest = new ByteArrayOutputStream();
            this.wireFormat.writeDeleteRequest(new DataOutputStream(delRequest),
                                               storeName,
                                               key,
                                               version,
                                               false);
            ByteArrayOutputStream delResponse = new ByteArrayOutputStream();
            this.wireFormat.handleRequest(inputStream(delRequest),
                                          new DataOutputStream(delResponse));
            boolean wasDeleted = this.wireFormat.readDeleteResponse(inputStream(delResponse));
            assertEquals(isDeleted, wasDeleted);
        } finally {
            this.store.deleteAll();
        }
    }

    public DataInputStream inputStream(ByteArrayOutputStream output) {
        return new DataInputStream(new ByteArrayInputStream(output.toByteArray()));
    }

}
