/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.performance;

import static voldemort.utils.Utils.croak;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

public class RemoteTest {

    public static void main(String[] args) throws Exception {
        if(args.length < 5 || args.length > 6)
            croak("USAGE: java " + RemoteTest.class.getName()
                  + " url num_threads num_requests value_size start_num [rwd]");

        System.err.println("Bootstraping cluster data.");

        int argIndex = 0;

        String url = args[argIndex++];
        int numThreads = Integer.parseInt(args[argIndex++]);
        int numRequests = Integer.parseInt(args[argIndex++]);
        int valueSize = Integer.parseInt(args[argIndex++]);
        int startNum = Integer.parseInt(args[argIndex++]);

        String ops = "rwd";

        if(args.length > 5)
            ops = args[argIndex++];

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setMaxThreads(numThreads);
        clientConfig.setMaxConnectionsPerNode(numThreads);
        clientConfig.setBootstrapUrls(url);

        StoreClientFactory factory = new SocketStoreClientFactory(clientConfig);
        StoreClient<String, String> store = factory.getStoreClient("test");

        String value = new String(TestUtils.randomBytes(valueSize));
        ExecutorService service = Executors.newFixedThreadPool(numThreads);

        if(ops.contains("r")) {
            test(store, service, numThreads, numRequests, startNum, value, new Test() {

                public void test(StoreClient<String, String> store, String key, String value) {
                    store.get(key);
                }

                public String getName() {
                    return "read";
                }

            });
        }

        if(ops.contains("w")) {
            test(store, service, numThreads, numRequests, startNum, value, new Test() {

                public void test(StoreClient<String, String> store, String key, String value) {
                    store.put(key, new Versioned<String>(value));
                }

                public String getName() {
                    return "write";
                }

            });
        }

        if(ops.contains("d")) {
            test(store, service, numThreads, numRequests, startNum, value, new Test() {

                public void test(StoreClient<String, String> store, String key, String value) {
                    store.delete(key);
                }

                public String getName() {
                    return "delete";
                }

            });
        }

        System.exit(0);
    }

    private static void test(final StoreClient<String, String> store,
                             final ExecutorService service,
                             final int numThreads,
                             final int numRequests,
                             final int startNum,
                             final String value,
                             final Test test) throws Exception {
        long totalRequests = numThreads * numRequests;
        final AtomicInteger count = new AtomicInteger(startNum);
        final CountDownLatch latch = new CountDownLatch(numThreads);

        long start = System.currentTimeMillis();

        for(int i = 0; i < numThreads; i++) {
            service.execute(new Runnable() {

                public void run() {
                    try {
                        for(int i = 0; i < numRequests; i++) {
                            String key = Integer.toString(count.getAndIncrement());
                            test.test(store, key, value);
                        }
                    } catch(Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
        long millis = System.currentTimeMillis() - start;

        double result = (double) totalRequests / (double) millis * 1000.0;

        System.out.println("Throughput: " + (long) result + " " + test.getName() + "s/sec.");
    }

    private interface Test {

        public void test(StoreClient<String, String> store, String key, String value);

        public String getName();

    }

}
