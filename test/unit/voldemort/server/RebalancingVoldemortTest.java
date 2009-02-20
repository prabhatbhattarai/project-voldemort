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

package voldemort.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;

import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.utils.Props;

/**
 * @author bbansal
 * 
 */
public class RebalancingVoldemortTest extends TestCase {

    VoldemortConfig config;

    @Override
    public void setUp() throws IOException {
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/unit/temp-output");
        props.put("bdb.cache.size", 1 * 1024 * 1024);
        config = new VoldemortConfig(props);

        // clean and reinit metadata dir.
        FileDeleteStrategy.FORCE.delete(new File(config.getMetadataDirectory()));
        File tempDir = new File(config.getMetadataDirectory());
        tempDir.mkdirs();

        // copy cluster.xml / stores.xml to temp metadata dir.
        FileUtils.copyFileToDirectory(new File("test/common/voldemort/config/cluster.xml"), tempDir);
        FileUtils.copyFileToDirectory(new File("test/common/voldemort/config/stores.xml"), tempDir);

    }

    @Override
    public void tearDown() throws IOException {
        FileDeleteStrategy.FORCE.delete(new File(config.getMetadataDirectory()));
    }

    public void testUpdateCluster() {
        VoldemortServer server = new VoldemortServer(config);
        Cluster cluster = server.getMetaDataStore().getCluster();

        // add node 3 and partition 4,5 to cluster.
        ArrayList<Integer> partitionList = new ArrayList<Integer>();
        partitionList.add(4);
        partitionList.add(5);
        ArrayList<Node> nodes = new ArrayList<Node>(cluster.getNodes());
        nodes.add(new Node(3, "localhost", 8883, 6668, partitionList));
        Cluster updatedCluster = new Cluster("new-cluster", nodes);

        // update VoldemortServer cluster.xml
        server.updateClusterMetadata(updatedCluster);

        TestUtils.checkClusterMatch(updatedCluster, server.getMetaDataStore().getCluster());
    }

}
