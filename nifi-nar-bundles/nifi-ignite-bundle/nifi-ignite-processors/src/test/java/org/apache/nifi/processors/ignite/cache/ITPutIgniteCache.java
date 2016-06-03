/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.ignite.cache;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITPutIgniteCache {

    private static final String CACHE_NAME = "testCache";
    private TestRunner runner;
    private PutIgniteCache putIgniteCache;
    private Map<String,String> properties1;

    @Before
    public void setUp() throws IOException {
        putIgniteCache = new PutIgniteCache();
        properties1 = new HashMap<String,String>();
        properties1.put(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "key1");
    }

    @After
    public void teardown() {
        runner = null;
        putIgniteCache = null;
    }

    @Test
    public void testPutIgniteCacheOnTriggerFileConfigurationOneFlowFile() throws IOException, InterruptedException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.IGNITE_CONFIGURATION_FILE,
                "file:///" + new File(".").getAbsolutePath() + "/src/test/resources/test-ignite.xml");
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");

        runner.assertValid();

        runner.enqueue("test".getBytes(),properties1);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 1);
        List<MockFlowFile> sucessfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, sucessfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out.assertContentEquals("test".getBytes());
        Assert.assertArrayEquals("test".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key1"));
        runner.shutdown();
    }

}
