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
package org.apache.nifi.processors.aws.kinesis.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestGetKinesis {

    protected ProcessSession mockProcessSession;
    protected ProcessContext mockProcessContext;

    @Before
    public void setUp() throws Exception {
        mockProcessSession = Mockito.mock(ProcessSession.class);
        mockProcessContext = Mockito.mock(ProcessContext.class);
    }

    @Test
    public void testPropertyDescriptors() {
        GetKinesis getKinesis = new GetKinesis();
        List<PropertyDescriptor> descriptors = getKinesis.getPropertyDescriptors();
        assertEquals("size should be same",18, descriptors.size());
        assertTrue(descriptors.contains(GetKinesis.REGION));
        assertTrue(descriptors.contains(GetKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_STREAM_NAME));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_APPLICATION_NAME));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_WORKER_ID_PREFIX));
        assertTrue(descriptors.contains(GetKinesis.BATCH_SIZE));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_INITIAL_POSITION_IN_STREAM));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_FAILOVER_TIME_MILLIS));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_MAX_RECORDS));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_IDLETIME_BETWEEN_READS_MILLIS));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_SHARD_SYNC_INTERVAL_MILLIS));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_TASK_BACKOFF_TIME_MILLIS));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_METRICS_BUFFER_TIME_MILLIS));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_METRICS_MAX_QUEUE_SIZE));
        assertTrue(descriptors.contains(GetKinesis.KINESIS_CONSUMER_DEFAULT_METRICS_LEVEL));
    }

    @Test
    public void testRelationships() {
        GetKinesis getKinesis = new GetKinesis();
        Set<Relationship> rels = getKinesis.getRelationships();
        assertEquals("size should be same",2, rels.size());
        assertTrue(rels.contains(GetKinesis.REL_FAILURE));
        assertTrue(rels.contains(GetKinesis.REL_SUCCESS));
    }

    @Test
    public void testOnTriggerCallsYield() throws Exception {
        GetKinesis getKinesis = new GetKinesis();
        getKinesis.onTrigger(mockProcessContext, (ProcessSession) null);
        Mockito.verify(mockProcessContext, Mockito.times(1)).yield();
    }
}
