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
package org.apache.nifi.processors.aws.kinesis.producer;

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

public class TestPutKinesis {

    protected ProcessSession mockProcessSession;
    protected ProcessContext mockProcessContext;

    @Before
    public void setUp() throws Exception {
        mockProcessSession = Mockito.mock(ProcessSession.class);
        mockProcessContext = Mockito.mock(ProcessContext.class);
    }

    @Test
    public void testPropertyDescriptors() {
        PutKinesis putKinesis = new PutKinesis();
        List<PropertyDescriptor> descriptors = putKinesis.getPropertyDescriptors();
        assertEquals("size should be same",20, descriptors.size());
        assertTrue(descriptors.contains(PutKinesis.REGION));
        assertTrue(descriptors.contains(PutKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_STREAM_NAME));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PARTITION_KEY));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_MAX_BUFFER_INTERVAL));
        assertTrue(descriptors.contains(PutKinesis.BATCH_SIZE));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_AGGREGATION_ENABLED));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_AGGREGATION_MAX_COUNT));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_AGGREGATION_MAX_SIZE));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_COLLECTION_MAX_COUNT));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_COLLECTION_MAX_SIZE));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_FAIL_IF_THROTTLED));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_MAX_CONNECTIONS_TO_BACKEND));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_MIN_CONNECTIONS_TO_BACKEND));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_METRICS_NAMESPACE));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_METRICS_GRANULARITY));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_METRICS_LEVEL));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_MAX_PUT_RATE));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_REQUEST_TIMEOUT));
        assertTrue(descriptors.contains(PutKinesis.KINESIS_PRODUCER_TLS_CONNECT_TIMEOUT));
    }

    @Test
    public void testRelationships() {
        PutKinesis putKinesis = new PutKinesis();
        Set<Relationship> rels = putKinesis.getRelationships();
        assertEquals("size should be same",2, rels.size());
        assertTrue(rels.contains(PutKinesis.REL_FAILURE));
        assertTrue(rels.contains(PutKinesis.REL_SUCCESS));
    }

}
