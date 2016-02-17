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
