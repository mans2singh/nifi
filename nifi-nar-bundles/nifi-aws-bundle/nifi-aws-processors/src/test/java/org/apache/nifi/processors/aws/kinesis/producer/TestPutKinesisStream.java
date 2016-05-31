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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.aws.AbstractBaseAWSProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.kinesis.KinesisHelper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;

@SuppressWarnings("unchecked")
public class TestPutKinesisStream {

    protected ProcessSession mockProcessSession;
    protected ProcessContext mockProcessContext;

    private TestRunner runner;

    private static final String kinesisStream = "k-stream";

    protected KinesisProducer mockProducer;

    protected PutKinesisStream putKinesis;
    private ListenableFuture<UserRecordResult> mockFuture1;
    private UserRecordResult mockUserRecordResult1;
    private Attempt mockAttempt1;
    private ListenableFuture<UserRecordResult> mockFuture2;
    private UserRecordResult mockUserRecordResult2;
    private Attempt mockAttempt2;
    private List<Attempt> listAttempts;

    private ListenableFuture<UserRecordResult> mockFuture3;
    private UserRecordResult mockUserRecordResult3;

    @Before
    public void setUp() throws Exception {
        putKinesis = new PutKinesisStream() {

            @Override
            protected KinesisProducer makeProducer(KinesisProducerConfiguration config) {
                return mockProducer;
            }

        };
        runner = TestRunners.newTestRunner(putKinesis);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, AbstractBaseAWSProcessor.CREDENTIALS_FILE,
                KinesisHelper.CREDENTIALS_FILE);
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        runner.setProperty(PutKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
        mockProcessSession = mock(ProcessSession.class);
        mockProcessContext = mock(ProcessContext.class);
        mockProducer = mock(KinesisProducer.class);
        mockFuture1 = mock(ListenableFuture.class);
        mockFuture2 = mock(ListenableFuture.class);
        mockFuture3 = mock(ListenableFuture.class);
        listAttempts = new ArrayList<>();
        mockUserRecordResult1 = mock(UserRecordResult.class);
        mockUserRecordResult2 = mock(UserRecordResult.class);
        mockUserRecordResult3 = mock(UserRecordResult.class);
        mockAttempt1 = mock(Attempt.class);
        mockAttempt2 = mock(Attempt.class);

        when(mockAttempt1.getDelay()).thenReturn(1);
        when(mockAttempt1.isSuccessful()).thenReturn(true);
        when(mockAttempt2.getDelay()).thenReturn(2);
        when(mockAttempt2.isSuccessful()).thenReturn(true);
    }

    @Test
    public void testPropertyDescriptors() {
        PutKinesisStream putKinesis = new PutKinesisStream();
        List<PropertyDescriptor> descriptors = putKinesis.getPropertyDescriptors();
        assertEquals("size should be same",20, descriptors.size());
        assertTrue(descriptors.contains(PutKinesisStream.REGION));
        assertTrue(descriptors.contains(PutKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_STREAM_NAME));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PARTITION_KEY));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_MAX_BUFFER_INTERVAL));
        assertTrue(descriptors.contains(PutKinesisStream.BATCH_SIZE));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_AGGREGATION_ENABLED));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_AGGREGATION_MAX_COUNT));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_AGGREGATION_MAX_SIZE));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_COLLECTION_MAX_COUNT));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_COLLECTION_MAX_SIZE));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_FAIL_IF_THROTTLED));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_MAX_CONNECTIONS_TO_BACKEND));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_MIN_CONNECTIONS_TO_BACKEND));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_METRICS_NAMESPACE));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_METRICS_GRANULARITY));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_METRICS_LEVEL));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_MAX_PUT_RATE));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_REQUEST_TIMEOUT));
        assertTrue(descriptors.contains(PutKinesisStream.KINESIS_PRODUCER_TLS_CONNECT_TIMEOUT));
    }

    @Test
    public void testRelationships() {
        Set<Relationship> rels = putKinesis.getRelationships();
        assertEquals("size should be same",2, rels.size());
        assertTrue(rels.contains(PutKinesisStream.REL_FAILURE));
        assertTrue(rels.contains(PutKinesisStream.REL_SUCCESS));
    }

    @Test
    public void testOnTriggerSuccess1Default() throws Exception {
        putKinesis.setProducer(mockProducer);

        when(mockProducer.addUserRecord(
                anyString(), anyString(), isA(ByteBuffer.class))).thenReturn(mockFuture1);
        when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult1.isSuccessful()).thenReturn(true);
        listAttempts.add(mockAttempt1);
        runner.setProperty(PutKinesisStream.KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(PutKinesisStream.KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("test".getBytes(),attrib);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisStream.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        Map<String, String> attributes = out.getAttributes();
        assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
        assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
        assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

        out.assertContentEquals("test".getBytes());
    }

    @Test
    public void testOnTriggerFailure1Default() throws Exception {
        putKinesis.setProducer(mockProducer);

        when(mockProducer.addUserRecord(
                anyString(), anyString(), isA(ByteBuffer.class))).thenReturn(mockFuture1);
        when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult1.isSuccessful()).thenReturn(false);
        listAttempts.add(mockAttempt1);
        runner.setProperty(PutKinesisStream.KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(PutKinesisStream.KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("test".getBytes(),attrib);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesisStream.REL_FAILURE, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(
                PutKinesisStream.REL_FAILURE);
        final MockFlowFile out = ffs.iterator().next();

        Map<String, String> attributes = out.getAttributes();
        assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
        assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
        assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

        out.assertContentEquals("test".getBytes());
    }

    @Test
    public void testOnTriggerSuccess2Default() throws Exception {
        putKinesis.setProducer(mockProducer);

        when(mockProducer.addUserRecord(
                anyString(), anyString(), isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult1.isSuccessful()).thenReturn(true);

        when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult2.isSuccessful()).thenReturn(true);

        runner.setProperty(PutKinesisStream.KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(PutKinesisStream.KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("success1".getBytes(),attrib);
        runner.enqueue("success2".getBytes(),attrib);
        runner.run(1);
        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        runner.assertAllFlowFilesTransferred(PutKinesisStream.REL_SUCCESS, 2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_SUCCESS);
        int count = 1;
        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if ( count == 1 ) {
                flowFile.assertContentEquals("success1".getBytes());
            }
            if ( count == 2 ) {
                flowFile.assertContentEquals("success2".getBytes());
            }
            count++;
        }
    }

    @Test
    public void testOnTrigger2FailedBothDefault() throws Exception {
        putKinesis.setProducer(mockProducer);

        when(mockProducer.addUserRecord(
                anyString(), anyString(), isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult1.isSuccessful()).thenReturn(false);

        when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult2.isSuccessful()).thenReturn(false);

        runner.setProperty(PutKinesisStream.KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(PutKinesisStream.KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("failure1".getBytes(),attrib);
        runner.enqueue("failure2".getBytes(),attrib);
        runner.run(1);
        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        runner.assertAllFlowFilesTransferred(PutKinesisStream.REL_FAILURE, 2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_FAILURE);
        int count = 1;
        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if ( count == 1 ) {
                flowFile.assertContentEquals("failure1".getBytes());
            }
            if ( count == 2 ) {
                flowFile.assertContentEquals("failure2".getBytes());
            }
            count++;
        }
    }

    @Test
    public void testOnTrigger2FirstFailedSecondSuccessDefault() throws Exception {
        putKinesis.setProducer(mockProducer);

        when(mockProducer.addUserRecord(
                anyString(), anyString(), isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult1.isSuccessful()).thenReturn(false);

        when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult2.isSuccessful()).thenReturn(true);

        runner.setProperty(PutKinesisStream.KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(PutKinesisStream.KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("failure1".getBytes(),attrib);
        runner.enqueue("success2".getBytes(),attrib);
        runner.run(1);
        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_FAILURE);
        int count = 1;
        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if ( count == 1 ) {
                flowFile.assertContentEquals("failure1".getBytes());
            }
        }

        final List<MockFlowFile> ffs2 = runner.getFlowFilesForRelationship(PutKinesisStream.REL_SUCCESS);
        int count2 = 1;
        for (MockFlowFile flowFile : ffs2) {

            Map<String, String> attributes = flowFile.getAttributes();
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if ( count2 == 1 ) {
                flowFile.assertContentEquals("success2".getBytes());
            }
        }
    }

    @Test
    public void testOnTrigger2FirstSucessSecondFailedDefault() throws Exception {
        putKinesis.setProducer(mockProducer);

        when(mockProducer.addUserRecord(
                anyString(), anyString(), isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult1.isSuccessful()).thenReturn(true);

        when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult2.isSuccessful()).thenReturn(false);

        runner.setProperty(PutKinesisStream.KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(PutKinesisStream.KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("success1".getBytes(),attrib);
        runner.enqueue("failure2".getBytes(),attrib);
        runner.run(1);
        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_FAILURE);
        int count = 1;
        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if ( count == 1 ) {
                flowFile.assertContentEquals("failure2".getBytes());
            }
        }

        final List<MockFlowFile> ffs2 = runner.getFlowFilesForRelationship(PutKinesisStream.REL_SUCCESS);
        int count2 = 1;
        for (MockFlowFile flowFile : ffs2) {

            Map<String, String> attributes = flowFile.getAttributes();
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if ( count2 == 1 ) {
                flowFile.assertContentEquals("success1".getBytes());
            }
        }
    }

    @Test
    public void testOnTrigger3FirstSucessSecondFailedThirdSuccessDefault() throws Exception {
        putKinesis.setProducer(mockProducer);

        when(mockProducer.addUserRecord(
                anyString(), anyString(), isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult1.isSuccessful()).thenReturn(true);

        when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult2.isSuccessful()).thenReturn(false);

        when(mockFuture3.get()).thenReturn(mockUserRecordResult3);
        when(mockUserRecordResult3.getAttempts()).thenReturn(listAttempts);
        when(mockUserRecordResult3.getSequenceNumber()).thenReturn("seq1");
        when(mockUserRecordResult3.getShardId()).thenReturn("shard1");
        when(mockUserRecordResult3.isSuccessful()).thenReturn(true);

        runner.setProperty(PutKinesisStream.KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(PutKinesisStream.KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("success1".getBytes(),attrib);
        runner.enqueue("failure2".getBytes(),attrib);
        runner.enqueue("success3".getBytes(),attrib);
        runner.run(1);
        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_FAILURE);
        int count = 1;
        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if ( count == 1 ) {
                flowFile.assertContentEquals("failure2".getBytes());
            }
            count++;
        }

        final List<MockFlowFile> ffs2 = runner.getFlowFilesForRelationship(PutKinesisStream.REL_SUCCESS);
        int count2 = 1;
        for (MockFlowFile flowFile : ffs2) {

            Map<String, String> attributes = flowFile.getAttributes();
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            assertTrue(attributes.containsKey(PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if ( count2 == 1 ) {
                flowFile.assertContentEquals("success1".getBytes());
            }
            if ( count2 == 2 ) {
                flowFile.assertContentEquals("success3".getBytes());
            }
            count++;
        }
    }
}
