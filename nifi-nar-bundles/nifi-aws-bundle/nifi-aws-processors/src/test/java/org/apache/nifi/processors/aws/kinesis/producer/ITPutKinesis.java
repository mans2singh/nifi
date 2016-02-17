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

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processors.aws.AbstractBaseAWSProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ITPutKinesis {

    private TestRunner runner;
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesis.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, AbstractBaseAWSProcessor.CREDENTIALS_FILE,
                CREDENTIALS_FILE);
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        runner.setProperty(PutKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
//    @Ignore
    public void testIntegrationSuccessWithPartitionKey() throws Exception {
        runner.setProperty(PutKinesis.KINESIS_STREAM_NAME, "kcontest-stream");
        runner.assertValid();
        runner.setProperty(PutKinesis.KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("test".getBytes(),attrib);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesis.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesis.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        Map<String, String> attributes = out.getAttributes();
        assertTrue(attributes.containsKey(PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
        assertTrue(attributes.containsKey(PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
        assertTrue(attributes.containsKey(PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

        out.assertContentEquals("test".getBytes());
    }

    @Test
//  @Ignore
  public void testIntegrationSuccessWithPartitionKeyDefaultSetting() throws Exception {
      runner.setProperty(PutKinesis.KINESIS_STREAM_NAME, "kcontest-stream");
      runner.assertValid();
      runner.setValidateExpressionUsage(true);
      Map<String,String> attrib = new HashMap<>();
      attrib.put("kinesis.partition.key", "p1");
      runner.enqueue("test".getBytes(),attrib);
      runner.run(1);

      runner.assertAllFlowFilesTransferred(PutKinesis.REL_SUCCESS, 1);

      final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesis.REL_SUCCESS);
      final MockFlowFile out = ffs.iterator().next();

      Map<String, String> attributes = out.getAttributes();
      assertTrue(attributes.containsKey(PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
      assertTrue(attributes.containsKey(PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
      assertTrue(attributes.containsKey(PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

      out.assertContentEquals("test".getBytes());
  }

    @Test
//  @Ignore
  public void testIntegrationSuccessWithOutPartitionKey() throws Exception {
      runner.setProperty(PutKinesis.KINESIS_STREAM_NAME, "kcontest-stream");
      runner.assertValid();
      runner.setProperty(PutKinesis.KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
      runner.setValidateExpressionUsage(true);
      Map<String,String> attrib = new HashMap<>();
      runner.enqueue("test".getBytes(),attrib);
      runner.run(1);

      runner.assertAllFlowFilesTransferred(PutKinesis.REL_SUCCESS, 1);

      final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesis.REL_SUCCESS);
      final MockFlowFile out = ffs.iterator().next();

      Map<String, String> attributes = out.getAttributes();
      assertTrue(attributes.containsKey(PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
      assertTrue(attributes.containsKey(PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
      assertTrue(attributes.containsKey(PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

      out.assertContentEquals("test".getBytes());
  }
    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
//    @Ignore
    public void testIntegrationFailedBadStreamName() throws Exception {
        runner.setProperty(PutKinesis.KINESIS_STREAM_NAME, "bad-stream");
        runner.assertValid();

        runner.enqueue("test".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKinesis.REL_FAILURE, 1);

    }
}
