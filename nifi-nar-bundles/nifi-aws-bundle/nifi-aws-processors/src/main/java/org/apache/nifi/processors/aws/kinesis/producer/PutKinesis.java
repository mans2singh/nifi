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
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.kinesis.producer.Attempt;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "kinesis", "put", "stream"})
@CapabilityDescription("Sends the contents of the flowfile to a specified Amazon Kinesis stream")
@ReadsAttribute(attribute = PutKinesis.AWS_KINESIS_PARTITION_KEY, description = "Partition key to be used for publishing data to kinesis.  If it is not available then a random key used")
@WritesAttributes({
    @WritesAttribute(attribute = PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL, description = "Was the record posted successfully"),
    @WritesAttribute(attribute = PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID, description = "Shard id where the record was posted"),
    @WritesAttribute(attribute = PutKinesis.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER, description = "Sequence number of the posted record"),
    @WritesAttribute(attribute = PutKinesis.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_COUNT, description = "Number of attempts for posting the record"),
    @WritesAttribute(attribute = PutKinesis.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + "<n>" + PutKinesis.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_CODE_SUFFIX , description =
        "Attempt error code for each attempt"),
    @WritesAttribute(attribute = PutKinesis.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + "<n>" + PutKinesis.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_DELAY_SUFFIX , description =
            "Attempt delay for each attempt"),
    @WritesAttribute(attribute = PutKinesis.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + "<n>" + PutKinesis.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_MESSAGE_SUFFIX , description =
            "Attempt error message for each attempt"),
    @WritesAttribute(attribute = PutKinesis.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + "<n>" + PutKinesis.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_SUCCESSFUL_SUFFIX , description =
            "Attempt successfulfor each attempt")
})
public class PutKinesis extends AbstractKinesisProducerProcessor {

    /**
     * Attributes written by the producer
     */
    public static final String AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL = "aws.kinesis.producer.record.successful";
    public static final String AWS_KINESIS_PRODUCER_RECORD_SHARD_ID ="aws.kinesis.producer.record.shard.id";
    public static final String AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER = "aws.kinesis.producer.record.sequencenumber";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX = "aws.kinesis.producer.record.attempt.";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_CODE_SUFFIX = ".error.code";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_DELAY_SUFFIX = ".delay";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_MESSAGE_SUFFIX = ".error.message";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_SUCCESSFUL_SUFFIX = ".successful";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_COUNT = "aws.kinesis.producer.record.attempts.count";

    /**
     * Attributes read the producer
     */
    public static final String AWS_KINESIS_PARTITION_KEY = "kinesis.partition.key";

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
        Arrays.asList(
            REGION,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            KINESIS_STREAM_NAME,
            KINESIS_PARTITION_KEY,
            KINESIS_PRODUCER_MAX_BUFFER_INTERVAL,
            KINESIS_PRODUCER_AGGREGATION_ENABLED,
            KINESIS_PRODUCER_AGGREGATION_MAX_COUNT,
            KINESIS_PRODUCER_AGGREGATION_MAX_SIZE,
            KINESIS_PRODUCER_COLLECTION_MAX_COUNT,
            KINESIS_PRODUCER_COLLECTION_MAX_SIZE,
            KINESIS_PRODUCER_FAIL_IF_THROTTLED,
            KINESIS_PRODUCER_MAX_CONNECTIONS_TO_BACKEND,
            KINESIS_PRODUCER_MIN_CONNECTIONS_TO_BACKEND,
            KINESIS_PRODUCER_METRICS_NAMESPACE,
            KINESIS_PRODUCER_METRICS_GRANULARITY,
            KINESIS_PRODUCER_METRICS_LEVEL,
            KINESIS_PRODUCER_MAX_PUT_RATE,
            KINESIS_PRODUCER_REQUEST_TIMEOUT,
            KINESIS_PRODUCER_TLS_CONNECT_TIMEOUT,
            BATCH_SIZE
            ));

    protected KinesisProducer producer;

    protected Random randomGenerator;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        randomGenerator = new Random();

        KinesisProducerConfiguration config = new KinesisProducerConfiguration();

        config.setRegion(context.getProperty(PutKinesis.REGION).getValue());

        final AWSCredentialsProviderService awsCredentialsProviderService =
                context.getProperty(PutKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentialsProviderService.class);

        config.setCredentialsProvider(awsCredentialsProviderService.getCredentialsProvider());

        config.setMaxConnections(context.getProperty(PutKinesis.KINESIS_PRODUCER_MAX_CONNECTIONS_TO_BACKEND).asInteger());
        config.setMinConnections(context.getProperty(PutKinesis.KINESIS_PRODUCER_MIN_CONNECTIONS_TO_BACKEND).asInteger());

        config.setRequestTimeout(context.getProperty(PutKinesis.KINESIS_PRODUCER_REQUEST_TIMEOUT).asInteger());
        config.setConnectTimeout(context.getProperty(PutKinesis.KINESIS_PRODUCER_TLS_CONNECT_TIMEOUT).asLong());

        config.setRecordMaxBufferedTime(context.getProperty(PutKinesis.KINESIS_PRODUCER_MAX_BUFFER_INTERVAL).asInteger());
        config.setRateLimit(context.getProperty(PutKinesis.KINESIS_PRODUCER_MAX_PUT_RATE).asInteger());

        config.setAggregationEnabled(context.getProperty(PutKinesis.KINESIS_PRODUCER_AGGREGATION_ENABLED).asBoolean());
        config.setAggregationMaxCount(context.getProperty(PutKinesis.KINESIS_PRODUCER_AGGREGATION_MAX_COUNT).asLong());
        config.setAggregationMaxSize(context.getProperty(PutKinesis.KINESIS_PRODUCER_AGGREGATION_MAX_SIZE).asInteger());

        config.setCollectionMaxCount(context.getProperty(PutKinesis.KINESIS_PRODUCER_COLLECTION_MAX_COUNT).asInteger());
        config.setCollectionMaxSize(context.getProperty(PutKinesis.KINESIS_PRODUCER_COLLECTION_MAX_SIZE).asInteger());

        config.setFailIfThrottled(context.getProperty(PutKinesis.KINESIS_PRODUCER_FAIL_IF_THROTTLED).asBoolean());

        config.setMetricsCredentialsProvider(awsCredentialsProviderService.getCredentialsProvider());
        config.setMetricsNamespace(context.getProperty(PutKinesis.KINESIS_PRODUCER_METRICS_NAMESPACE).getValue());
        config.setMetricsGranularity(context.getProperty(PutKinesis.KINESIS_PRODUCER_METRICS_GRANULARITY).getValue());
        config.setMetricsLevel(context.getProperty(PutKinesis.KINESIS_PRODUCER_METRICS_LEVEL).getValue());

        producer = new KinesisProducer(config);

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        final String stream = context.getProperty(KINESIS_STREAM_NAME).getValue();

        try {
            List<Future<UserRecordResult>> addRecordFutures = new ArrayList<Future<UserRecordResult>>();
            // Prepare batch of records
            for (int i = 0; i < flowFiles.size(); i++) {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                session.exportTo(flowFiles.get(i), baos);

                String partitionKey = context.getProperty(PutKinesis.KINESIS_PARTITION_KEY)
                    .evaluateAttributeExpressions(flowFiles.get(i)).getValue();
                if ( StringUtils.isBlank(partitionKey) ) {
                    partitionKey = Integer.toString(randomGenerator.nextInt());
                }
                addRecordFutures.add(producer.addUserRecord(stream, partitionKey, ByteBuffer.wrap(baos.toByteArray()))) ;
            }

            // Apply attributes to flow files
            List<FlowFile> failedFlowFiles = new ArrayList<>();
            List<FlowFile> successfulFlowFiles = new ArrayList<>();
            for (int i = 0; i < addRecordFutures.size(); i++ ) {
                Future<UserRecordResult> future = addRecordFutures.get(i);
                FlowFile flowFile = flowFiles.get(i);
                UserRecordResult userRecordResult = null;

                try {
                    userRecordResult = future.get();
                } catch(ExecutionException ee) {
                    // Handle exception from individual record
                    Throwable cause = ee.getCause();
                    if ( cause instanceof UserRecordFailedException ) {
                        UserRecordFailedException urfe = (UserRecordFailedException) cause;
                        userRecordResult = urfe.getResult();
                    } else {
                        session.transfer(flowFile, PutKinesis.REL_FAILURE);
                        getLogger().error("Failed to publish to kinesis {} record {}", new Object[]{stream, flowFile});
                        continue;
                    }
                }
                Map<String, String> attributes = createAttributes(userRecordResult);

                flowFile = session.putAllAttributes(flowFile, attributes);
                if ( ! userRecordResult.isSuccessful() ) {
                    failedFlowFiles.add(flowFile);
                } else {
                    successfulFlowFiles.add(flowFile);
                }
            }

            if ( failedFlowFiles.size() > 0 ) {
                session.transfer(failedFlowFiles, PutKinesis.REL_FAILURE);
                getLogger().error("Failed to publish to kinesis {} records {}", new Object[]{stream, failedFlowFiles});
            }

            if ( successfulFlowFiles.size() > 0 ) {
                session.transfer(successfulFlowFiles, REL_SUCCESS);
                getLogger().info("Successfully published to kinesis {} records {}", new Object[]{stream, successfulFlowFiles});
            }

        } catch (final Exception exception) {
            getLogger().error("Failed to publish to kinesis {} with exception {}", new Object[]{flowFiles, exception});
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }

    /**
     * Helper method to create attributes form result
     * @param result UserRecordResult
     * @return Map of attributes
     */
    protected Map<String, String> createAttributes(UserRecordResult result) {
        Map<String,String> attributes = new HashMap<>();
        attributes.put(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL, Boolean.toString(result.isSuccessful()));
        attributes.put(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID, result.getShardId());
        attributes.put(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER, result.getSequenceNumber());
        int attemptCount = 1;
        for (Attempt attempt : result.getAttempts()) {
            attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + attemptCount + AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_CODE_SUFFIX,attempt.getErrorCode());
            attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + attemptCount + AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_DELAY_SUFFIX,Integer.toString(attempt.getDelay()));
            attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + attemptCount + AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_MESSAGE_SUFFIX,attempt.getErrorMessage());
            attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + attemptCount + AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_SUCCESSFUL_SUFFIX,
                Boolean.toString(attempt.isSuccessful()));
            attemptCount++;
        };
        attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_COUNT, Integer.toString(result.getAttempts().size()));
        return attributes;
    }

    @OnShutdown
    public void onShutdown() {
        if ( producer != null ) {
            producer.flushSync();
            producer.destroy();
        }
    }
}