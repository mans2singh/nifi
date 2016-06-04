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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.kinesis.AbstractKinesisProcessor;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

/**
 * This class provides processor the base class for Kinesis stream consumer.  It declares the
 * property descriptors and supporting methods for the consumer
 */
public abstract class AbstractKinesisConsumerProcessor extends AbstractKinesisProcessor {

    /**
     * The consumer application name
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_APPLICATION_NAME = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Application Name")
            .description("The consumer application name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The consumer worker id prefix.  This prefix is used along with host name and a UUID to generate
     * the final worker id
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_WORKER_ID_PREFIX = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Consumer Worker Id Prefix")
            .description("The Consumer worker id prefix")
            .defaultValue("KinesisConsumerWorkerId")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The starting point in the stream for the consumer
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_INITIAL_POSITION_IN_STREAM = new PropertyDescriptor.Builder()
            .name("Initial Position in Stream")
            .description("Initial position in stream from which to start getting events")
            .required(false)
            .defaultValue(InitialPositionInStream.LATEST.name())
            .allowableValues(getInitialPositions())
            .build();

    /**
     * Default time for renewal of lease by a consumer worker
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_FAILOVER_TIME_MILLIS = new PropertyDescriptor.Builder()
            .name("Default Failover Time")
            .description("Lease renewal time interval (millis) after which the worker is regarded as failed and lease granted to another worker")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Max records to fetch in each request
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_MAX_RECORDS = new PropertyDescriptor.Builder()
            .name("Max Records in Each Request")
            .description("Maximum number of records to be fetched in each request from the Kinesis stream")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Idle time between record reads
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_IDLETIME_BETWEEN_READS_MILLIS = new PropertyDescriptor.Builder()
            .name("Idle Time Betweeen Record Fetch")
            .description("Idle time between record reads (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("1000")
            .build();

    /**
     * Skip empty records call to records processor
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST = new PropertyDescriptor.Builder()
            .name("Skip call if records list is empty record lists")
            .description("Don't call record processor if record list is empty")
            .required(false)
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .defaultValue("true")
            .build();

    /**
     * Polling interval for parent shard
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS = new PropertyDescriptor.Builder()
            .name("Parent Shard Poll Interval")
            .description("Interval between polling to check for parent shard completion (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Sync shard interval
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_SHARD_SYNC_INTERVAL_MILLIS = new PropertyDescriptor.Builder()
            .name("Shard Sync Interval")
            .description("Sync interval for shard tasks (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("60000")
            .build();

    /**
     * Clean up lease after shard completion
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION = new PropertyDescriptor.Builder()
            .name("Clean up Lease on Shard Completion")
            .description("Proactively clean up leases to reduce resource tracking")
            .required(false)
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .defaultValue("true")
            .build();

    /**
     * Back off time interval in case of failures
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_TASK_BACKOFF_TIME_MILLIS = new PropertyDescriptor.Builder()
            .name("Back Off Time on Failure")
            .description("Backoff time interval on failure (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("500")
            .build();

    /**
     * Metrics buffer interval in millis
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_METRICS_BUFFER_TIME_MILLIS = new PropertyDescriptor.Builder()
            .name("Max Metrics Buffer interval")
            .description("Interval for which metrics are buffered (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Buffer metrics max count
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_METRICS_MAX_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("Max Metrics Buffer Count")
            .description("Buffer max count for metrics")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Metrics level
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_METRICS_LEVEL = new PropertyDescriptor.Builder()
            .name("Metrics Level")
            .description("Level of metrics send to cloud watch")
            .required(false)
            .allowableValues(getMetricsAllowableValues())
            .defaultValue(MetricsLevel.DETAILED.name())
            .build();

    /**
     * The procession session factory reference
     */
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        sessionFactoryReference.compareAndSet(null, sessionFactory);
        super.onTrigger(context, sessionFactoryReference.get());
    }

    /**
     * Get the metrics levels for reporting to AWS
     * @return metric levels
     */
    protected static Set<String> getMetricsAllowableValues() {
        Set<String> values = new HashSet<>();
        for (MetricsLevel ml : MetricsLevel.values()) {
            values.add(ml.name());
        }
        return values;
    }

    /**
     * Get the initial positions options to indicate where to start the stream
     * @return initial position options
     */
    protected static Set<String> getInitialPositions() {
        Set<String> values = new HashSet<>();
        for (InitialPositionInStream ipis : InitialPositionInStream.values()) {
            values.add(ipis.name());
        }
        return values;
    }

    /**
     * Get reference to ProcessSessionFactory
     * @return the process session factory
     */
    protected ProcessSessionFactory getSessionFactory() {
        return sessionFactoryReference.get();
    }

}