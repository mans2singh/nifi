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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.kinesis.AbstractKinesisProcessor;

/**
 * This class provides processor the base class for kinesis stream
 */
public abstract class AbstractKinesisProducerProcessor extends AbstractKinesisProcessor {

    public static final PropertyDescriptor KINESIS_PARTITION_KEY = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Stream Partition Key.")
            .description("The partition key attribute.  If it is not set, a random value is used.")
            .expressionLanguageSupported(true)
            .defaultValue("${kinesis.partition.key}")
            .required(false)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_AGGREGATION_ENABLED = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Aggregation Enabled")
            .description("Producer aggregation enabled")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("true")
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_AGGREGATION_MAX_COUNT = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Aggregation Max Count")
            .description("Producer itmes aggregated in each kinesis record for each request (beween 1-4294967295)")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("4294967295")
            .addValidator(StandardValidators.createLongValidator(1, 4294967295L, true))
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_AGGREGATION_MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Aggregation Max Size")
            .description("Producer max aggregation size for data to be posted to Kinesis (64-1048576")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("51200")
            .addValidator(StandardValidators.createLongValidator(64, 1048576, true))
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_COLLECTION_MAX_COUNT = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Collection Max Count")
            .description("Producer items posted in each request (between 1-500)")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("500")
            .addValidator(StandardValidators.createLongValidator(1,500,true))
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_COLLECTION_MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Collection Max Size")
            .description("Producer collection max size (between 52224-9223372036854775807")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("5242880")
            .addValidator(StandardValidators.createLongValidator(52224,9223372036854775807L,true))
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_FAIL_IF_THROTTLED = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Fail If Throttled Enabled")
            .description("Producer fail the request if being throttled by AWS")
            .expressionLanguageSupported(false)
            .required(false)
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_MAX_CONNECTIONS_TO_BACKEND = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Max Connections To Backend")
            .description("Producer max connections to backend (between 1-256")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("24")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_MIN_CONNECTIONS_TO_BACKEND = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Min Connections To Backend")
            .description("Producer min connections to backend (between 1-16")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("1")
            .addValidator(StandardValidators.createLongValidator(1,16,true))
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_METRICS_GRANULARITY = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Metrics Granularity")
            .description("The metrics granularity for stream")
            .expressionLanguageSupported(false)
            .required(true)
            .defaultValue("shard")
            .allowableValues(new AllowableValue("shard"), new AllowableValue("global"), new AllowableValue("stream"))
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_METRICS_NAMESPACE = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Metrics Namespace")
            .description("The metrics granularity for stream")
            .expressionLanguageSupported(false)
            .required(true)
            .defaultValue("NifiKinesisProducer")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_METRICS_LEVEL = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Metrics Level")
            .description("The metrics level for the producer")
            .expressionLanguageSupported(false)
            .required(true)
            .defaultValue("detailed")
            .allowableValues(new AllowableValue("none"), new AllowableValue("summary"), new AllowableValue("detailed"))
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_MAX_PUT_RATE = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Producer Max Put Rate")
            .description("Producer aggregation count (between 1-9223372036854775807)")
            .expressionLanguageSupported(false)
            .required(false)
            .defaultValue("150")
            .addValidator(StandardValidators.createLongValidator(1,9223372036854775807L,true))
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_MAX_BUFFER_INTERVAL = new PropertyDescriptor.Builder()
            .name("Max Buffer Interval")
            .description("Buffering interval for messages (between 60 and 900 seconds).")
            .defaultValue("60")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(60,900,true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_TLS_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("TLS Connect time out")
            .description("TLS Connect time out (between 100 and 300000 milli seconds).")
            .defaultValue("6000")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(100,300000,true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor KINESIS_PRODUCER_REQUEST_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Request time out")
            .description("Request time out (between 100 and 600000 milli seconds).")
            .defaultValue("6000")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(100,600000,true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Batch size for messages to be processed on each trigger request (between 1-500).")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1,500,true))
            .sensitive(false)
            .build();
}