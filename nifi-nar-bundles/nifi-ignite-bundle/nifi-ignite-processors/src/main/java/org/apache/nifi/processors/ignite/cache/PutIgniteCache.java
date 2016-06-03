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

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

/**
 * Put cache processors which pushes the flow file content into Ignite Cache using
 * DataStreamer interface
 */
@EventDriven
@SupportsBatching
@Tags({ "Ignite", "insert", "update", "stream", "write", "put", "cache", "key" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Stream the contents of a FlowFile to Ignite Cache using DataStreamer. " +
    "The processor uses the value of FlowFile attribute " + PutIgniteCache.IGNITE_CACHE_ENTRY_KEY + " as the " +
    "cache key and the byte array of the FlowFile as the value of the cache entry value.  Both the string key and a " +
    " non-empty byte array value are required otherwise the FlowFile is transfered to the failure relation.")
@WritesAttributes({
    @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, description = "The total number of FlowFile in the batch"),
    @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, description = "The item number of FlowFile in the batch"),
    @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, description = "The successful FlowFile item number"),
    @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, description = "The number of successful FlowFiles"),
    @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, description = "The failed FlowFile item number"),
    @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, description = "The total number of failed FlowFiles in the batch"),
    @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, description = "The failed reason attribute key")
    })
@ReadsAttributes({
    @ReadsAttribute(attribute = PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, description = "Ignite cache key"),
    })
public class PutIgniteCache extends AbstractIgniteCacheProcessor {

    /**
     * The batch size of flow files to be processed on invocation of onTrigger
     */
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size For Entries")
            .description("Batch size for entries (1-500).")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();

    /**
     * Data streamer's per node parallelism
     */
    public static final PropertyDescriptor DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS = new PropertyDescriptor.Builder()
            .name("Data Streamer Per Node Parallel Operations")
            .description("Data streamer per node parallelism")
            .defaultValue("5")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 10, true))
            .sensitive(false)
            .build();

    /**
     * Data streamers per node buffer size
     */
    public static final PropertyDescriptor DATA_STREAMER_PER_NODE_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Data Streamer Per Node Buffer Size")
            .description("Data streamer per node buffer size (1-500).")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();

    /**
     * Data streamers auto flush frequency
     */
    public static final PropertyDescriptor DATA_STREAMER_AUTO_FLUSH_FREQUENCY = new PropertyDescriptor.Builder()
            .name("Data Streamer Auto Flush Frequency in millis")
            .description("Data streamer flush interval in millis")
            .defaultValue("10")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 100, true))
            .sensitive(false)
            .build();

    /**
     * Data streamers override values property
     */
    public static final PropertyDescriptor DATA_STREAMER_ALLOW_OVERRIDE = new PropertyDescriptor.Builder()
            .name("Data Streamer Allow Override")
            .description("Whether to override values already in the cache")
            .defaultValue("false")
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .sensitive(false)
            .build();

    /** Flow file attribute keys and messages */
    public static final String IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT = "ignite.cache.batch.flow.file.total.count";
    public static final String IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER = "ignite.cache.batch.flow.file.item.number";
    public static final String IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT = "ignite.cache.batch.flow.file.successful.count";
    public static final String IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER = "ignite.cache.batch.flow.file.successful.number";
    public static final String IGNITE_BATCH_FLOW_FILE_FAILED_COUNT = "ignite.cache.batch.flow.file.failed.count";
    public static final String IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER = "ignite.cache.batch.flow.file.failed.number";
    public static final String IGNITE_BATCH_FLOW_FILE_FAILED_FILE_SIZE = "ignite.cache.batch.flow.file.failed.size";
    public static final String IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY = "ignite.cache.batch.flow.file.failed.reason";
    public static final String IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE = "The FlowFile key attribute was missing";
    public static final String IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE = "The FlowFile size was zero";

    static {
        descriptors = new ArrayList<>();
        descriptors.add(IGNITE_CONFIGURATION_FILE);
        descriptors.add(CACHE_NAME);
        descriptors.add(BATCH_SIZE);
        descriptors.add(DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS);
        descriptors.add(DATA_STREAMER_PER_NODE_BUFFER_SIZE);
        descriptors.add(DATA_STREAMER_AUTO_FLUSH_FREQUENCY);
        descriptors.add(DATA_STREAMER_ALLOW_OVERRIDE);
    }

    /**
     * Data streamer instance
     */
    private transient IgniteDataStreamer<String, byte[]> igniteDataStreamer;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * Close data streamer and calls base classes close ignite cache
     */
    @OnStopped
    public final void closeIgniteDataStreamer() {
        if (igniteDataStreamer != null) {
            getLogger().info("Closing ignite data streamer");
            igniteDataStreamer.flush();
            igniteDataStreamer.close();
            igniteDataStreamer = null;
        }
        super.closeIgniteCache();
    }

    /**
     * Get data streamer
     * @return data streamer instance
     */
    protected IgniteDataStreamer<String, byte[]> getIgniteDataStreamer() {
        return igniteDataStreamer;
    }

    /**
     * Initialize ignite cache
     */
    @OnScheduled
    public final void initilizeIgniteDataStreamer(ProcessContext context) throws ProcessException {
        super.initializeIgniteCache(context);

        if ( getIgniteDataStreamer() != null ) {
            return;
        }

        getLogger().info("Creating Ignite Datastreamer");
        try {
            int perNodeParallelOperations = context.getProperty(DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS).asInteger();
            int perNodeBufferSize = context.getProperty(DATA_STREAMER_PER_NODE_BUFFER_SIZE).asInteger();
            int autoFlushFrequency = context.getProperty(DATA_STREAMER_AUTO_FLUSH_FREQUENCY).asInteger();
            boolean allowOverride = context.getProperty(DATA_STREAMER_ALLOW_OVERRIDE).asBoolean();

            igniteDataStreamer = getIgnite().dataStreamer(getIgniteCache().getName());
            igniteDataStreamer.perNodeBufferSize(perNodeBufferSize);
            igniteDataStreamer.perNodeParallelOperations(perNodeParallelOperations);
            igniteDataStreamer.autoFlushFrequency(autoFlushFrequency);
            igniteDataStreamer.allowOverwrite(allowOverride);

        } catch (Exception e) {
            getLogger().error("Failed to schedule PutIgnite due to {}", new Object[] { e }, e);
            throw new ProcessException(e);
        }
    }

    /**
     * Handle flow files
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<FlowFile> flowFiles = session.get(batchSize);

        if (flowFiles.isEmpty()) {
            return;
        }

//        Map<String, byte[]> cacheMap = new java.util.HashMap<>();
        List<Map.Entry<String, byte[]>> cacheItems = new ArrayList<>();
        List<FlowFile> successfulFlowFiles = new ArrayList<>();
        List<FlowFile> failedFlowFiles = new ArrayList<>();
        try {
            for (int i = 0; i < flowFiles.size(); i++) {
                FlowFile flowFile = null;
                try {
                    flowFile = flowFiles.get(i);

                    String key = flowFile.getAttribute(IGNITE_CACHE_ENTRY_KEY);

                    if ( isFailedFlowFile(flowFile) ) {
                        failedFlowFiles.add(flowFile);
                        continue;
                    }

                    final byte[] byteArray = new byte[(int) flowFile.getSize()];
                    session.read(flowFile, new InputStreamCallback() {
                        @Override
                        public void process(final InputStream in) throws IOException {
                            StreamUtils.fillBuffer(in, byteArray, true);
                        }
                    });

                    cacheItems.add(new AbstractMap.SimpleEntry<String,byte[]>(key, byteArray));
                    successfulFlowFiles.add(flowFile);

                } catch (Exception e) {
                    getLogger().error("Failed to insert {} into IgniteDB due to {}", new Object[] { flowFile, e }, e);
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
        } finally {
            if (!cacheItems.isEmpty()) {
                IgniteFuture<?> futures = igniteDataStreamer.addData(cacheItems);
                Object result = futures.get();
                getLogger().debug("Result {} of addData", new Object [] {result});
            }

            if (!successfulFlowFiles.isEmpty()) {
                successfulFlowFiles = updateSuccessfulFlowFileAttributes(flowFiles, successfulFlowFiles, session);
                session.transfer(successfulFlowFiles, REL_SUCCESS);
            }

            if (!failedFlowFiles.isEmpty()) {
                failedFlowFiles = updateFailedFlowFileAttributes(flowFiles, failedFlowFiles, session);
                session.transfer(failedFlowFiles, REL_FAILURE);
            }
        }
    }

    /**
     * Check if flow if corrupted (either flow file is empty or does not have a key attribute)
     * @param flowFile the flow file to check
     * @return <code>true</code> if flow file is incomplete
     */
    private boolean isFailedFlowFile(FlowFile flowFile) {
        String key = flowFile.getAttribute(IGNITE_CACHE_ENTRY_KEY);
        if ( StringUtils.isEmpty(key) ) {
            return true;
        }
        if ( flowFile.getSize() == 0 ) {
            return true;
        }
        return false;
    }

    /**
     * Add successful flow file attributes
     * @param flowFiles all flow files
     * @param successfulFlowFiles list of successful flow files
     * @param session process session
     * @return successful flow files with updated attributes
     */
    protected List<FlowFile> updateSuccessfulFlowFileAttributes(
            List<FlowFile> flowFiles,
            List<FlowFile> successfulFlowFiles, ProcessSession session) {

        int flowFileCount = flowFiles.size();
        int flowFileSuccessful = successfulFlowFiles.size();
        List<FlowFile> updatedSuccessfulFlowFiles = new ArrayList<>();

        for (int i = 0; i < flowFileSuccessful; i++) {
            FlowFile flowFile = successfulFlowFiles.get(i);
            Map<String,String> attributes = new HashMap<>();
            attributes.put(IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, Integer.toString(i));
            attributes.put(IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, Integer.toString(flowFileCount));
            attributes.put(IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, Integer.toString(flowFiles.indexOf(flowFile)));
            attributes.put(IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, Integer.toString(flowFileSuccessful));
            flowFile = session.putAllAttributes(flowFile, attributes);
            updatedSuccessfulFlowFiles.add(flowFile);
        }

        return updatedSuccessfulFlowFiles;

    }

    /**
     * Add failed flow file attributes
     * @param flowFiles all flow files
     * @param failedFlowFiles list of failed flow files
     * @param session process session
     * @return failed flow files with updated attributes
     */
    protected List<FlowFile> updateFailedFlowFileAttributes(
            List<FlowFile> flowFiles,
            List<FlowFile> failedFlowFiles, ProcessSession session) {

        int flowFileCount = flowFiles.size();
        int flowFileFailed = failedFlowFiles.size();
        List<FlowFile> updatedFailedFlowFiles = new ArrayList<>();

        for (int i = 0; i < flowFileFailed; i++) {
            FlowFile flowFile = failedFlowFiles.get(i);

            Map<String,String> attributes = new HashMap<>();
            attributes.put(IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, Integer.toString(i));
            attributes.put(IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, Integer.toString(flowFileCount));
            attributes.put(IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, Integer.toString(flowFiles.indexOf(flowFile)));
            attributes.put(IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, Integer.toString(flowFileFailed));

            if (StringUtils.isEmpty(flowFile.getAttribute(IGNITE_CACHE_ENTRY_KEY))) {
                attributes.put(IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY,
                        IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
            } else if (flowFile.getSize() == 0) {
                attributes.put(IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY,
                        IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE);
            } else {
                throw new ProcessException("Unknown reason for failing file: " + flowFile);
            }

            flowFile = session.putAllAttributes(flowFile, attributes);
            updatedFailedFlowFiles.add(flowFile);
        }

        return updatedFailedFlowFiles;

    }
}
