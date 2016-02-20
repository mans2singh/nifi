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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * The records processor factory which creates the processor with the records handler references
 *
 * @see IRecordProcessor
 * @see RecordsHandler
 */
public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

    /**
     * Reference to records handler to be passed to record processor
     */
    protected RecordsHandler recordsHandler;

    public KinesisRecordProcessorFactory(RecordsHandler recordsHandler) {
        this.recordsHandler = recordsHandler;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisRecordProcessor(recordsHandler);
    }

}
