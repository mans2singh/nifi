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
package org.apache.nifi.processors.aws.dynamodb;

import java.util.List;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public abstract class AbstractWriteDynamoDBProcessor extends AbstractDynamoDBProcessor {

    /**
     * Helper method to handle unprocessed items
     * @param session
     * @param keysToFlowFileMap
     * @param table
     * @param hashKeyName
     * @param hashKeyValueType
     * @param rangeKeyName
     * @param rangeKeyValueType
     * @param outcome
     */
    protected void handleUnprocessedItems(final ProcessSession session, Map<ItemKeys, FlowFile> keysToFlowFileMap, final String table, final String hashKeyName, final String hashKeyValueType,
            final String rangeKeyName, final String rangeKeyValueType, BatchWriteItemOutcome outcome) {
        BatchWriteItemResult result = outcome.getBatchWriteItemResult();

        // Handle unprocessed items
        List<WriteRequest> unprocessedItems = result.getUnprocessedItems().get(table);
        if ( unprocessedItems != null && unprocessedItems.size() > 0 ) {
            for ( WriteRequest request : unprocessedItems) {
                Map<String,AttributeValue> item = request.getPutRequest().getItem();
                Object hashKeyValue = getValue(item, hashKeyName, hashKeyValueType);
                Object rangeKeyValue = getValue(item, rangeKeyName, rangeKeyValueType);

                sendUnhandledToFailure(session, keysToFlowFileMap, hashKeyValue, rangeKeyValue);
            }
        }
    }
}
