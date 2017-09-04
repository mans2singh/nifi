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
package org.apache.nifi.processors.neo4jcypher;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.SummaryCounters;

import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"neo4j", "graph", "insert", "update", "delete", "put", "get", "node", "relationship", "executor"})
@CapabilityDescription("Processor execute a Neo4J Query (https://www.neo4j.com/). The flow file will contain the result of the query in JSON format")
@WritesAttributes({
    })
public class Neo4JCypherExecutor extends AbstractNeo4JCypherExecutor {

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;


    static {
        final Set<Relationship> tempRelationships = new HashSet<>();
        tempRelationships.add(REL_SUCCESS);
        tempRelationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(tempRelationships);

        final List<PropertyDescriptor> tempDescriptors = new ArrayList<>();
        tempDescriptors.add(NEO4J_CONNECTION_URL);
        tempDescriptors.add(USERNAME);
        tempDescriptors.add(PASSWORD);
        tempDescriptors.add(NEO4J_QUERY);
        propertyDescriptors = Collections.unmodifiableList(tempDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String query = context.getProperty(NEO4J_QUERY).evaluateAttributeExpressions(flowFile).getValue();

        if (StringUtils.isBlank(query)) {
            getLogger().error("Empty query");
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, "Empty query");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            long startTimeMillis = System.currentTimeMillis();
   
            StatementResult statementResult = executeQuery(query);
            
            List<Map<String, Object>> returnValue = statementResult.list().stream().map(i -> i.asMap()).collect(Collectors.toList());
            
            getLogger().debug("Result of query {} is {}", new Object [] { query, returnValue });

            Gson gson = new Gson(); 
            String json = gson.toJson(returnValue);
            
            ByteArrayInputStream bios = new ByteArrayInputStream(json.getBytes(Charset.defaultCharset()));
            session.importFrom(bios, flowFile);
            
            final long endTimeMillis = System.currentTimeMillis();
            getLogger().debug("Executed statement with result {}", new Object[] {statementResult});

            flowFile = populateAttributes(session, flowFile, statementResult, returnValue.size());

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile,
                    new StringBuilder(connectionUrl).toString(),
                    (endTimeMillis - startTimeMillis));
        } catch (Exception exception) {
            getLogger().error("Failed to execute Neo4J statement due to {}",
                    new Object[]{exception.getLocalizedMessage()}, exception);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, String.valueOf(exception.getMessage()));
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

	protected StatementResult executeQuery(String query) {
		try ( Session session = getNeo4JDriver().session()) {
			return session.run(query);
		}
	}

    private FlowFile populateAttributes(final ProcessSession session, FlowFile flowFile,
    		StatementResult statementResult, int size) {
    	ResultSummary summary = statementResult.summary();
    	
    	SummaryCounters counters = summary.counters();
        
    	Map<String,String> resultAttributes = new HashMap<>();
        resultAttributes.put(NODES_CREATED,String.valueOf(counters.nodesCreated()));
        resultAttributes.put(RELATIONS_CREATED,String.valueOf(counters.relationshipsCreated()));
        resultAttributes.put(LABELS_ADDED,String.valueOf(counters.labelsAdded()));
        resultAttributes.put(NODES_DELETED,String.valueOf(counters.nodesDeleted()));
        resultAttributes.put(RELATIONS_DELETED,String.valueOf(counters.relationshipsDeleted()));
        resultAttributes.put(PROPERTIES_SET, String.valueOf(counters.propertiesSet()));
        resultAttributes.put(ROWS_RETURNED, String.valueOf(size));
        
        flowFile = session.putAllAttributes(flowFile, resultAttributes);
        return flowFile;
    }

    @OnStopped
    public void close() {
        super.close();
    }
}