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
package org.apache.nifi.processors.aws.lambda;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.InvalidParameterValueException;
import com.amazonaws.services.lambda.model.InvalidRequestContentException;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.LogType;
import com.amazonaws.services.lambda.model.RequestTooLargeException;
import com.amazonaws.services.lambda.model.ResourceNotFoundException;
import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.model.TooManyRequestsException;
import com.amazonaws.services.lambda.model.UnsupportedMediaTypeException;
import com.amazonaws.util.Base64;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "lambda", "put"})
@CapabilityDescription("Sends the contents to a specified Amazon Lamba Funciton")
@WritesAttributes({
    @WritesAttribute(attribute = "aws.lambda.result.function.error", description = "Function error message in result on posting message to AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.result.status.code", description = "Status code in the result for the message when posting to AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.result.payload", description = "Payload in the result from AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.result.log", description = "Log in the result of the message posted to Lambda")})
public class PutLambda extends AbstractAWSLambdaProcessor {

    /**
     * Lambda result function error message
     */
    public static final String AWS_LAMBDA_RESULT_FUNCTION_ERROR = "aws.lambda.result.function.error";

    /**
     * Lambda response status code
     */
    public static final String AWS_LAMBDA_RESULT_STATUS_CODE = "aws.lambda.result.status.code";

    /**
     * Lambda response log tail (4kb)
     */
    public static final String AWS_LAMBDA_RESULT_LOG = "aws.lambda.result.log";

    /**
     * Lambda payload in response
     */
    public static final String AWS_LAMBDA_RESULT_PAYLOAD = "aws.lambda.result.payload";

    /**
     * Max request body size
     */
    public static final long MAX_REQUEST_SIZE = 6 * 1000 * 1000;

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(AWS_LAMBDA_FUNCTION_NAME, AWS_LAMBDA_FUNCTION_QUALIFIER, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT
            ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String functionName = context.getProperty(AWS_LAMBDA_FUNCTION_NAME).getValue();

        final String qualifier = context.getProperty(AWS_LAMBDA_FUNCTION_QUALIFIER).getValue();

        // Max size of message is 6 MB
        if ( flowFile.getSize() > MAX_REQUEST_SIZE) {
            getLogger().error("Max size for request body is 6mb but was {} for flow file {} for function {}",
                new Object[]{flowFile.getSize(), flowFile, functionName});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final AWSLambdaClient client = getClient();

        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            session.exportTo(flowFile, baos);

            InvokeRequest invokeRequest = new InvokeRequest()
                .withFunctionName(functionName)
                .withLogType(LogType.Tail).withInvocationType(InvocationType.RequestResponse)
                .withPayload(ByteBuffer.wrap(baos.toByteArray()))
                .withQualifier(qualifier);
            long startTime = System.nanoTime();

            InvokeResult result = client.invoke(invokeRequest);

            flowFile = session.putAttribute(flowFile, AWS_LAMBDA_RESULT_STATUS_CODE, result.getStatusCode().toString());

            if ( !StringUtils.isBlank(result.getLogResult() )) {
                flowFile = session.putAttribute(flowFile, AWS_LAMBDA_RESULT_LOG, new String(Base64.decode(result.getLogResult()),Charset.defaultCharset()));
            }

            if ( result.getPayload() != null ) {
                flowFile = session.putAttribute(flowFile, AWS_LAMBDA_RESULT_PAYLOAD, new String(result.getPayload().array(),Charset.defaultCharset()));
            }

            if ( ! StringUtils.isBlank(result.getFunctionError()) ){
                flowFile = session.putAttribute(flowFile, AWS_LAMBDA_RESULT_FUNCTION_ERROR, result.getFunctionError());
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
                final long totalTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                session.getProvenanceReporter().send(flowFile, functionName, totalTimeMillis);
            }
        } catch (final InvalidRequestContentException
            | InvalidParameterValueException
            | RequestTooLargeException
            | ResourceNotFoundException
            | UnsupportedMediaTypeException unrecoverableException) {
                getLogger().error("Failed to invoke lambda {} with unrecoverable exception {} for flow file {}",
                    new Object[]{functionName, unrecoverableException, flowFile});
                session.transfer(flowFile, REL_FAILURE);
        } catch (final ServiceException | TooManyRequestsException exception) {
            getLogger().error("Failed to invoke lambda {} with exception {} for flow file {}",
                new Object[]{functionName, exception, flowFile});
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        } catch (final Exception exception) {
            getLogger().error("Failed to invoke lambda {} with exception {} for flow file {}",
                new Object[]{functionName, exception, flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

}
