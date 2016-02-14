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
package org.apache.nifi.processors.aws;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.ssl.SSLContextService;

import com.amazonaws.Protocol;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * This is a base class of Nifi AWS Processors.  This class contains basic property descriptors, AWS credentials and relationships and
 * is not dependent on AmazonWebServiceClient classes.  It's subclasses add support for interacting with AWS with AWS respective
 * clients.
 *
 * @see AbstractAWSCredentialsProviderProcessor
 * @see AbstractAWSProcessor
 * @see AmazonWebServiceClient
 */
public abstract class AbstractBaseAWSProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
                .description("FlowFiles are routed to success after being successfully copied to Amazon S3").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
                .description("FlowFiles are routed to failure if unable to be copied to Amazon S3").build();
    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));
    /**
     * AWS credentials provider service
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider service")
            .description("The Controller Service that is used to obtain aws credentials provider")
            .required(false)
            .identifiesControllerService(AWSCredentialsProviderService.class)
            .build();

    public static final PropertyDescriptor CREDENTIALS_FILE = new PropertyDescriptor.Builder()
                .name("Credentials File")
                .expressionLanguageSupported(false)
                .required(false)
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .build();
    public static final PropertyDescriptor ACCESS_KEY = new PropertyDescriptor.Builder()
                .name("Access Key")
                .expressionLanguageSupported(true)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .sensitive(true)
                .build();
    public static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor.Builder()
                .name("Secret Key")
                .expressionLanguageSupported(true)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .sensitive(true)
                .build();
    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
                .name("Region")
                .required(true)
                .allowableValues(getAvailableRegions())
                .defaultValue(createAllowableValue(Regions.DEFAULT_REGION).getValue())
                .build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
                .name("Communications Timeout")
                .required(true)
                .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
                .defaultValue("30 secs")
                .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
                .name("SSL Context Service")
                .description("Specifies an optional SSL Context Service that, if provided, will be used to create connections")
                .required(false)
                .identifiesControllerService(SSLContextService.class)
                .build();
    public static final PropertyDescriptor ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
                .name("Endpoint Override URL")
                .description("Endpoint URL to use instead of the AWS default including scheme, host, port, and path. " +
                        "The AWS libraries select an endpoint URL based on the AWS region, but this property overrides " +
                        "the selected endpoint URL, allowing use with other S3-compatible endpoints.")
                .required(false)
                .addValidator(StandardValidators.URL_VALIDATOR)
                .build();
    protected volatile Region region;
    protected static final Protocol DEFAULT_PROTOCOL = Protocol.HTTPS;
    protected static final String DEFAULT_USER_AGENT = "NiFi";

    private static AllowableValue createAllowableValue(final Regions regions) {
        return new AllowableValue(regions.getName(), regions.getName(), regions.getName());
    }

    private static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Regions regions : Regions.values()) {
            values.add(createAllowableValue(regions));
        }

        return (AllowableValue[]) values.toArray(new AllowableValue[values.size()]);
    }

    public AbstractBaseAWSProcessor() {
        super();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        final boolean accessKeySet = validationContext.getProperty(ACCESS_KEY).isSet();
        final boolean secretKeySet = validationContext.getProperty(SECRET_KEY).isSet();
        if ((accessKeySet && !secretKeySet) || (secretKeySet && !accessKeySet)) {
            problems.add(new ValidationResult.Builder().input("Access Key").valid(false).explanation("If setting Secret Key or Access Key, must set both").build());
        }

        final boolean credentialsFileSet = validationContext.getProperty(CREDENTIALS_FILE).isSet();
        if ((secretKeySet || accessKeySet) && credentialsFileSet) {
            problems.add(new ValidationResult.Builder().input("Access Key").valid(false).explanation("Cannot set both Credentials File and Secret Key/Access Key").build());
        }

        return problems;
    }

    protected Region getRegion() {
        return region;
    }

}