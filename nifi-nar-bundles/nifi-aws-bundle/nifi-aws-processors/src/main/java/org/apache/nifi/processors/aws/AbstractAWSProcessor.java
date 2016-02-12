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

import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.ssl.SSLContextService;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Abstract base class for aws processors.  This class uses aws credentials for creating aws clients
 *
 * @deprecated use {@link AbstractAWSCredentialsProviderProcessor} instead which uses credentials providers or creating aws clients
 * @see AbstractAWSCredentialsProviderProcessor
 *
 */
@Deprecated
public abstract class AbstractAWSProcessor<ClientType extends AmazonWebServiceClient> extends AbstractBaseAWSProcessor {

    protected volatile ClientType client;
    protected ClientConfiguration createConfiguration(final ProcessContext context) {
        final ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(context.getMaxConcurrentTasks());
        config.setMaxErrorRetry(0);
        config.setUserAgent(DEFAULT_USER_AGENT);
        // If this is changed to be a property, ensure other uses are also changed
        config.setProtocol(DEFAULT_PROTOCOL);
        final int commsTimeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        config.setConnectionTimeout(commsTimeout);
        config.setSocketTimeout(commsTimeout);

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE);
            SdkTLSSocketFactory sdkTLSSocketFactory = new SdkTLSSocketFactory(sslContext, null);
            config.getApacheHttpClientConfig().setSslSocketFactory(sdkTLSSocketFactory);
        }

        return config;
    }

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		super.onScheduled(context);
	    final ClientType awsClient = createClient(context, getCredentials(context), createConfiguration(context));
	    this.client = awsClient;
	    intializeRegionAndEndpoint(context);
	}

    protected void intializeRegionAndEndpoint(ProcessContext context) {
        // if the processor supports REGION, get the configured region.
        if (getSupportedPropertyDescriptors().contains(REGION)) {
            final String region = context.getProperty(REGION).getValue();
            if (region != null) {
                this.region = Region.getRegion(Regions.fromName(region));
                client.setRegion(this.region);
            } else {
                this.region = null;
            }
        }

        // if the endpoint override has been configured, set the endpoint.
        // (per Amazon docs this should only be configured at client creation)
        final String urlstr = StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE).getValue());
        if (!urlstr.isEmpty()) {
            this.client.setEndpoint(urlstr);
        }

    }

    /**
     * Create client from the arguments
     * @param context process context
     * @param credentials static aws credentials
     * @param config aws client configuration
     * @return ClientType aws client
     *
     * @deprecated use {@link AbstractAWSCredentialsProviderProcessor#createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)}
     */
    @Deprecated
    protected abstract ClientType createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config);

    protected ClientType getClient() {
        return client;
    }

}
