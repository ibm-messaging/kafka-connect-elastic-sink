/**
 * Copyright 2020 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventstreams.connect.elasticsink;

import com.ibm.eventstreams.connect.elasticsink.builders.DocumentBuilder;
import com.ibm.eventstreams.connect.elasticsink.builders.IdentifierBuilder;
import com.ibm.eventstreams.connect.elasticsink.builders.IndexBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.api.AuthenticationStore;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes messages to Elasticsearch using a REST call. Messages are added to the current
 * operation until told to commit. Automatically reconnects as needed.
 */
public class ElasticWriter {
    private static final String classname = ElasticWriter.class.getName();

    private static final Logger log = LoggerFactory.getLogger(ElasticWriter.class);

    private DocumentBuilder documentBuilder;
    private IndexBuilder indexBuilder;
    private IdentifierBuilder identifierBuilder;

    private boolean connected = false;                              // Whether connected to ES

    private static final long reconnectDelayMs[] = {0, 100, 500, 1000, 2000, 4000, 8000, 30000, 60000}; // Retry intervals for connection to server - up to 1 minute which then continues forever. 
    private int reconnectDelayIndex = 0;
    private boolean errorMaxDelay = false;

    private String userid;
    private String password;
    private String connection;
    private URI uri = null;

    private String keyStore;
    private String keyStorePassword;
    private String trustStore;
    private String trustStorePassword;

    private String protocol = "http";
    private String proxyHost = null;
    private int proxyPort = 0;

    private int jettyMaxConnections = ElasticSinkConnector.DEFAULT_JETTY_MAX_CONNECTIONS;
    private int jettyIdleTimeoutSec = ElasticSinkConnector.DEFAULT_JETTY_MAX_CONNECTIONS;
    private int jettyConnectionTimeoutSec = ElasticSinkConnector.DEFAULT_JETTY_CONNECTION_TIMEOUT_SEC;
    private int jettyOperationTimeoutSec = ElasticSinkConnector.DEFAULT_JETTY_OPERATION_TIMEOUT_SEC;

    private int maxCommitFailures = ElasticSinkConnector.DEFAULT_MAX_COMMIT_FAILURES; // How many commit failures allowed before treating it as fatal

    private String destination;
    private StringBuffer bulkMsg = new StringBuffer();

    private int commitFailures = 0; // This is only updated during commit by a single thread. No locking needed.

    private HttpClient httpClient;

    /**
     * Configure this class.
     *
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void configure(Map<String, String> props) throws ConnectException {
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(),classname, props);

        // Basic connection for the server
        connection = props.get(ElasticSinkConnector.CONFIG_NAME_ES_CONNECTION);
        userid = props.get(ElasticSinkConnector.CONFIG_NAME_ES_USER_NAME);
        password = props.get(ElasticSinkConnector.CONFIG_NAME_ES_PASSWORD);

        // TLS attributes
        keyStore = props.get(ElasticSinkConnector.CONFIG_NAME_ES_TLS_KEYSTORE_LOCATION);
        keyStorePassword = props.get(ElasticSinkConnector.CONFIG_NAME_ES_TLS_KEYSTORE_PASSWORD);
        trustStore = props.get(ElasticSinkConnector.CONFIG_NAME_ES_TLS_TRUSTSTORE_LOCATION);
        trustStorePassword = props.get(ElasticSinkConnector.CONFIG_NAME_ES_TLS_TRUSTSTORE_PASSWORD);

        // Jetty configuration for HTTP client behaviour
        proxyHost = props.get(ElasticSinkConnector.CONFIG_NAME_ES_HTTP_PROXY_HOST);
        proxyPort = getPropInt(props,ElasticSinkConnector.CONFIG_NAME_ES_HTTP_PROXY_PORT,ElasticSinkConnector.DEFAULT_HTTP_PROXY_PORT);

        jettyMaxConnections = getPropInt(props,ElasticSinkConnector.CONFIG_NAME_ES_JETTY_CONNECTIONS,ElasticSinkConnector.DEFAULT_JETTY_MAX_CONNECTIONS);
        jettyConnectionTimeoutSec = getPropInt(props,ElasticSinkConnector.CONFIG_NAME_ES_JETTY_CONNECTION_TIMEOUT_SEC,ElasticSinkConnector.DEFAULT_JETTY_CONNECTION_TIMEOUT_SEC);
        jettyIdleTimeoutSec = getPropInt(props,ElasticSinkConnector.CONFIG_NAME_ES_JETTY_IDLE_TIMEOUT_SEC,ElasticSinkConnector.DEFAULT_JETTY_IDLE_TIMEOUT_SEC);
        jettyOperationTimeoutSec = getPropInt(props,ElasticSinkConnector.CONFIG_NAME_ES_JETTY_OPERATION_TIMEOUT_SEC,ElasticSinkConnector.DEFAULT_JETTY_OPERATION_TIMEOUT_SEC);

        // Other connector configuration
        maxCommitFailures = getPropInt(props,ElasticSinkConnector.CONFIG_NAME_ES_MAX_COMMIT_FAILURES,ElasticSinkConnector.DEFAULT_MAX_COMMIT_FAILURES);
        String documentBuilderClass = props.get(ElasticSinkConnector.CONFIG_NAME_ES_DOCUMENT_BUILDER);
        String indexBuilderClass = props.get(ElasticSinkConnector.CONFIG_NAME_ES_INDEX_BUILDER);
        String identifierBuilderClass = props.get(ElasticSinkConnector.CONFIG_NAME_ES_IDENTIFIER_BUILDER);

        try {
            Class<? extends DocumentBuilder> c = Class.forName(documentBuilderClass).asSubclass(DocumentBuilder.class);
            documentBuilder = c.newInstance();
            documentBuilder.configure(props);
            log.debug("Instantiated document builder {}", documentBuilderClass);
        }
        catch (ClassNotFoundException | ClassCastException | IllegalAccessException | InstantiationException | NullPointerException e) {
            log.error("Could not instantiate document builder {}", documentBuilderClass);
            throw new ConnectException("Could not instantiate document builder", e);
        }

        try {
            Class<? extends IndexBuilder> c = Class.forName(indexBuilderClass).asSubclass(IndexBuilder.class);
            indexBuilder = c.newInstance();
            indexBuilder.configure(props);
            log.debug("Instantiated index builder {}", indexBuilderClass);
        }
        catch (ClassNotFoundException | ClassCastException | IllegalAccessException | InstantiationException | NullPointerException e) {
            log.error("Could not instantiate index builder '{}' {}", indexBuilderClass,e);
            throw new ConnectException("Could not instantiate index builder", e);
        }

        try {
            Class<? extends IdentifierBuilder> c = Class.forName(identifierBuilderClass).asSubclass(IdentifierBuilder.class);
            identifierBuilder = c.newInstance();
            identifierBuilder.configure(props);
            log.debug("Instantiated document identifier builder {}", identifierBuilderClass);
        }
        catch (ClassNotFoundException | ClassCastException | IllegalAccessException | InstantiationException | NullPointerException e) {
            log.error("Could not instantiate document identifier builder '{}' {}", identifierBuilderClass,e);
            throw new ConnectException("Could not instantiate document identifier builder", e);
        }
    }

    /**
     * getPropInt: If the configuration parameter is expected to be an integer, do
     * the conversion and return the value. Throw an exception if there is a bad format. If
     * the user has not provided a specific parameter, return the default value.
     */
    private int getPropInt(Map<String, String> props, String key, int defaultValue) {
        int rc = 0;
        String s = props.get(key);
        if (s == null) {
            return defaultValue;
        }
        try {
            rc = Integer.valueOf(s);
        }
        catch (NumberFormatException e) {
            log.error(e.getMessage());
            throw new ConnectException(e);
        }

        return rc;
    }

    /**
     * Connects to Elasticsearch.
     */
    public void connect() {
        log.trace("[{}] Entry {}.connect", Thread.currentThread().getId(), classname);

        setupConnection();

        // Try to connect to the server 
        while (!connected) {
            try {
                connectInternal(true);
            }
            catch (Exception e) {
                log.error("Cannot connect to Elasticsearch server: {}", e.getMessage());
                if (!(e instanceof RetriableException))
                    throw new ConnectException(e);
            }
        }

        log.trace("[{}]  Exit {}.connect", Thread.currentThread().getId(), classname);
    }

    /**
     * Adds a document to the request that's going to be sent to Elasticsearch.
     * In the BULK API, data is not actually sent yet.
     * We just add to the batch of data that will be sent during commit.
     *
     * @see the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html">Elasticsearch API</a>
     * for more information on how the HTTP operations work
     *
     * @param record                  The Kafka message and schema to send
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void send(SinkRecord record) throws ConnectException, RetriableException {
        log.trace("[{}] Entry {}.send", Thread.currentThread().getId(), classname);

        connectInternal(false);

        String jsonString = documentBuilder.fromSinkRecord(record);

        log.debug("Received message: \n  " + jsonString);

        // Create the actual identifiers for the document sent to Elasticsearch
        // - The "index" is generated by a separate configurable class from information 
        //   in the event
        String index = indexBuilder.generateIndex(record);

        // The document id can either come from the Kafka record's key or be a unique id.
        String id = identifierBuilder.fromSinkRecord(record);

        /* The BULK API requires the body of the request to be a list of pairs of JSON objects
         * with the first of the pair being the request itself and the second being the document
         * content. Each element must be separated from the next, and the whole request terminated by
         * a "\n". So in this method we just add the document to the request body and then leave the
         * submission until commit time
         */

        /* Example of calling the BULK API
            curl -X POST "localhost:9200/_bulk" -H 'Content-Type: application/json' -d'
            { "index" : { "_index" : "test", "_id" : "1" } }
            { "field1" : "value1" }
            { "delete" : { "_index" : "test", "_id" : "2" } }
            { "create" : { "_index" : "test", "_id" : "3" } }
            { "field1" : "value3" }
            { "update" : {"_id" : "1", "_index" : "test"} }
            { "doc" : {"field2" : "value2"} }
            '
         */

        JSONObject line = new JSONObject();
        JSONObject label = new JSONObject();

        // If the document is not empty, add it to the index
        if ((jsonString != null) && !jsonString.isEmpty()) {
            label.put("_index", index);
            label.put("_id", id);
            line.put("index", label);
            bulkMsg.append(line.toString());
            bulkMsg.append("\n");

            bulkMsg.append(jsonString);
            bulkMsg.append("\n");

            log.debug("Index document: index=" + index + ", _id=" + id);
        }
        else {
            // If the document ID is not unique, an empty record deletes from the index
            if (!identifierBuilder.isUnique()) {
                label.put("_index", index);
                label.put("_id", id);
                line.put("delete", label);
                bulkMsg.append(line.toString());
                bulkMsg.append("\n");

                log.debug("Delete document: index=" + index + ", _id=" + id);
            }
        }

        log.trace("[{}]  Exit {}.send", Thread.currentThread().getId(), classname);
    }

    /**
     * Executes the current transaction. At this point we send a single REST call to
     * the server containing all documents that need to be created during the batch.
     * This call is done synchronously to ensure we get a reasonable return code, and to
     * allow errors to be dealt with.
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed, and connector should stop.
     */
    public void commit() throws ConnectException, RetriableException {
        if (bulkMsg.length() == 0)
            return;

        log.trace("[{}] Entry {}.commit", Thread.currentThread().getId(),classname);

        connectInternal(false);

        // The "_bulk" path is where we need to execute the request
        destination = uri + "/_bulk";

        log.debug("About to put bulk message {} to {}", bulkMsg.toString(), destination);

        ContentResponse response=null;
        try {
            response = httpClient.newRequest(destination)
                                 .timeout(jettyOperationTimeoutSec, TimeUnit.SECONDS)
                                 .method(HttpMethod.POST)
                                 .content(new StringContentProvider(bulkMsg.toString()), "application/json")
                                 .send();

            // Always empty the request batch, even after a failure as the Connector framework
            // will restart the batch.
            bulkMsg.delete(0, bulkMsg.length());

            int status = (response != null) ? response.getStatus() : -100;
            String responseString = (response != null) ? response.toString() : "UNKNOWN";
            log.debug("Bulk insert returned {} {}", status, responseString);

            if (status < 200 || status > 299) {
                throw new RetriableException(responseString);
            }

            // After a success, reset the number of failures.
            commitFailures = 0;
        }
        catch (Exception e) {
            log.error("Error flushing messages to Elasticsearch", e);
            commitFailures++;
            bulkMsg.delete(0, bulkMsg.length());
            if (commitFailures > maxCommitFailures) {      
                throw new ConnectException("Reached maximum failures of commit processing. Last error: " + e.getMessage());
            }
            else {
                throw new RetriableException(e.getMessage());
            }
        }

        log.trace("[{}]  Exit {}.commit", Thread.currentThread().getId(), classname);
    }

    /**
     * Closes the connection.
     */
    public void close() {
        log.trace("[{}] Entry {}.close", Thread.currentThread().getId(), classname);

        try {
            connected = false;
            if (httpClient != null) {
                httpClient.stop();
            }
        }
        catch (Exception e) {
            ;
        }
        finally {
            httpClient = null;
            log.debug("Connection to Elasticsearch closed");
        }

        log.trace("[{}]  Exit {}.close", Thread.currentThread().getId(), classname);
    }

    /**
     * Internal method to connect to Elasticsearch. We do a synchronous connection and query
     * to the server to make sure it's reachable. The query is one that all servers
     * should respond to.
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    protected void connectInternal(boolean initialConnection) throws ConnectException, RetriableException {
        log.trace("[{}] Entry {}.connectInternal", Thread.currentThread().getId(), classname);

        if (connected) {
            return;
        }

        if (httpClient == null) {
            httpClient = setupConnection();
        }

        try {
            httpClient.start();

            // Doing a health check. We don't care about the return information
            // only that the request succeeds. This is done synchronously. This particular
            // URL is documented as giving some build information about the Elasticsearch server.
            String healthDestination =  uri + "/_cat/health";

            ContentResponse response = httpClient.newRequest(healthDestination)
                                                 .timeout(jettyConnectionTimeoutSec, TimeUnit.SECONDS)
                                                 .method(HttpMethod.GET)
                                                 .send();
            int status = response.getStatus();
 
            log.debug("Connection test returned {}", response.toString());

            // 4xx errors including 404 (page not found) are treated as immediately fatal as it
            // suggests either a failed server or a bad configuration for this connector.
            if (status >= 400 && status <= 499)
                throw new GeneralSecurityException(response.getReason());
            else if (status < 200 || status > 299) // 2xx codes are success
                throw new RetriableException(response.getReason());

            log.info("Connection to Elasticsearch established");
            reconnectDelayIndex = 0;
            errorMaxDelay = false;
            connected = true;
        }
        catch (Exception e) {
            int idx = reconnectDelayIndex;

            // Try to connect forever (or until some higher layer gives up). The
            // delay array has a set of values to sleep for until we reach the
            // maximum delay. And then we continue using that final delay value.
            if (idx >= reconnectDelayMs.length) {
                idx = reconnectDelayMs.length - 1;
                // Give an error message once
                if (!errorMaxDelay) {
                    log.error("Maximum connection delay value reached. Continuing to try connections...");
                    errorMaxDelay = true;
                }
            }  
            try {
                Thread.sleep(reconnectDelayMs[idx]);
                reconnectDelayIndex++;
            }
            catch (InterruptedException ie) {
                ;
            }

            throw handleException(e, initialConnection);
        }

        log.trace("[{}]  Exit {}.connectInternal", Thread.currentThread().getId(), classname);
    }

    /**
     * Handles exceptions from Elasticsearch. Almost all exceptions are treated as retriable meaning that the
     * connector can keep running. Security problems are fatal. The initialConnection is not used at the moment, 
     * but might be used to process failures differently depending on whether this is a startup or reconnect scenario.
     */
    private ConnectException handleException(Exception e, boolean initialConnection) {
        boolean isRetriable = false;
        boolean mustClose = true;

        log.info("Exception {} needs to be handled. ReconnectCount {}", e.getMessage(),reconnectDelayIndex);

        // Apart from security problems, all exceptions during connect are treated as retriable
        if (e instanceof GeneralSecurityException) {
            isRetriable = false;
            mustClose = true;
        }
        else { 
            isRetriable = true;
            mustClose = false;
        }

        if (mustClose) {
            close();
        }

        if (isRetriable) {
            return new RetriableException(e);
        }

        return new ConnectException(e);
    }

    /*
     * Initialise the HTTP Client object with necessary configuration including
     * authentication, TLS options and any defined Jetty tuning parameters.
     */
    private HttpClient setupConnection() {
        SslContextFactory sslContextFactory = new SslContextFactory.Client();

        // Point at the keystore and truststore. The passwords
        // are only set if necessary, as a default truststore may not be protected with password.
        if (notNullOrEmpty(keyStore)) {
            protocol = "https";
            sslContextFactory.setKeyStorePath(keyStore);
            if (notNullOrEmpty(keyStorePassword)) {
                sslContextFactory.setKeyStorePassword(keyStorePassword);
            }
        }
        if (notNullOrEmpty(trustStore)) {
            protocol = "https";
            sslContextFactory.setTrustStorePath(trustStore);
            if (notNullOrEmpty(trustStorePassword)) {
                sslContextFactory.setTrustStorePassword(trustStorePassword);
            }
        }

        try {
            uri = new URI(protocol + "://" + connection);
        }
        catch (URISyntaxException e) {
            log.error("Invalid URI {}",uri.toString());
            throw new ConnectException(e);
        }

        // Force the use of TLSv1.2 or later
        // Set the Protocols and CipherSuites that are permitted
        setDefaults(sslContextFactory);

        if (protocol.equals("https"))
            httpClient = new HttpClient(sslContextFactory);
        else
            httpClient = new HttpClient();

        // Authentication using userid/password is enabled here
        if (notNullOrEmpty(userid)) {
            AuthenticationStore auth = httpClient.getAuthenticationStore();
            auth.addAuthenticationResult(new BasicAuthentication.BasicResult(uri, userid, password));
        }

        // Tuning parameters for Jetty connections.
        int maxConnections = jettyMaxConnections;
        log.debug("Setting HTTP maxConnections to {}", maxConnections);
        httpClient.setMaxConnectionsPerDestination(maxConnections);

        // Setting an idle timeout can reduce the number of active threads/connections when set to non-zero value
        int idleTimeout = jettyIdleTimeoutSec;
        if (idleTimeout > 0) {
            log.debug("Setting idleTimeout to {} seconds", idleTimeout);
            httpClient.setIdleTimeout((long)(idleTimeout * 1000)); // Be explicit about casting to API datatype
        }

        // How long to wait for the server to respond during initial connection
        httpClient.setConnectTimeout(jettyConnectionTimeoutSec * 1000);

        setProxy(httpClient);

        return httpClient;
    }

    // set HTTP proxy settings
    private void setProxy(HttpClient httpClient) {
        if (proxyHost != null) {
            HttpProxy proxy = new HttpProxy(proxyHost, proxyPort);
            httpClient.getProxyConfiguration().getProxies().add(proxy);
        }
    }

    // Jetty 9.4.11 disables all ciphers beginning "SSL_" but when running under the IBM JRE it
    // has the effect of removing ALL the ciphersuites because that JRE has a different naming
    // pattern for the Ciphers.
    //
    // So we cannot rely on the Jetty default behaviour and if we can tell we're in
    // the IBM JRE we instead copy the patterns that Jetty disables except for one overreaching expression.
    private void setDefaults(SslContextFactory sslContextFactory) {
        log.trace("[{}] Entry {}.setDefaults", Thread.currentThread().getId(),classname);

        // Only support TLS 1.2
        String protocols[] = new String[] { "TLSv1.2" };
        sslContextFactory.setIncludeProtocols(protocols);

        String vendor = System.getProperty("java.vendor");
        if (vendor != null && vendor.toUpperCase().contains("IBM")) {
            log.debug("Doing manual exclusion of ciphersuites");

            // Exclude weak / insecure ciphers
            sslContextFactory.setExcludeCipherSuites("^.*_(MD5|SHA|SHA1)$");
            // Exclude ciphers that don't support forward secrecy
            sslContextFactory.addExcludeCipherSuites("^TLS_RSA_.*$");

            // The Jetty code uses the simple SSL_.* pattern, but this pattern has a similar effect
            // for the IBM JRE which uses 'SSL' instead of 'TLS' in many of the canonical cipher names
            sslContextFactory.addExcludeCipherSuites("^SSL_RSA_.*$");

            // Exclude NULL ciphers (that are accidentally present due to Include patterns)
            sslContextFactory.addExcludeCipherSuites("^.*_NULL_.*$");
            // Exclude anon ciphers (that are accidentally present due to Include patterns)
            sslContextFactory.addExcludeCipherSuites("^.*_anon_.*$");
        }

        log.trace("[{}]  Exit {}.setDefaults", Thread.currentThread().getId(), classname);
        return;
    }

    boolean isNullOrEmpty(String s) {
        if (s==null || s.isEmpty())
            return true;
        return false;
    }

    boolean notNullOrEmpty(String s) {
        return !isNullOrEmpty(s);
    }
}
