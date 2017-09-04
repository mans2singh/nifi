package org.apache.nifi.processors.neo4jcypher;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
/**
 * Abstract base class for Neo4JCypherExecutor processors
 */
abstract class AbstractNeo4JCypherExecutor extends AbstractProcessor {

    protected static final PropertyDescriptor NEO4J_QUERY = new PropertyDescriptor.Builder()
            .name("neo4J-query")
            .displayName("Neo4J Query")
            .description("Specifies the Neo4j Query.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor NEO4J_CONNECTION_URL = new PropertyDescriptor.Builder()
            .name("neo4j-connection-url")
            .displayName("Neo4j Connection URL")
            .description("Neo4J Connection URL")
            .required(true)
            .defaultValue("bolt://localhost:7687")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("neo4j-cypher-username")
            .displayName("Username")
            .description("Username for accessing Neo4J")
            .required(false)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("neo4j-cypher-password")
            .displayName("Password")
            .description("Password for Neo4J user")
            .required(false)
            .defaultValue("")
            .sensitive(true)
            .addValidator(Validator.VALID)
            .build();
    
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Sucessful FlowFiles are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed FlowFiles are routed to this relationship").build();

    public static final String ERROR_MESSAGE = "neo4j.error.message";
    public static final String NODES_CREATED= "neo4j.nodes.created";
    public static final String RELATIONS_CREATED = "neo4j.relations.created";
    public static final String LABELS_ADDED = "neo4j.labels.added";
    public static final String NODES_DELETED = "neo4j.nodes.deleted";
    public static final String RELATIONS_DELETED = "neo4j.relations.deleted";
    public static final String PROPERTIES_SET = "neo4j.properties.set";
    public static final String ROWS_RETURNED = "neo4j.rows.returned";
    
    protected Driver neo4JDriver;

    protected String username;
    protected String password;
    protected String connectionUrl;
    protected Integer port;
    protected long maxDocumentsSize;

    /**
     * Helper method to help testability
     * @return Driver instance
     */
    protected Driver getNeo4JDriver() {
        return neo4JDriver;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        connectionUrl = context.getProperty(NEO4J_CONNECTION_URL).getValue();
        username = nullSafeString(context.getProperty(USERNAME).getValue());
        password = nullSafeString(context.getProperty(PASSWORD).getValue());

        try {
            neo4JDriver = getDriver(connectionUrl, username, password);
        } catch(Exception e) {
            getLogger().error("Error while getting connection " + e.getLocalizedMessage(),e);
            throw new RuntimeException("Error while getting connection" + e.getLocalizedMessage(),e);
        }
        getLogger().info("Neo4JCypherExecutor connection created for url ",
                new Object[] {connectionUrl});
    }

    private String nullSafeString(String value) {
    	return value == null ? "" : value;
	}

	protected Driver getDriver(String connectionUrl, String username, String password) {
		 return GraphDatabase.driver( connectionUrl, AuthTokens.basic( username, password) );
    }

    @OnStopped
    public void close() {
        getLogger().info("Closing driver");
        if ( neo4JDriver != null )
        	neo4JDriver.close();
    }
}