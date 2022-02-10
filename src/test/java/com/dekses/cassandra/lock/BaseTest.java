package com.dekses.cassandra.lock;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.InetSocketAddress;

public class BaseTest {

    private static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse("cassandra:4.0.1");

    public static final String LOCAL_HOST = "127.0.0.1";
    public static final String LOCAL_DC = "datacenter1";
    public static final String KEYSPACE = "casslock_test";

    public static CassandraContainer CASSANDRA_CONTAINER;
    public static int CASSANDRA_PORT;
    public static String CASSANDRA_USER;
    public static String CASSANDRA_PASSWORD;
    public static String CONTACT_POINT;


    protected CqlSession session;

    @BeforeClass
    public static void startCassandra() throws IOException, InterruptedException {
        CASSANDRA_CONTAINER = (CassandraContainer) new CassandraContainer(CASSANDRA_IMAGE)
                .withConfigurationOverride("cassandra-config")
                .waitingFor(Wait.forLogMessage(".*Created default superuser role 'cassandra'.*\\n", 1));

        CASSANDRA_CONTAINER.start();
        CASSANDRA_PORT = CASSANDRA_CONTAINER.getMappedPort(9042);
        CASSANDRA_USER = CASSANDRA_CONTAINER.getUsername();
        CASSANDRA_PASSWORD = CASSANDRA_CONTAINER.getPassword();
        CONTACT_POINT = LOCAL_HOST + ":" + CASSANDRA_PORT;
    }

    @AfterClass
    public static void stopCassandra() {
        CASSANDRA_CONTAINER.stop();
    }

    @Before
    public void setUp() {
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(LOCAL_HOST, CASSANDRA_PORT))
                .withLocalDatacenter(LOCAL_DC)
                .withAuthCredentials(CASSANDRA_USER, CASSANDRA_PASSWORD).build();

        session.execute("Create keyspace if not exists " + KEYSPACE + " with replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
        session.execute("USE " + KEYSPACE);
        session.execute("CREATE TABLE if not exists lock_leases (name text PRIMARY KEY, owner text, value text ) WITH default_time_to_live = 60;");
        session.execute("TRUNCATE lock_leases");

    }

    @After
    public void cleanUp() {
        session.close();
    }

}
