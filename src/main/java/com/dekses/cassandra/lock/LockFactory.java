package com.dekses.cassandra.lock;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

/**
 * Factory class for lock objects. Encapsulates creation of Cassandra
 * based locks. Also provide methods for singleton style usage of
 * the factory.
 */
public class LockFactory {

	private static final int CASSANDRA_DEFAULT_PORT = 9042;

	/** Current Cassandra session */
	private CqlSession session;
	
	/** Default owner name */
	private String defaultOwner;
	
	/** Default lock time to live in seconds */
	private int defaultTTL = 60;
	
	// Prepared CQL statements
	private PreparedStatement insertPrep;
	private PreparedStatement selectPrep;
	private PreparedStatement deletePrep;
	private PreparedStatement updatePrep;
	
	
	/**
	 * Constructor, uses Cassandra session
	 * @param session External Cassandra session
	 */
	public LockFactory(CqlSession session) {
		this.session = session;
		generalInit();
	}
	
	/**
	 * Constructor, creates Cassandra session
	 * @param contactPoints Cassandra cluster contact points
	 * @param keyspace Keyspace for `lock_leases`
	 * @param localDataCenter Local Datacenter of the Cassandra cluster
	 * @param userName Cassandra user name
	 * @param password Cassandra password
	 */
	public LockFactory(String contactPoints, String keyspace, String localDataCenter, String userName, String password) {

		List<InetSocketAddress> contactPointsList = new ArrayList();
		for (String n : contactPoints.split(",")) {
			String[] hostPort = n.split(":");
			//In case the contact points only contain the host assuming the default port.
			int port = CASSANDRA_DEFAULT_PORT;
			if (hostPort.length > 1) {
				port = Integer.parseInt(hostPort[1]);
			}
			contactPointsList.add(new InetSocketAddress(hostPort[0], port));
		}

		DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
				.withString(DefaultDriverOption.REQUEST_CONSISTENCY, ConsistencyLevel.QUORUM.name())
				.build();

		CqlSessionBuilder builder = CqlSession.builder()
				.addContactPoints(contactPointsList)
				.withConfigLoader(loader)
				.withLocalDatacenter(localDataCenter)
				.withAuthCredentials(userName, password);

	    session = builder.build();
		session.execute("USE " + keyspace);
	    generalInit();
	}
	
	/** 
	 * Shared initialization method.
	 * Generates random owner name for the factory.
	 */
	private void generalInit() {
		defaultOwner = UUID.randomUUID().toString();
		insertPrep = session.prepare("INSERT INTO lock_leases (name, owner) VALUES (?,?) IF NOT EXISTS USING TTL ?");
		selectPrep = session.prepare("SELECT * FROM lock_leases WHERE name = ?");
		deletePrep = session.prepare("DELETE FROM lock_leases WHERE name = ? IF owner = ?");
		updatePrep = session.prepare("UPDATE lock_leases USING TTL ? SET owner = ? WHERE name = ? IF owner = ?");
	}
	
	/**
	 * Set new default TTL for locks
	 * @param ttl New TTL value
	 */
	public void setDefaultTTL(int ttl) {
		defaultTTL = ttl;
	}

	/**
	 * Set new default owner of lock
	 * @param owner New owner of lock
	 */
	public void setDefaultOwner(String owner) {
		defaultOwner = owner;
	}
	
	
	/**
	 * Lock factory method
	 * @param resource Unique name of resource to be locked
	 * @return New lock object
	 */
	public Lock getLock(final String resource) {
		return new CassandraLock(session, defaultOwner, resource, defaultTTL, insertPrep, selectPrep, deletePrep, updatePrep);
	}
	
	/**
	 * Lock factory method, supports custom owner
	 * @param resource Unique name of resource to be locked
	 * @param owner Custom owner of lock
	 * @return New lock object
	 */
	public Lock getLock(final String resource, final String owner) {
		return new CassandraLock(session, owner, resource, defaultTTL, insertPrep, selectPrep, deletePrep, updatePrep);
	}
	
	/**
	 * Lock factory method, supports custom TTL
	 * @param resource Unique name of resource to be locked
	 * @param ttl Custom TTL value for lock
	 * @return New lock object
	 */
	public Lock getLock(final String resource, final int ttl) {
		return new CassandraLock(session, defaultOwner, resource, ttl, insertPrep, selectPrep, deletePrep, updatePrep);
	}
	
	/**
	 * Lock factory method, supports custom owner and TTL
	 * @param resource Unique name of resource to be locked
	 * @param owner Custom owner of lock
	 * @param ttl Custom TTL value for lock
	 * @return New lock object
	 */
	public Lock getLock(final String resource, final String owner, final int ttl) {
		return new CassandraLock(session, owner, resource, ttl, insertPrep, selectPrep, deletePrep, updatePrep);
	}
	
	
	/// --- Singleton implementation below --- ///
	
	/** Single instance */
	private static LockFactory instance;
	
	/** 
	 * Singleton initialization
	 * @param session Cassandra session
	 */
	public static void initialize(CqlSession session) {
		instance = new LockFactory(session);
	}
	
	/**
	 * Singleton initialization
	 * @param contactPoints Cassandra cluster contact points
	 * @param keyspace Keyspace for `lock_leases`
	 * @param datacenter Local Datacenter of the Cassandra cluster
	 * @param userName Cassandra user name
	 * @param password Cassandra password
	 */
	public static void initialize(String contactPoints, String keyspace, String datacenter, String userName, String password) {
		instance = new LockFactory(contactPoints, keyspace, datacenter, userName, password);
	}
	
	/**
	 * Factory getter, must be called only after singleton initialization.
	 * @return Single factory instance.
	 */
	public static LockFactory getInstance() {
		return instance;
	}
}
