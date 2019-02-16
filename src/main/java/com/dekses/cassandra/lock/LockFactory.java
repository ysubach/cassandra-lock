package com.dekses.cassandra.lock;

import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

/**
 * Factory class for lock objects. Encapsulates creation of Cassandra
 * based locks. Also provide methods for singleton style usage of
 * the factory.
 */
public class LockFactory {
	
	/** Current Cassandra session */
	private Session session;
	
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
	public LockFactory(Session session) {
		this.session = session;
		generalInit();
	}
	
	/**
	 * Constructor, creates Cassandra session
	 * @param contactPoints Cassandra cluster contact points
	 * @param keyspace Keyspace for `lock_leases`
	 */
	public LockFactory(String contactPoints, String keyspace) {
		Builder builder = Cluster.builder();
		for (String point : contactPoints.split(",")) {
			builder.addContactPoint(point.trim());
		}
		
		Cluster cluster = builder.build();
	    session = cluster.connect();
	    session.execute("USE " + keyspace);
	    generalInit();
	}
	
	/** 
	 * Shared initialization method.
	 * Generates random owner name for the factory.
	 */
	private void generalInit() {
		defaultOwner = UUID.randomUUID().toString();
		insertPrep = session.prepare("INSERT INTO lock_leases (name, owner) VALUES (?,?) IF NOT EXISTS USING TTL ?"); // 
		insertPrep.setConsistencyLevel(ConsistencyLevel.QUORUM);
		selectPrep = session.prepare("SELECT * FROM lock_leases WHERE name = ?");
		selectPrep.setConsistencyLevel(ConsistencyLevel.SERIAL);
		deletePrep = session.prepare("DELETE FROM lock_leases where name = ? IF owner = ?");
		deletePrep.setConsistencyLevel(ConsistencyLevel.QUORUM);
		updatePrep = session.prepare("UPDATE lock_leases set owner = ? where name = ? IF owner = ?");
		updatePrep.setConsistencyLevel(ConsistencyLevel.QUORUM);
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
	 * Lock factory method, supports custom TTL
	 * @param resource Unique name of resource to be locked
	 * @param owner Custom owner of lock
	 * @return New lock object
	 */
	public Lock getLock(final String resource, final String owner) {
		return new CassandraLock(session, owner, resource, defaultTTL, insertPrep, selectPrep, deletePrep, updatePrep);
	}

	
	/**
	 * Lock factory method, supports custom owner
	 * @param resource Unique name of resource to be locked
	 * @param ttl Custom TTL value for lock
	 * @return New lock object
	 */
	public Lock getLock(final String resource, final int ttl) {
		return new CassandraLock(session, defaultOwner, resource, ttl, insertPrep, selectPrep, deletePrep, updatePrep);
	}
	
	
	/// --- Singleton implementation below --- ///
	
	/** Single instance */
	private static LockFactory instance;
	
	/** 
	 * Singleton initialization
	 * @param session Cassandra session
	 */
	public static void initialize(Session session) {
		instance = new LockFactory(session);
	}
	
	/**
	 * Singleton initialization
	 * @param contactPoints Cassandra cluster contact points
	 * @param keyspace Keyspace for `lock_leases`
	 */
	public static void initialize(String contactPoints, String keyspace) {
		instance = new LockFactory(contactPoints, keyspace);
	}
	
	/**
	 * Factory getter, must be called only after singleton initialization.
	 * @return Single factory instance.
	 */
	public static LockFactory getInstance() {
		return instance;
	}
}
