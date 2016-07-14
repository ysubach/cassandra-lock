package com.dekses.cassandra.lock;

import java.util.UUID;

import com.datastax.driver.core.Cluster;
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
	
	/** Owner name */
	private String owner;
	
	/** Default lock time to live in seconds */
	private int defaultTTL = 60;
	
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
		owner = UUID.randomUUID().toString();
	}
	
	/**
	 * Set new default TTL for locks
	 * @param ttl New TTL value
	 */
	public void setDefaultTTL(int ttl) {
		defaultTTL = ttl;
	}
	
	/**
	 * Lock factory method
	 * @param resource Unique name of resource to be locked
	 * @return New lock object
	 */
	public Lock getLock(final String resource) {
		return new CassandraLock(session, owner, resource, defaultTTL);
	}
	
	/**
	 * Lock factory method, supports custom TTL
	 * @param resource Unique name of resource to be locked
	 * @param ttl Custom TTL value for lock
	 * @return New lock object
	 */
	public Lock getLock(final String resource, final int ttl) {
		return new CassandraLock(session, owner, resource, ttl);
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
