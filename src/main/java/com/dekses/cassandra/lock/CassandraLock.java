package com.dekses.cassandra.lock;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Distributed lock implementation based on Cassandra lightweight 
 * transactions feature (uses Paxos consensus protocol)
 */
public class CassandraLock implements Lock {

	/** Current Cassandra session */
	private Session session;
	
	/** Lock owner name */
	private String owner;
	
	/** Lock lease (resource) name */
	private String name;
	
	/** Lock time to live in seconds */
	private int ttl;
	
	// Prepared CQL statements
	private PreparedStatement insertPrep;
	private PreparedStatement selectPrep;
	private PreparedStatement deletePrep;
	private PreparedStatement updatePrep;
	
	/**
	 * Constructor 
	 * @param session
	 * @param owner
	 * @param name
	 * @param ttl
	 * @param insertPrep
	 * @param selectPrep
	 * @param deletePrep
	 * @param updatePrep
	 */
	public CassandraLock(Session session, String owner, String name, int ttl, PreparedStatement insertPrep, PreparedStatement selectPrep, PreparedStatement deletePrep, PreparedStatement updatePrep) {
		this.session = session;
		this.owner = owner;
		this.name = name;
		this.ttl = ttl;
		this.insertPrep = insertPrep;
		this.selectPrep = selectPrep;
		this.deletePrep = deletePrep;
		this.updatePrep = updatePrep;
	}
	
	/** @return Lock owner */
	public String getOwner() {
		return owner;
	}

	/** @return Lock resource name */
	public String getName() {
		return name;
	}

	/** @return Lock TTL */
	public int getTTL() {
		return ttl;
	}

	/**
	 * Try to acquire lock lease, uses INSERT query.
	 */
	public boolean tryLock() {
		ResultSet rs = session.execute(insertPrep.bind(name, owner, ttl));
		if (rs.wasApplied()) {
			return true;
		} else {
			Row lease = rs.one();
			return owner.equals(lease.getString("owner"));
		}
	}

	/**
	 * Releases lock lease using DELETE query.
	 * Throws exception if query not applied, it means lock lease was lost.
	 */
	public void unlock() throws LockLeaseLostException {
		ResultSet rs = session.execute(deletePrep.bind(name, owner));
		if (!rs.wasApplied()) {
			throw new LockLeaseLostException();
		}
	}

	/**
	 * Updates current lock lease using UPDATE query.
	 * Throws exception if query not applied, it means lock lease was lost.
	 */
	public void keepAlive() throws LockLeaseLostException {
		ResultSet rs = session.execute(updatePrep.bind(owner, name, owner));
		if (!rs.wasApplied()) {
			throw new LockLeaseLostException();
		}
	}
}