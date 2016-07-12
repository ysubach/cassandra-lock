package com.dekses.cassandra.lock;

import com.datastax.driver.core.ConsistencyLevel;
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
	 */
	public CassandraLock(Session session, String owner, String name) {
		this.session = session;
		this.owner = owner;
		this.name = name;
		insertPrep = session.prepare("INSERT INTO lock_leases (name, owner) VALUES (?,?) IF NOT EXISTS");
		insertPrep.setConsistencyLevel(ConsistencyLevel.QUORUM);
		selectPrep = session.prepare("SELECT * FROM lock_leases WHERE name = ?");
		selectPrep.setConsistencyLevel(ConsistencyLevel.SERIAL);
		deletePrep = session.prepare("DELETE FROM lock_leases where name = ? IF owner = ?");
		deletePrep.setConsistencyLevel(ConsistencyLevel.QUORUM);
		updatePrep = session.prepare("UPDATE lock_leases set owner = ? where name = ? IF owner = ?");
		updatePrep.setConsistencyLevel(ConsistencyLevel.QUORUM);
	}
	
	/**
	 * Try to acquire lock lease, uses INSERT query.
	 */
	public boolean tryLock() {
		ResultSet rs = session.execute(insertPrep.bind(name, owner));
		if (rs.wasApplied()) {
			return true;
		} else {
			Row lease = rs.one();
			return owner.equals(lease.getString("owner"));
		}
	}

	/**
	 * Releases lock lease using DELETE query.
	 */
	public void unlock() {
		session.execute(deletePrep.bind(name, owner));
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