package com.dekses.cassandra.lock;

/**
 * Generalized lock interface 
 */
public interface Lock {

	/**
	 * Try to acquire lock lease
	 * @return True if acquired, False if not (lease taken already) 
	 */
	boolean tryLock();

	/**
	 * Releases current lock lease
	 * @throws LockLeaseLostException Thrown in case lock lease lost by the owner
	 */
	void unlock() throws LockLeaseLostException;

	/**
	 * Updates current lock lease
	 * @throws LockLeaseLostException Thrown in case lock lease lost by the owner
	 */
	void keepAlive() throws LockLeaseLostException;
	
	/** @return Lock owner */
	String getOwner();
	
	/** @return Lock resource name */
	String getName();

	/** @return Lock TTL */
	int getTTL();
}
