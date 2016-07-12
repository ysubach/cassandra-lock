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
	 */
	void unlock();

	/**
	 * Updates current lock lease
	 * @throws LockLeaseLostException Thrown in case lock lease lost by the owner
	 */
	void keepAlive() throws LockLeaseLostException;
}
