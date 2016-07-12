package com.dekses.cassandra.lock;

/**
 * Thrown if lock lease was lost by the owner, this can happen
 * due to client inability to update lease on time. 
 */
public class LockLeaseLostException extends IllegalMonitorStateException {

	/** Serial UID */
	private static final long serialVersionUID = 7515630326135527763L;

	/** Empty constructor */
	public LockLeaseLostException() {
		super();
	}
}
