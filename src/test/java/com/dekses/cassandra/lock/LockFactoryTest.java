package com.dekses.cassandra.lock;

import static org.junit.Assert.*;
import org.junit.Test;
import com.datastax.driver.core.exceptions.DriverException;

public class LockFactoryTest {

	@Test
	public void testInitialize() {
		LockFactory lf1 = new LockFactory("127.0.0.1", "casslock_test");
		assertNotNull(lf1);
		LockFactory lf2 = new LockFactory("localhost, 127.0.0.1", "casslock_test");
		assertNotNull(lf2);
	}
	
	@Test(expected=DriverException.class)
	public void testInitializeFailure() {
		LockFactory lf1 = new LockFactory("127.0.0.1", "casslock_test__");
		assertNull(lf1); // should not happen
	}
}
