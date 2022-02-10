package com.dekses.cassandra.lock;

import static org.junit.Assert.*;

import com.datastax.oss.driver.api.core.DriverException;
import org.junit.Test;

public class LockFactoryTest extends BaseTest{

	@Test
	public void testInitialize() {
		LockFactory lf1 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf1);
		LockFactory lf2 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf2);
	}
	
	@Test(expected= DriverException.class)
	public void testInitializeFailure() {
		LockFactory lf1 = new LockFactory(CONTACT_POINT, "casslock_test__", LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNull(lf1); // should not happen
	}
}
