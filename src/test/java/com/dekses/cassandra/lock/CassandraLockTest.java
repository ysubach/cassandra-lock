package com.dekses.cassandra.lock;

import static org.junit.Assert.*;


import org.junit.*;


public class CassandraLockTest extends BaseTest{
	
	@After
	public void tearDown() {
	    session.close();
	}
	
	@Test
	public void testLockTrueFalse() {
		LockFactory lf1 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		LockFactory lf2 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf2);
		Lock l2 = lf2.getLock("test");
		assertFalse(l2.tryLock());
	}

	@Test
	public void testLockTrueTrue() {
		LockFactory lf1 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		Lock l2 = lf1.getLock("test");
		assertTrue(l2.tryLock());
	}
	
	@Test
	public void testLockAndUnlock() {
		LockFactory lf1 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		LockFactory lf2 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf2);
		Lock l2 = lf2.getLock("test");
		assertFalse(l2.tryLock());
		
		l1.unlock();
		assertTrue(l2.tryLock());
	}
	
	@Test(expected=LockLeaseLostException.class)
	public void testLockAndUnlockException() throws InterruptedException {
		LockFactory lf1 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		// simulates lock loss due to TTL
	    session.execute("TRUNCATE lock_leases");
	    Thread.sleep(1000);
		
		l1.unlock();
	}
	
	@Test
	public void testKeepAlive() {
		LockFactory lf1 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		l1.keepAlive();
		assertTrue(true);
	}
	
	@Test(expected=LockLeaseLostException.class)
	public void testKeepAliveException() throws InterruptedException {
		LockFactory lf1 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		// simulates lock loss due to TTL
	    session.execute("TRUNCATE lock_leases");
	    Thread.sleep(1000);
		
		l1.keepAlive();
	}
	
	@Test(expected=LockLeaseLostException.class)
	public void testShortTTL() throws InterruptedException {
		LockFactory lf1 = new LockFactory(CONTACT_POINT, KEYSPACE, LOCAL_DC, CASSANDRA_USER, CASSANDRA_PASSWORD);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test", 1);
		assertTrue(l1.tryLock());
		
		// lock loss due to TTL
	    Thread.sleep(1500);
		
		l1.keepAlive();
	}
}