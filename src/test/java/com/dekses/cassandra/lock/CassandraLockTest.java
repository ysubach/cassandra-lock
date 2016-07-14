package com.dekses.cassandra.lock;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraLockTest {

	private static final String HOST = "127.0.0.1";
	private static final String KEYSPACE = "casslock_test";
	
	private Session session;
	
	@Before
    public void setUp() {
		session = Cluster.builder().addContactPoint(HOST).build().connect();
	    session.execute("USE " + KEYSPACE);
	    session.execute("TRUNCATE lock_leases");
	}
	
	@After
	public void tearDown() {
	    session.close();
	}
	
	@Test
	public void testLockTrueFalse() {
		LockFactory lf1 = new LockFactory(HOST, KEYSPACE);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		LockFactory lf2 = new LockFactory(HOST, KEYSPACE);
		assertNotNull(lf2);
		Lock l2 = lf2.getLock("test");
		assertFalse(l2.tryLock());
	}

	@Test
	public void testLockTrueTrue() {
		LockFactory lf1 = new LockFactory(HOST, KEYSPACE);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		Lock l2 = lf1.getLock("test");
		assertTrue(l2.tryLock());
	}
	
	@Test
	public void testLockAndUnlock() {
		LockFactory lf1 = new LockFactory(HOST, KEYSPACE);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		LockFactory lf2 = new LockFactory(HOST, KEYSPACE);
		assertNotNull(lf2);
		Lock l2 = lf2.getLock("test");
		assertFalse(l2.tryLock());
		
		l1.unlock();
		assertTrue(l2.tryLock());
	}
	
	@Test(expected=LockLeaseLostException.class)
	public void testLockAndUnlockException() throws InterruptedException {
		LockFactory lf1 = new LockFactory(HOST, KEYSPACE);
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
		LockFactory lf1 = new LockFactory(HOST, KEYSPACE);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test");
		assertTrue(l1.tryLock());
		
		l1.keepAlive();
		assertTrue(true);
	}
	
	@Test(expected=LockLeaseLostException.class)
	public void testKeepAliveException() throws InterruptedException {
		LockFactory lf1 = new LockFactory(HOST, KEYSPACE);
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
		LockFactory lf1 = new LockFactory(HOST, KEYSPACE);
		assertNotNull(lf1);
		Lock l1 = lf1.getLock("test", 1);
		assertTrue(l1.tryLock());
		
		// lock loss due to TTL
	    Thread.sleep(1500);
		
		l1.keepAlive();
	}
}