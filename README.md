# cassandra-lock

Java implementation of distributed lock based on lightweight transactions in
Cassandra database.

Inspired by [Consensus on
Cassandra](http://www.datastax.com/dev/blog/consensus-on-cassandra)

## Installation

_TODO_

## Usage

### LockFactory

Create `lock_leases` table in your project keyspace, it's definition can be
found in `tables.cql` file. 

```
CREATE TABLE lock_leases (
    name text PRIMARY KEY,
    owner text,
    value text
) WITH default_time_to_live = 180;
```

In your Java code create `LockFactory` instance using existing Cassandra
session object or by providing cluster contact points and keyspace:

```java
// with session
LockFactory lockFactory = new LockFactory(session);

// with contact points and keyspace
LockFactory lockFactory = new LockFactory("127.0.0.1", "casslock_test");
```

Alternatively, if you prefer to use factory singleton object, you can
initialize it and then retrieve using `getInstance()`:

```java
// Initialize
// with session
LockFactory.initialize(session);

// with contact points and keyspace
LockFactory.initialize("127.0.0.1", "casslock_test");

// Retrieve factory object
LockFactory lockFactory = LockFactory.getInstance();
```

### Lock

Create new `Lock` instances using `LockFactory` object:

```java
Lock lock = lockFactory.getLock("my-resource");
if (lock.tryLock()) {
    try {
      // do something related to my-resource
      // ...

      // call periodically to keep lock
      lock.keepAlive();
    } finally {
      lock.unlock();
    }
} else {
    // Can not acquire lock on resource,
    // it's already taken by other owner/process
}
```
