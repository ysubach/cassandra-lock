# cassandra-lock

Java implementation of distributed lock based on lightweight transactions in
Cassandra database.

Inspired by [Consensus on
Cassandra](http://www.datastax.com/dev/blog/consensus-on-cassandra)

## Installation

Add Maven dependency in your project:
```
<dependency>
  <groupId>com.dekses</groupId>
  <artifactId>cassandra-lock</artifactId>
  <version>0.0.2</version>
</dependency>
```

More details for other build systems and latest version available at
[Maven Central 
repository](https://search.maven.org/artifact/com.dekses/cassandra-lock).

## Usage

### LockFactory

Create `lock_leases` table in your project keyspace, it's definition can be
found in `tables.cql` file. 

```
CREATE TABLE lock_leases (
    name text PRIMARY KEY,
    owner text,
    value text
) WITH default_time_to_live = 60;
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

### TTL (time to live)

Each lock lease has TTL measured in seconds. It is 60 seconds by default, if
lease is not updated during this interval using keepAlive() method call it will
be automatically removed.

You can change default TTL for LockFactory, it affects every lock created by
this factory (excluding locks that already exist).

```java
// local object
lockFactory.setDefaultTTL(120);

// singleton
LockFactory.getInstance().setDefaultTTL(120);
```

Also TTL can be specified for each individual lock object.

```java
Lock lock = lockFactory.getLock("my-resource", 120);
```
