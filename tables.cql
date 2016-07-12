DROP KEYSPACE IF EXISTS casslock_test;

CREATE KEYSPACE casslock_test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
  AND DURABLE_WRITES = true;

USE casslock_test;

CREATE TABLE lock_leases (
    name text PRIMARY KEY,
    owner text,
    value text
) WITH default_time_to_live = 180;
