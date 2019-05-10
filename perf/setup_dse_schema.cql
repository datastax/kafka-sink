CREATE KEYSPACE stocks WITH replication = {'class': 'NetworkTopologyStrategy', 'kc-dc': 3};
CREATE TABLE stocks.ticks (symbol text, ts timestamp, exchange text, industry text, name text, value double, PRIMARY KEY (symbol, ts));
