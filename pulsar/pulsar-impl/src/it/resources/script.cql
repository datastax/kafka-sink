create keyspace testks with replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
use testks;

create table if not exists testtbl (
    part text,
    id uuid,
    num int,
    fact boolean,
    added timeuuid,
    primary key((part),id)
);





