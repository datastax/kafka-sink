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

create table if not exists pk_value (
    my_pk bigint primary key,
    my_value boolean
);

create table if not exists small_simple (
    bigintcol bigint primary key,
    booleancol boolean,
    intcol int
);

create table if not exists small_compound (
    bigintcol bigint,
    booleancol boolean,
    intcol int,
    primary key (bigintcol, booleancol)
);

create type if not exists myudt (udtmem1 int, udtmem2 text);
create type if not exists mycomplexudt (a int, b text, c list<int>);
create type if not exists mybooleanudt (udtmem1 boolean, udtmem2 text);

create table if not exists types (
    bigintcol bigint primary key, 
    booleancol boolean, 
    doublecol double, 
    floatcol float, 
    intcol int, 
    smallintcol smallint, 
    textcol text, 
    tinyintcol tinyint, 
    mapcol map<text, int>, 
    mapnestedcol frozen<map<text, map<int, text>>>, 
    listcol list<int>, 
    listnestedcol frozen<list<set<int>>>, 
    setcol set<int>, 
    setnestedcol frozen<set<list<int>>>, 
    tuplecol tuple<smallint, int, int>, 
    udtcol frozen<myudt>, 
    udtfromlistcol frozen<myudt>, 
    booleanudtcol frozen<mybooleanudt>, 
    booleanudtfromlistcol frozen<mybooleanudt>, 
    listudtcol frozen<mycomplexudt>, 
    blobcol blob, 
    datecol date, 
    timecol time, 
    timestampcol timestamp, 
    secondscol timestamp,
    loaded_at timeuuid,
    loaded_at2 timeuuid
);

create table if not exists pk_value_with_timeuuid (
    my_pk bigint PRIMARY KEY,
    my_value boolean,
    loaded_at timeuuid
);

create table if not exists types_with_frozen (
    bigintCol bigint PRIMARY KEY,
    udtCol frozen<myudt>,
    udtColNotFrozen myudt
);
