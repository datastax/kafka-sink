📘 [Documentation](../README.md) > [Data Mapping](README.md) > Multiple Topics

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Multiple topics to multiple tables
 
Ingest multiple topics and write to different tables using a single connector
 instance.
 The DataStax Connector allows for mapping multiple topics to multiple tables in a
 single connector instance.
> **Note:** Note: Most Apache Kafka™ systems store all messages
 in the same format and Kafka Connect workers only support a single converter class for key
 and value.
 In the example `stocks_topic`, the key
 is a basic string and the value is regular JSON.
| key | value |
| --- | --- |
| APPLE | ```json {"symbol":"APPL", "value":208, "exchange":"NASDAQ", "industry":"TECH", "ts":"2018-11-26T19:26:27.483"} ``` |
| EXXON MOBIL | ```json {"symbol":"M", "value":80, "exchange":"NYSE", "industry":"ENERGY", "ts":"2018-11-26T19:26:27.483"} ``` |
| GENERAL MOTORS | ```json {"symbol":"GM", "value":38, "exchange":"NYSE", "industry":"AUTO", "ts":"2018-11-26T19:26:27.483"} ``` |
| AT&T | ```json {"symbol":"AT&T", "value":33, "exchange":"NYSE", "industry":"TELECOM", "ts":"2018-11-26T19:26:27.483"} ``` |
| FORD MOTOR | ```json {"symbol":"F", "value":10, "exchange":"NYSE", "industry":"AUTO", "ts":"2018-11-26T19:26:27.483"} ``` |
 And `baseball_topic` has the same structure, basic key and JSON
 value.
| key | value |
| --- | --- |
| redsox | ```json {"number":50, "firstname":"mookie", "lastname":"betts", "street":"4 yawkey way", "city":"boston"} ``` |
| redsox | ```json {"number":28, "firstname":"jd", "lastname":"martinez", "street":"4 yawkey way", "city":"boston"} ``` |
| redsox | ```json {"number":16, "firstname":"andrew", "lastname":"benintendi", "street":"4 yawkey way", "city":"boston"} ``` |
| redsox | ```json {"number":41, "firstname":"chris", "lastname":"sale", "street":"4 yawkey way", "city":"boston"} ``` |
| redsox | ```json {"number":24, "firstname":"david", "lastname":"price", "street":"4 yawkey way", "city":"boston"} ``` |
 
Map the topics into different keyspaces and tables,
 `stocks_keyspace.stocks_table` and
 `baseball_keyspace.baseball_table`.
 The DataStax Schema definitions:
- `stocks_table_by_symbol`
```cql
CREATE TABLE stocks_keyspace.stocks_table_by_symbol (
 symbol text, 
 ts timestamp, 
 exchange text, 
 industry text, 
 name text, 
 value double, 
 PRIMARY KEY (symbol, ts));
```
- ```cql
CREATE TABLE baseball_keyspace.baseball_table (
 team text primary key, 
 number int,
 firstname text, 
 lastname text, 
 street text, 
 city text);
```
 In the connector configuration, add the following settings:
- `"topics”: “stocks_topic,baseball_topic”`
- `stocks_topic` to
 `stocks_table_by_symbol`
```
“topic.stocks_topic.stocks_keyspace.stocks_table_by_symbol.mapping”:
“symbol=value.symbol, ts=value.dateTime, exchange=value.exchange, industry=value.industry, name=key.name, value=value.value”
```
- `baseball_topic` to `baseball_keyspace.baseball_table` 
```
“topic.baseball_topic.baseball_keyspace.baseball_table.mapping”: 
 “team=key, number=value.number, firstname=value.firstname, lastname=value.lastname, street=value.street, city=value.city”
```
