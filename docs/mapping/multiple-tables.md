---
title: Mapping a topic to multiple tables
source: kafkaMapMultipleTables.html
---

📘 [Documentation](../README.md) > [Data Mapping](README.md) > Multiple Tables

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Mapping a topic to multiple tables
 
Ingest a single topic into multiple tables using a single connector
 instance.
 
The DataStax Connector allows for mapping a single topic to multiple tables for a
 single connector instance. 
 In the example `stocks_topic`, the key
 is a basic string and the value is regular JSON.
| key | value |
| --- | --- |
| APPLE | ```json {"symbol":"APPL", "value":208, "exchange":"NASDAQ", "industry":"TECH", "ts":"2018-11-26T19:26:27.483"} ``` |
| EXXON MOBIL | ```json {"symbol":"M", "value":80, "exchange":"NYSE", "industry":"ENERGY", "ts":"2018-11-26T19:26:27.483"} ``` |
| GENERAL MOTORS | ```json {"symbol":"GM", "value":38, "exchange":"NYSE", "industry":"AUTO", "ts":"2018-11-26T19:26:27.483"} ``` |
| AT&T | ```json {"symbol":"AT&T", "value":33, "exchange":"NYSE", "industry":"TELECOM", "ts":"2018-11-26T19:26:27.483"} ``` |
| FORD MOTOR | ```json {"symbol":"F", "value":10, "exchange":"NYSE", "industry":"AUTO", "ts":"2018-11-26T19:26:27.483"} ``` |
 In the DataStax keyspace `stocks_keyspace`, create three different
 tables that optimized with different schemas.
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
- `stocks_table_by_exhange`
```cql
CREATE TABLE stocks_keyspace.stocks_table_by_exchange (
 symbol text, 
 ts timestamp,
 exchange text, 
 industry text, 
 name text, 
 value double, 
 PRIMARY KEY (exchange, ts));
```
- `stocks_table_by_industry`
```cql
CREATE TABLE stocks_keyspace.stocks_table_by_industry (
 symbol text, 
 ts timestamp,
 exchange text, 
 industry text, 
 name text, 
 value double, 
 PRIMARY KEY (industry, ts));
```
 Configure the connector and add all the map specifications:
- `stocks_topic` to
 `stocks_table_by_symbol`
```
“topic.stocks_topic.stocks_keyspace.stocks_table_by_symbol.mapping”:
“symbol=value.symbol, ts=value.ts, exchange=value.exchange, industry=value.industry, name=key.name, value=value.value”
```
- `stocks_topic` to
 `stocks_table_by_exchange`
```
“topic.stocks_topic.stocks_keyspace.stocks_table_by_exchange.mapping”:
 “symbol=value.symbol, ts=value.ts, exchange=value.exchange, industry=value.industry, name=key.name, value=value.value”
```
- `stocks_topic` to
 `stocks_table_by_industry`
```
“topic.stocks_topic.stocks_keyspace.stocks_table_by_industry.mapping”:
 “symbol=value.symbol, ts=value.ts, exchange=value.exchange, industry=value.industry, name=key.name, value=value.value”
```
