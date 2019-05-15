#!/usr/bin/env bash

CTOOL_ENV=ctool-env

pyenv activate ${CTOOL_ENV}

./setupPerfEnv.sh 500 json

# Produce 1_000_000_000 records to json-stream topic
ctool run kc-brokers 0 "cd kafka-examples/producers; mvn clean compile exec:java -Dexec.mainClass=json.JsonProducer"

CONNECT_FIRST_ADDRESS=`ctool info --public-ips kc-connect-l -n 0`
DSE_FIRST_ADDRESS=`ctool info --public-ips kc-dse -n 0`
DSE_SECOND_ADDRESS=`ctool info --public-ips kc-dse -n 1`
# Submit connector task
sed -i "s/{dse_contact_point_1}/$DSE_FIRST_ADDRESS/g" dse-sink.json
sed -i "s/{dse_contact_point_2}/$DSE_SECOND_ADDRESS/g" dse-sink.json
curl -X POST -H "Content-Type: application/json" -d @dse-sink.json "$CONNECT_FIRST_ADDRESS:8083/connectors"

# WAIT FOR COMPLETE && validate number of inserted records using DSBULK:
# ./verifyJsonDseTable.sh


truncate_json_dse_table(){
    ctool run kc-dse "cqlsh -e \"TRUNCATE stocks.ticks;\""
    ctool run kc-dse 'nodetool clearsnapshot --all'
}