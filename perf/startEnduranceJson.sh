#!/usr/bin/env bash

CTOOL_ENV=ctool-env

pyenv activate ${CTOOL_ENV}

./setupPerfEnv.sh 500 json

BROKER_FIRST_ADDRESS=`ctool info --public-ips kc-brokers -n 0`
BROKER_SECOND_ADDRESS=`ctool info --public-ips kc-brokers -n 1`
BROKER_THIRD_ADDRESS=`ctool info --public-ips kc-brokers -n 2`

# Produce 20_000 records per second to json-stream topic
KC_BROKERS_LIST=${BROKER_FIRST_ADDRESS}:9092,${BROKER_SECOND_ADDRESS}:9092,${BROKER_THIRD_ADDRESS}:9092
ctool run kc-brokers 0 "cd kafka-examples/producers; mvn clean compile exec:java -Dexec.mainClass=json.InfiniteJsonProducer -Dexec.args=\"20000 json-stream $KC_BROKERS_LIST\" &> infinite-json-producer.log &"

start_json_test

start_json_test(){
    CONNECT_FIRST_ADDRESS=`ctool info --public-ips kc-connect-l -n 0`
    DSE_FIRST_ADDRESS=`ctool info --public-ips kc-dse -n 0`
    DSE_SECOND_ADDRESS=`ctool info --public-ips kc-dse -n 1`
    # Submit connector task
    sed -i "s/{dse_contact_point_1}/$DSE_FIRST_ADDRESS/g" dse-sink.json
    sed -i "s/{dse_contact_point_2}/$DSE_SECOND_ADDRESS/g" dse-sink.json
    curl -X POST -H "Content-Type: application/json" -d @dse-sink.json "$CONNECT_FIRST_ADDRESS:8083/connectors"

    # WAIT FOR COMPLETE && validate number of inserted records using DSBULK:
    # ctool run kc-dse 0 "dse/bin/dsbulk count -k stocks -t ticks"
}

truncate_json_dse_table(){
    ctool run kc-dse "cqlsh -e \"TRUNCATE stocks.ticks;\""
    ctool run kc-dse 'nodetool clearsnapshot --all'
}