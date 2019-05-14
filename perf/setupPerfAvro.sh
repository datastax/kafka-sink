#!/usr/bin/env bash

CTOOL_ENV=ctool-env

pyenv activate ${CTOOL_ENV}

./setupPerfAvro.sh 100 avro

# Produce 200_000_000 avro record
ctool run kc-brokers 0 "confluent/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 2 --partitions 100 --topic avro-stream --config retention.ms=-1 delete.topic.enable=true"
ctool run kc-brokers 0 "cd kafka-examples/producers; mvn clean compile exec:java -Dexec.mainClass=avro.AvroProducer -Dexec.args=\"avro-stream 200000000\""

# Restart script

start_avro_test


start_avro_test(){
    CONNECT_FIRST_ADDRESS=`ctool info --public-ips kc-connect-l -n 0`
    DSE_FIRST_ADDRESS=`ctool info --public-ips kc-dse -n 0`
    DSE_SECOND_ADDRESS=`ctool info --public-ips kc-dse -n 1`
    # Submit connector task
    sed -i "s/{dse_contact_point_1}/$DSE_FIRST_ADDRESS/g" dse-sink-avro.json
    sed -i "s/{dse_contact_point_2}/$DSE_SECOND_ADDRESS/g" dse-sink-avro.json
    curl -X POST -H "Content-Type: application/json" -d @dse-sink-avro.json "$CONNECT_FIRST_ADDRESS:8083/connectors"

    # WAIT FOR COMPLETE && validate number of inserted records using DSBULK:
    # ctool run kc-dse 0 "dse/bin/dsbulk count -k kafka_examples -t avro_udt_table"
}


truncate_avro_dse_table(){
    ctool run kc-dse "cqlsh -e \"TRUNCATE kafka_examples.avro_udt_table;\""
    ctool run kc-dse 'nodetool clearsnapshot --all'
}
