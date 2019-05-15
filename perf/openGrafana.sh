#!/usr/bin/env bash
CTOOL_ENV=ctool-env

pyenv activate ${CTOOL_ENV}

GRAPHITE_ADDRESS=`ctool info --public-ips kc-perf -n 0`
open "http://${GRAPHITE_ADDRESS}:3000"
