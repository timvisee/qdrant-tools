#!/bin/bash

set -e

BIN=./target/debug/qdrant
STORAGE_BASE=./storage_

if [[ $1 == "rm" ]]; then
    rm -rf $STORAGE_BASE*
    echo "All storages removed!"
    exit 0
fi

ID=$1
IP=127.0.0.$ID

export QDRANT__CLUSTER__ENABLED=true
export QDRANT__SERVICE__HOST=$IP
export QDRANT__STORAGE__STORAGE_PATH=$STORAGE_BASE$ID

if [[ $ID == 1 ]]; then
    $BIN --uri http://127.0.0.1:6335
else
    $BIN --bootstrap http://127.0.0.1:6335 --uri http://$IP:6335
fi
