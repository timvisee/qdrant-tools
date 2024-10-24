#!/bin/bash

set -e

QDRANT_BIN=${QDRANT_BIN:-./target/debug/qdrant}
STORAGE_BASE=/tmp/storage_
SNAPSHOTS_BASE=/tmp/snapshots_

if [[ $1 == "rm" ]]; then
    rm -rf $STORAGE_BASE* $SNAPSHOTS_BASE*
    echo "All storages removed!"
    exit 0
fi

ID=$1
IP=127.0.0.$ID

shift

export QDRANT__CLUSTER__ENABLED=true
export QDRANT__SERVICE__HOST=$IP
export QDRANT__STORAGE__STORAGE_PATH=$STORAGE_BASE$ID
export QDRANT__STORAGE__SNAPSHOTS_PATH=$SNAPSHOTS_BASE$ID

if [[ $ID == 1 ]]; then
    $QDRANT_BIN --uri http://127.0.0.1:6335 $@
else
    $QDRANT_BIN --bootstrap http://127.0.0.1:6335 --uri http://$IP:6335 $@
fi
