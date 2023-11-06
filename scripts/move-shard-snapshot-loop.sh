#!/bin/bash

# On a cluster, in a loop, move shard from `test` collection to another peer using shard
# snapshot transfer and back.
#
# Start first node:
# $ QDRANT__LOG_LEVEL=collection::shards=trace,actix_web=warn,debug QDRANT__CLUSTER__ENABLED=true mold -run cargo run -- --uri http://localhost:6335
#
# Start second node:
# $ QDRANT__LOG_LEVEL=collection::shards=trace,actix_web=warn,debug QDRANT__CLUSTER__ENABLED=true QDRANT__SERVICE__HTTP_PORT=6343 QDRANT__SERVICE__GRPC_PORT=6344 QDRANT__CLUSTER__P2P__PORT=6345 QDRANT__STORAGE__STORAGE_PATH=./storage2 mold -run cargo run -- --bootstrap http://localhost:6335
#
# Run test command:
# $ bfb --collection-name test -n 10000 --indexing-threshold 0 --skip-wait-index && ./move-shard-snapshot-loop.sh
#
# Clean up after testing:
# rm -r storage{,2}

QDRANT_HOST="localhost:6333"
COLLECTION=test

CLUSTER=$(curl -L -X GET "http://$QDRANT_HOST/collections/$COLLECTION/cluster" \
  -H 'Content-Type: application/json' \
  --fail -s)

MOVE1=$(echo $CLUSTER | jq '{
    "move_shard": {
        "from_peer_id": .result.peer_id,
        "shard_id": .result.local_shards[0].shard_id,
        "to_peer_id": .result.remote_shards[0].peer_id,
        "method": "snapshot"
    }
}')
MOVE2=$(echo $CLUSTER | jq '{
    "move_shard": {
        "from_peer_id": .result.remote_shards[0].peer_id,
        "shard_id": .result.local_shards[0].shard_id,
        "to_peer_id": .result.peer_id,
        "method": "snapshot"
    }
}')

function move1 {
    echo Requesting shard move: $MOVE1
    curl -X POST "http://$QDRANT_HOST/collections/$COLLECTION/cluster" \
        -H 'Content-Type: application/json' \
        --fail -s \
        --data-raw "$MOVE1" | jq
}

function move2 {
    echo Requesting shard move: $MOVE2
    curl -X POST "http://$QDRANT_HOST/collections/$COLLECTION/cluster" \
        -H 'Content-Type: application/json' \
        --fail -s \
        --data-raw "$MOVE2" | jq
}

function wait_no_transfers {
    while true; do
        count=$(curl -L -X GET "http://$QDRANT_HOST/collections/$COLLECTION/cluster" \
            -H 'Content-Type: application/json' \
            --fail -s | jq '.result.shard_transfers | length')
        if [[ $count == '0' ]]; then
            break
        fi
        sleep 0.5s
    done
}

function updates {
    for ((i=1; ;++i))
    do
        curl -L -X PUT "http://$QDRANT_HOST/collections/$COLLECTION/points?wait=true" \
            -H 'Content-Type: application/json' \
            --fail -s \
            --data-raw "{\"points\": [
                {\"id\": $i, \"vector\": [$i.0, ${RANDOM}.0, ${RANDOM}.0, ${RANDOM}.0, ${RANDOM}.0, ${RANDOM}.0, ${RANDOM}.0, ${RANDOM}.0, ${RANDOM}.0, ${RANDOM}.0, 7.0, 2.0, 1.0, 3.0, 2.0, 9.0, 1.0, 0.0, 6.0, 3.0, 2.0, 2.0, 2.0, 5.0, 3.0, 4.0, 8.0, 1.0, 7.0, 4.0, 1.0, 1.0, 6.0, 5.0, 0.0, 2.0, 8.0, 9.0, 9.0, 6.0, 4.0, 1.0, 6.0, 2.0, 0.0, 9.0, 6.0, 2.0, 5.0, 2.0, 9.0, 3.0, 5.0, 9.0, 9.0, 4.0, 1.0, 6.0, 4.0, 4.0, 9.0, 3.0, 1.0, 5.0, 5.0, 1.0, 7.0, 8.0, 0.0, 8.0, 9.0, 5.0, 9.0, 2.0, 2.0, 7.0, 7.0, 8.0, 7.0, 6.0, 5.0, 2.0, 1.0, 8.0, 7.0, 9.0, 9.0, 4.0, 3.0, 4.0, 7.0, 6.0, 1.0, 7.0, 9.0, 9.0, 1.0, 3.0, 8.0, 5.0, 9.0, 5.0, 8.0, 1.0, 7.0, 8.0, 7.0, 6.0, 9.0, 2.0, 0.0, 2.0, 4.0, 0.0, 1.0, 9.0, 8.0, 5.0, 5.0, 7.0, 9.0, 4.0, 8.0, 7.0, 0.0, 1.0, 9.0, 6.0]}
            ]}" | jq
        sleep 0.5
    done
}

# Update points twice a second forever
updates &

# Loop transfer forever
while true; do
    move1
    sleep 1
    wait_no_transfers
    move2
    sleep 1
    wait_no_transfers
done
