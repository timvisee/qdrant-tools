#!/bin/bash

set -e

FROM_ID=$1
FROM_IP=127.0.0.$FROM_ID
TO_ID=$2
TO_IP=127.0.0.$TO_ID
COLLECTION=benchmark

FROM_CLUSTER_DATA=$(curl -sX GET http://$FROM_IP:6333/collections/$COLLECTION/cluster)
TO_CLUSTER_DATA=$(curl -sX GET http://$TO_IP:6333/collections/$COLLECTION/cluster)

FROM_PEER_ID=$(echo $FROM_CLUSTER_DATA | jq .result.peer_id)
TO_PEER_ID=$(echo $TO_CLUSTER_DATA | jq .result.peer_id)

echo Moving shard 0 from $FROM_PEER_ID to $TO_PEER_ID

curl -sX POST http://$FROM_IP:6333/collections/$COLLECTION/cluster \
    -H "Content-Type: application/json" \
    --data "{
        \"move_shard\": {
            \"shard_id\": 0,
            \"from_peer_id\": $FROM_PEER_ID,
            \"to_peer_id\": $TO_PEER_ID,
            \"method\": \"wal_delta\"
        }
    }" | jq
