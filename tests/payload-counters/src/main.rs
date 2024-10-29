use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{
    CollectionStatus, CreateCollectionBuilder, GetPointsBuilder, OptimizersConfigDiffBuilder,
    PointStruct, ReplicateShardBuilder, ScrollPointsBuilder, ShardTransferMethod,
    UpdateCollectionBuilder, UpdateCollectionClusterSetupRequestBuilder, UpsertPointsBuilder,
    VectorParamsBuilder,
};
use qdrant_client::qdrant::{Distance, PointId};
use qdrant_client::Qdrant;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};

const COLLECTION_NAME: &str = "benchmark";
const SHARD_COUNT: u32 = 1;
const SEGMENT_COUNT: u64 = 3;
const REPLICATION_FACTOR: u32 = HOSTS.len() as u32;
const WRITE_CONSISTENCY_FACTOR: u32 = 1;
// const WRITE_CONSISTENCY_FACTOR: u32 = REPLICATION_FACTOR - 1;
// const BATCH_SIZE: usize = 250;
const BATCH_SIZE: usize = 50;
// const INDEXING_THRESHOLD: u64 = 1;
const INDEXING_THRESHOLD: u64 = 1;
// const POINT_COUNT: u64 = 20_000;
const POINT_COUNT: u64 = 200;
const SHUFFLE_POINTS: bool = false;
// const DIM: u64 = 128;
const DIM: u64 = 1;
const COUNTER_KEY: &str = "counter";
const WAIT: bool = true;
const ALWAYS_CHECK: bool = true;
const TRANSFERS: bool = true;
const TRANSFER_METHODS: &[ShardTransferMethod] = &[
    // ShardTransferMethod::StreamRecords,
    // ShardTransferMethod::Snapshot,
    ShardTransferMethod::WalDelta,
];
const CANCEL_OPTIMIZERS: bool = false;
const SCROLL: bool = false;
const UPDATE_RETRIES: u32 = 5;
const UPDATE_RETRY_INTERVAL: Duration = Duration::from_millis(50);

const WAIT_GREEN: bool = false;
const COLLECTION_POLL_INTERVAL: Duration = Duration::from_millis(50);
const COLLECTION_POLL_MAX: Duration = Duration::from_secs(120);

const HOSTS: &[&str] = &[
    "http://127.0.0.1:6334",
    "http://127.0.0.2:6334",
    "http://127.0.0.3:6334",
];
const API_KEY: Option<&str> = None;

#[tokio::main]
async fn main() {
    let clients = build_clients();

    println!("Set up collection");
    delete_collection(&clients[0]).await;
    create_collection(&clients[0]).await;

    if TRANSFERS {
        tokio::spawn(run_transfers(Arc::clone(&clients)));
    }

    if CANCEL_OPTIMIZERS {
        tokio::spawn(run_cancel_optimizers(Arc::clone(&clients)));
    }

    for round in 0.. {
        println!("Touch points: {round} -> {}", round + 1);
        touch_points(&clients).await;

        if ALWAYS_CHECK || round % 10 == 0 || round < 5 {
            let mut errors = vec![];
            for (i, client) in clients.iter().enumerate() {
                println!("Check points {}: expect {}", HOSTS[i], round + 1);

                let result = check_points(client, round as i64 + 1).await;

                if let Err(err) = result {
                    errors.push(format!("- {}: {err}", HOSTS[i]));
                }
            }

            if !errors.is_empty() {
                panic!("\n!!!INCONSISTENCY!!!\n{}", errors.join("\n"));
            }
        }
    }

    println!("Done");
}

fn build_clients() -> Arc<Vec<Qdrant>> {
    let clients = HOSTS
        .iter()
        .map(|host| {
            let mut client = Qdrant::from_url(host)
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(20));
            if let Some(api_key) = API_KEY {
                client = client.api_key(api_key);
            }
            client.build().expect("failed to create client")
        })
        .collect();
    Arc::new(clients)
}

async fn delete_collection(client: &Qdrant) {
    let result = client.delete_collection(COLLECTION_NAME).await;
    if let Err(err) = result {
        eprintln!("Failed to delete collection: {:?}", err);
    }
}

async fn create_collection(client: &Qdrant) {
    client
        .create_collection(
            CreateCollectionBuilder::new(COLLECTION_NAME)
                .vectors_config(VectorParamsBuilder::new(DIM, Distance::Cosine).on_disk(true))
                .optimizers_config(
                    OptimizersConfigDiffBuilder::default()
                        .default_segment_number(SEGMENT_COUNT)
                        // .max_optimization_threads(0),
                        .indexing_threshold(INDEXING_THRESHOLD),
                )
                .shard_number(SHARD_COUNT)
                .replication_factor(REPLICATION_FACTOR)
                .write_consistency_factor(WRITE_CONSISTENCY_FACTOR),
        )
        .await
        .expect("failed to create collection");

    let ids = (0..POINT_COUNT).collect::<Vec<_>>();

    for batch_ids in ids.chunks(BATCH_SIZE) {
        let points = batch_ids
            .iter()
            .map(|id| {
                let mut vector = vec![0.0; DIM as usize];
                thread_rng().fill(&mut vector[..]);
                PointStruct::new(*id, vector, [(COUNTER_KEY, 0i64.into())])
            })
            .collect::<Vec<_>>();

        client
            .upsert_points(UpsertPointsBuilder::new(COLLECTION_NAME, points).wait(WAIT))
            .await
            .expect("failed to upsert points");
    }
}

async fn touch_points(clients: &[Qdrant]) {
    if SCROLL && SHUFFLE_POINTS {
        panic!("SCROLL and SHUFFLE_POINTS are incompatible");
    }

    let mut ids = (0..POINT_COUNT).collect::<Vec<_>>();
    if SHUFFLE_POINTS {
        ids.shuffle(&mut thread_rng());
    }

    for batch_ids in ids.chunks(BATCH_SIZE) {
        let client_index = thread_rng().gen_range(0..clients.len());
        let client = &clients[client_index];

        let payload_values = if SCROLL {
            let (first, len) = (batch_ids[0], batch_ids.len() as u32);
            debug_assert!(batch_ids.windows(2).all(|n| n[0] == n[1] - 1));

            let response = client
                .scroll(
                    ScrollPointsBuilder::new(COLLECTION_NAME)
                        .offset(first)
                        .limit(len)
                        .with_vectors(false)
                        .with_payload(true),
                )
                .await
                .expect("failed to get existing points");

            response
                .result
                .into_iter()
                .map(|point| {
                    (
                        point_num(&point.id.unwrap()),
                        point.payload[COUNTER_KEY].as_integer().unwrap(),
                    )
                })
                .collect::<HashMap<_, _>>()
        } else {
            let response = client
                .get_points(
                    GetPointsBuilder::new(
                        COLLECTION_NAME,
                        batch_ids
                            .iter()
                            .map(|id| PointId::from(*id))
                            .collect::<Vec<_>>(),
                    )
                    .with_vectors(false)
                    .with_payload(true),
                )
                .await
                .expect("failed to get existing points");

            response
                .result
                .into_iter()
                .map(|point| {
                    (
                        point_num(&point.id.unwrap()),
                        point.payload[COUNTER_KEY].as_integer().unwrap(),
                    )
                })
                .collect::<HashMap<_, _>>()
        };

        let points = batch_ids
            .iter()
            .map(|id| {
                let mut vector = vec![0.0; DIM as usize];
                thread_rng().fill(&mut vector[..]);
                PointStruct::new(
                    *id,
                    vector,
                    [(COUNTER_KEY, (payload_values[id] + 1).into())],
                )
            })
            .collect::<Vec<_>>();

        for retries_left in (0..UPDATE_RETRIES).rev() {
            let result = client
                .upsert_points(UpsertPointsBuilder::new(COLLECTION_NAME, points.clone()).wait(WAIT))
                .await;

            match result {
                Ok(_) => break,
                Err(err) if retries_left > 0 => {
                    println!("Failed to upsert points ({retries_left} retries left, client {client_index}): {err}");
                    tokio::time::sleep(UPDATE_RETRY_INTERVAL).await;
                }
                Err(err) => panic!("failed to upsert points: {err}"),
            }
        }
    }
}

async fn wait_for_green(client: &Qdrant) {
    let start = Instant::now();

    while start.elapsed() <= COLLECTION_POLL_MAX {
        let info = client
            .collection_info(COLLECTION_NAME)
            .await
            .expect("failed to get collection info");

        if info.result.expect("missing collection info").status() == CollectionStatus::Green {
            return;
        }

        std::thread::sleep(COLLECTION_POLL_INTERVAL);
    }

    panic!("Timeout waiting for green status");
}

async fn wait_for_transfer_count(client: &Qdrant, count: usize) {
    let start = Instant::now();

    while start.elapsed() <= COLLECTION_POLL_MAX {
        let cluster_info = client
            .collection_cluster_info(COLLECTION_NAME)
            .await
            .expect("failed to get collection cluster info");

        if cluster_info.shard_transfers.len() == count {
            return;
        }

        std::thread::sleep(COLLECTION_POLL_INTERVAL);
    }

    panic!("Timeout waiting for transfer count");
}

async fn check_points(client: &Qdrant, expected: i64) -> Result<(), String> {
    if WAIT_GREEN {
        wait_for_green(client).await;
    }

    let ids = (0..POINT_COUNT).collect::<Vec<_>>();

    for batch_ids in ids.chunks(BATCH_SIZE) {
        let response = client
            .get_points(
                GetPointsBuilder::new(
                    COLLECTION_NAME,
                    batch_ids
                        .iter()
                        .map(|id| PointId::from(*id))
                        .collect::<Vec<_>>(),
                )
                .with_vectors(false)
                .with_payload(true),
            )
            .await
            .expect("failed to get existing points");

        for point in response.result {
            let num = point.payload[COUNTER_KEY].as_integer().unwrap();
            if num != expected {
                return Err(format!(
                    "PAYLOAD COUNTER MISMATCH: {num} != {expected} (point {:?})",
                    point_num(&point.id.unwrap()),
                ));
            }
        }
    }

    Ok(())
}

async fn run_transfers(clients: Arc<Vec<Qdrant>>) {
    if clients.len() < 2 {
        return;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    loop {
        let shard_id = 0;
        let clients: Vec<_> = clients
            .choose_multiple(&mut rand::thread_rng(), 2)
            .collect();
        let [from, to] = &clients[..] else {
            unreachable!()
        };

        let method = TRANSFER_METHODS.choose(&mut rand::thread_rng()).unwrap();

        let from_peer_id = from
            .collection_cluster_info(COLLECTION_NAME)
            .await
            .expect("failed to get collection cluster info")
            .peer_id;
        let to_peer_id = to
            .collection_cluster_info(COLLECTION_NAME)
            .await
            .expect("failed to get collection cluster info")
            .peer_id;

        // Start transfer
        println!("Transfer {from_peer_id}:{shard_id} -> {to_peer_id}:{shard_id} ({method:?})",);
        let response = from
            .update_collection_cluster_setup(UpdateCollectionClusterSetupRequestBuilder::new(
                COLLECTION_NAME,
                ReplicateShardBuilder::new(shard_id, from_peer_id, to_peer_id).method(*method),
            ))
            .await;
        if let Err(err) = response {
            println!("Failed to start shard transfer: {err}");
            wait_for_transfer_count(from, 0).await;
            continue;
        }

        // Wait for transfer start and completion
        wait_for_transfer_count(from, 1).await;
        wait_for_transfer_count(from, 0).await;
    }
}

async fn run_cancel_optimizers(clients: Arc<Vec<Qdrant>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let client = clients.choose(&mut rand::thread_rng()).unwrap();

        println!("Cancel optimizers");
        client
            .update_collection(UpdateCollectionBuilder::new(COLLECTION_NAME))
            .await
            .expect("failed to cancel optimizers");
    }
}

fn point_num(id: &PointId) -> u64 {
    match id.point_id_options.as_ref().unwrap() {
        PointIdOptions::Num(num) => *num,
        PointIdOptions::Uuid(_) => unreachable!(),
    }
}
