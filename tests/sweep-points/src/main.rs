use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, DeletePointsBuilder, OptimizersConfigDiffBuilder, PointStruct,
    ReplicateShardBuilder, ScrollPointsBuilder, ShardTransferMethod, UpdateCollectionBuilder,
    UpdateCollectionClusterSetupRequestBuilder, UpsertPointsBuilder, VectorParamsBuilder,
};
use qdrant_client::qdrant::{Distance, PointId};
use qdrant_client::Qdrant;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use tokio::sync::Mutex;

const COLLECTION_NAME: &str = "benchmark";
const SHARD_COUNT: u32 = 1;
const SEGMENT_COUNT: u64 = 3;
const REPLICATION_FACTOR: u32 = HOSTS.len() as u32;
const WRITE_CONSISTENCY_FACTOR: u32 = 1;
const BATCH_SIZE: usize = 25;
const INDEXING_THRESHOLD: u64 = 1;
const POINT_COUNT: u64 = 200;
const SHUFFLE_POINTS: bool = false;
const DIM: u64 = 128;
const PAYLOAD_KEY: &str = "key";
const WAIT: bool = true;
const DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6f";
const TRANSFERS: bool = true;
const TRANSFER_METHODS: &[ShardTransferMethod] = &[
    ShardTransferMethod::StreamRecords,
    // ShardTransferMethod::Snapshot,
    ShardTransferMethod::WalDelta,
];
const CANCEL_OPTIMIZERS: bool = false;
const UPDATE_RETRIES: u32 = 100;
const UPDATE_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const CHECK_RETRIES: usize = 25;
const CHECK_RETRY_DELAY: Duration = Duration::from_millis(100);
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

    let transfer_lock = Arc::new(Mutex::new(()));

    if TRANSFERS {
        tokio::spawn(run_transfers(
            Arc::clone(&clients),
            Arc::clone(&transfer_lock),
        ));
    }

    if CANCEL_OPTIMIZERS {
        tokio::spawn(run_cancel_optimizers(Arc::clone(&clients)));
    }

    for round in 0.. {
        let sweep_start = POINT_COUNT * round;

        sweep_points(&clients, sweep_start).await;

        if let Err(err) = check_points(&clients, sweep_start, CHECK_RETRIES, &transfer_lock).await {
            panic!("\n!!!INCONSISTENCIES AFTER {CHECK_RETRIES} ATTEMPTS!!!\n{err}\n");
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
}

async fn sweep_points(clients: &[Qdrant], sweep_start: u64) {
    let delete_range = sweep_start.saturating_sub(POINT_COUNT)..sweep_start;
    let upsert_range = sweep_start..sweep_start + POINT_COUNT;

    println!("Sweep points: delete {delete_range:?}, upsert {upsert_range:?}",);

    let mut delete_ids = delete_range.collect::<Vec<_>>();
    let mut upsert_ids = upsert_range.collect::<Vec<_>>();

    if SHUFFLE_POINTS {
        delete_ids.shuffle(&mut thread_rng());
        upsert_ids.shuffle(&mut thread_rng());
    }

    // Delete points first
    for batch_ids in delete_ids.chunks(BATCH_SIZE) {
        let client_index = thread_rng().gen_range(0..clients.len());
        let client = &clients[client_index];

        for retries_left in (0..UPDATE_RETRIES).rev() {
            let point_ids: Vec<PointId> = batch_ids.iter().cloned().map(PointId::from).collect();

            let result = client
                .delete_points(
                    DeletePointsBuilder::new(COLLECTION_NAME)
                        .points(point_ids)
                        .wait(WAIT),
                )
                .await;

            match result {
                Ok(_) => break,
                Err(err) if retries_left > 0 => {
                    println!("Failed to delete points ({retries_left} retries left, client {client_index}): {err}");
                    tokio::time::sleep(UPDATE_RETRY_INTERVAL).await;
                }
                Err(err) => panic!("failed to delete points: {err}"),
            }
        }
    }

    // Then upsert new points
    for batch_ids in upsert_ids.chunks(BATCH_SIZE) {
        let client_index = thread_rng().gen_range(0..clients.len());
        let client = &clients[client_index];

        let points = batch_ids
            .iter()
            .map(|id| {
                let mut vector = vec![0.0; DIM as usize];
                thread_rng().fill(&mut vector[..]);
                PointStruct::new(
                    *id,
                    vector,
                    [(PAYLOAD_KEY, thread_rng().gen::<i64>().into())],
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

        tokio::time::sleep(COLLECTION_POLL_INTERVAL).await;
    }

    panic!("Timeout waiting for transfer count");
}

async fn check_points(
    clients: &[Qdrant],
    sweep_start: u64,
    attempts: usize,
    transfer_lock: &Mutex<()>,
) -> Result<(), String> {
    let range = sweep_start..sweep_start + POINT_COUNT;
    let mut errors = vec![];
    let mut transfer_lock_guard = None;

    for retries_left in (0..attempts).rev() {
        errors.clear();

        for (i, client) in clients.iter().enumerate() {
            println!("Check points {}: expect {range:?}", HOSTS[i]);

            let time = Utc::now();
            let result = check_points_on_peer(client, sweep_start).await;

            if let Err(err) = result {
                errors.push(format!(
                    "- {} {}: {err}",
                    time.format(DATETIME_FORMAT),
                    HOSTS[i],
                ));
            }
        }

        if errors.is_empty() {
            return Ok(());
        }

        println!("Got inconsistencies:\n{}", errors.join("\n"));

        // Block transfers until we are consistent
        if transfer_lock_guard.is_none() {
            transfer_lock_guard.replace(transfer_lock.lock().await);
        }

        if retries_left > 0 {
            tokio::time::sleep(CHECK_RETRY_DELAY).await;
        }
    }

    Err(errors.join("\n"))
}

async fn check_points_on_peer(client: &Qdrant, sweep_start: u64) -> Result<(), String> {
    let records = client
        .scroll(
            ScrollPointsBuilder::new(COLLECTION_NAME)
                .with_vectors(false)
                .with_payload(true)
                .limit(u32::MAX - 1),
        )
        .await
        .expect("failed to scroll")
        .result;

    let mut ids: Vec<u64> = records
        .into_iter()
        .map(|record| point_num(record.id.as_ref().expect("missing point ID")))
        .collect();
    ids.sort_unstable();

    if ids.is_empty() {
        let range = sweep_start..sweep_start + POINT_COUNT;
        return Err(format!(
            "expect {range:?}, got zero points (len: 0 vs {POINT_COUNT})"
        ));
    }

    debug_assert!(
        ids.windows(2).all(|w| w[0] < w[1]),
        "point IDs contain duplicate or are not sorted",
    );

    let (min, max) = (*ids.first().unwrap(), *ids.last().unwrap());
    let wrong_lowest = min != sweep_start;
    let wrong_highest = max != sweep_start + POINT_COUNT - 1;
    let wrong_count = ids.len() != POINT_COUNT as usize;
    if wrong_lowest || wrong_highest || wrong_count {
        let range = sweep_start..sweep_start + POINT_COUNT;
        return Err(format!(
            "expect {range:?}, got {} (len: {} vs {POINT_COUNT})",
            format_ranges(&ids),
            ids.len(),
        ));
    }

    Ok(())
}

async fn run_transfers(clients: Arc<Vec<Qdrant>>, transfer_lock: Arc<Mutex<()>>) {
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

        // Block transfers if we're currently waiting on inconsistency
        drop(transfer_lock.lock().await);

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

fn format_ranges(ids: &[u64]) -> String {
    let mut ranges = vec![];
    let mut range = None;

    for id in ids {
        match range {
            // New range
            None => {
                range.replace(*id..*id + 1);
                continue;
            }
            // Extend current range
            Some(ref r) if r.end == *id => {
                range.replace(r.start..*id + 1);
                continue;
            }
            // Close current range
            Some(_) => {
                ranges.push(format!("{:?}", range.replace(*id..*id + 1).unwrap()));
            }
        }
    }

    if let Some(range) = range {
        ranges.push(format!("{range:?}"));
    }

    ranges.join(",")
}

fn point_num(id: &PointId) -> u64 {
    match id.point_id_options.as_ref().unwrap() {
        PointIdOptions::Num(num) => *num,
        PointIdOptions::Uuid(_) => unreachable!(),
    }
}
