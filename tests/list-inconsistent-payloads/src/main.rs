use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::GetPointsBuilder;
use qdrant_client::qdrant::PointId;
use qdrant_client::Qdrant;

const COLLECTION_NAME: &str = "benchmark";
const BATCH_SIZE: usize = 10000;
const POINT_COUNT: u64 = 200_000;
const RETRY_TIMEOUT: Duration = Duration::from_secs(60);
const PAYLOAD_KEY: &str = "timestamp";

const GET_RETRIES: usize = 30;
const GET_RETRY_INTERVAL: Duration = Duration::from_secs(1);

const HOSTS: &[&str] = &[
    "http://127.0.0.1:6334",
    "http://127.0.0.2:6334",
    "http://127.0.0.3:6334",
];
const API_KEY: Option<&str> = None;

#[tokio::main]
async fn main() {
    let clients = build_clients();

    loop {
        check_points(&clients).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
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

async fn get_payloads(client: &Qdrant, ids: Vec<u64>) -> HashMap<u64, String> {
    let mut map = HashMap::with_capacity(ids.len());

    for batch_ids in ids.chunks(BATCH_SIZE) {
        for retries_left in (0..GET_RETRIES).rev() {
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
                .await;

            let response = match response {
                Ok(response) => response,
                Err(err) if retries_left == 0 => {
                    panic!("GET failed after {GET_RETRIES} retries: {err:?}");
                }
                Err(err) => {
                    println!("GET failed: {err:?}");
                    tokio::time::sleep(GET_RETRY_INTERVAL).await;
                    continue;
                }
            };

            map.extend(response.result.into_iter().map(|point| {
                (
                    point_num(&point.id.unwrap()),
                    point.payload[PAYLOAD_KEY].as_str().unwrap().to_string(),
                )
            }));
            break;
        }
    }

    map
}

async fn check_points(clients: &[Qdrant]) -> Result<(), String> {
    println!("Checking payloads on {} hosts", clients.len());

    let mut remaining = (0..POINT_COUNT).collect::<Vec<_>>();

    let start = Instant::now();
    let mut ids = remaining.split_off(remaining.len().saturating_sub(BATCH_SIZE));

    let mut retry = 0;
    while !remaining.is_empty() || !ids.is_empty() {
        if start.elapsed() > RETRY_TIMEOUT {
            panic!(
                "Got {} inconsistent payloads after {:?}",
                ids.len(),
                RETRY_TIMEOUT
            );
        }

        let left = remaining.len() + ids.len();
        let percent = (POINT_COUNT as usize - left) as f64 / POINT_COUNT as f64 * 100.0;

        if retry > 0 {
            println!(
                "Checking {}/{left} ({percent:.3}%) payloads (retry {retry})",
                ids.len(),
            );
        } else {
            println!("Checking {}/{left} ({percent:.3}%) payloads", ids.len(),);
        }

        let payloads = join_all(clients.iter().map(|client| {
            let ids = ids.clone();
            get_payloads(client, ids)
        }))
        .await;

        let mut new_ids = Vec::with_capacity(BATCH_SIZE);
        let mut is_retry = false;
        for id in &ids {
            let consistent = payloads
                .windows(2)
                .all(|payload| payload[0][id] == payload[1][id]);

            if !consistent {
                println!("- mismatch: {id}");
                new_ids.push(*id);
                is_retry = true;
            }
        }

        if is_retry {
            retry += 1;
        }

        ids = new_ids;
        ids.extend(remaining.split_off(remaining.len().saturating_sub(BATCH_SIZE - ids.len())));
    }

    println!("ALL CONSISTENT AFTER {retry} RETRIES!\n\n");

    Ok(())
}

fn point_num(id: &PointId) -> u64 {
    match id.point_id_options.as_ref().unwrap() {
        PointIdOptions::Num(num) => *num,
        PointIdOptions::Uuid(_) => unreachable!(),
    }
}
