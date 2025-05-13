use std::ops::Range;
use std::time::Duration;

use anyhow::Result;
use futures::future::try_join_all;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use qdrant_client::prelude::*;
use qdrant_client::qdrant::WithVectorsSelector;
use qdrant_client::qdrant::{point_id::PointIdOptions, RetrievedPoint};
#[allow(unused_imports)]
use qdrant_client::qdrant::{PointId, ScrollPoints, WithPayloadSelector};

const COLLECTION_NAME: &str = "benchmark";
// const BATCH_SIZE: usize = 500;
// const RANGE: Range<u64> = 0..200000;
const SCROLL: bool = true;

// const BATCH_SIZE: usize = 1;
// const RANGE: Range<u64> = 25850..25850 + 1;

// const BATCH_SIZE: usize = 1;
// const RANGE: Range<u64> = 167502..167502 + 1;

const BATCH_SIZE: usize = 500;
const RANGE: Range<u64> = 0..10000;

const HOSTS: [&str; 3] = [
    "http://127.0.0.1:6334",
    "http://127.0.0.2:6334",
    "http://127.0.0.3:6334",
];
const API_KEY: Option<&str> = None;

#[tokio::main]
async fn main() -> Result<()> {
    println!("### FETCHING POINTS ###");

    let multi = MultiProgress::new();
    let pb_style =
        ProgressStyle::with_template("{prefix} {wide_bar} {pos}/{len} points ({eta})").unwrap();

    let fetchers = HOSTS
        .iter()
        .enumerate()
        .map(|(i, host)| {
            let pb = ProgressBar::new(RANGE.count() as u64);
            multi.add(pb.clone());
            pb.set_style(pb_style.clone());
            pb.set_prefix(format!("Node {i}"));
            pb.set_position(0);

            fetch_host_points(i, host, pb)
        })
        .collect::<Vec<_>>();

    let mut points = try_join_all(fetchers).await?;

    // Sort by ID
    for points in points.iter_mut() {
        points.sort_unstable_by_key(|point| point_num(point.id.as_ref().unwrap()));
    }

    check_point_consistency(points);

    Ok(())
}

async fn fetch_host_points(
    node: usize,
    host: &str,
    pb: ProgressBar,
) -> Result<Vec<RetrievedPoint>> {
    let mut client = QdrantClient::from_url(host)
        .with_connect_timeout(Duration::from_secs(10))
        .with_timeout(Duration::from_secs(20));
    if let Some(api_key) = API_KEY {
        client = client.with_api_key(api_key);
    }
    let client = client.build()?;

    let ids = RANGE.collect::<Vec<_>>();
    let mut missing_ids = Vec::new();
    let mut points = Vec::new();

    for batch_ids in ids.chunks(BATCH_SIZE) {
        let response_points = if !SCROLL {
            let batch_ids = batch_ids
                .iter()
                .map(|id| PointId {
                    point_id_options: Some(PointIdOptions::Num(*id)),
                })
                .collect::<Vec<_>>();
            let response = client
                .get_points(
                    COLLECTION_NAME,
                    None,
                    &batch_ids,
                    Some(true),
                    Some(true),
                    None,
                )
                .await?;
            response.result
        } else {
            let scroll = ScrollPoints {
                collection_name: COLLECTION_NAME.into(),
                filter: None,
                offset: Some(PointId {
                    point_id_options: Some(PointIdOptions::Num(*batch_ids.first().unwrap())),
                }),
                limit: Some(batch_ids.len() as u32),
                with_payload: Some(WithPayloadSelector {
                    selector_options: Some(
                        qdrant_client::qdrant::with_payload_selector::SelectorOptions::Enable(true),
                    ),
                }),
                with_vectors: Some(WithVectorsSelector {
                    selector_options: Some(
                        qdrant_client::qdrant::with_vectors_selector::SelectorOptions::Enable(true),
                    ),
                }),
                read_consistency: None,
                shard_key_selector: None,
                order_by: None,
            };
            let response = client.scroll(&scroll).await?;
            response.result
        };

        batch_ids
            .iter()
            .filter(|&id| {
                !response_points.iter().any(|point| {
                    point.id.as_ref().unwrap()
                        == &PointId {
                            point_id_options: Some(PointIdOptions::Num(*id)),
                        }
                })
            })
            .for_each(|id| {
                println!("missing: {id}");
                missing_ids.push(id);
            });

        assert_eq!(response_points.len(), batch_ids.len());

        points.extend(response_points);
        pb.inc(batch_ids.len() as u64);
    }

    pb.println(format!(
        "Node {node} - missing {} points: {missing_ids:?}",
        missing_ids.len(),
    ));

    pb.finish_and_clear();

    Ok(points)
}

fn check_point_consistency(points: Vec<Vec<RetrievedPoint>>) {
    let mut wrong_vector_counts = vec![0; HOSTS.len() - 1];
    let mut wrong_payload_counts = vec![0; HOSTS.len() - 1];

    println!("\n### CHECK POINTS CONSISTENCY ###");
    for (i, (points, (wrong_vector_count, wrong_payload_count))) in points
        .windows(2)
        .zip(
            wrong_vector_counts
                .iter_mut()
                .zip(wrong_payload_counts.iter_mut()),
        )
        .enumerate()
    {
        for (a, b) in points[0].iter().zip(points[1].iter()) {
            // Point IDs we're comparing must be equal
            if a.id != b.id {
                panic!(
                    "point ids are not equal: {:?}, {:?}",
                    point_num(a.id.as_ref().unwrap()),
                    point_num(b.id.as_ref().unwrap()),
                );
            }

            // Check vector consistency
            let inconsistent_vectors = a.vectors != b.vectors;
            let inconsistent_payload = a.payload != b.payload;

            if inconsistent_vectors || inconsistent_payload {
                print!(
                    "Node {i} vs {} - point {} inconsistency:",
                    i + 1,
                    point_num(a.id.as_ref().unwrap()),
                );
                if inconsistent_vectors {
                    print!(" vector,");
                }
                if inconsistent_payload {
                    print!(" payload,");
                }
                println!();
            }

            if inconsistent_vectors {
                *wrong_vector_count += 1;
            }
            if inconsistent_payload {
                *wrong_payload_count += 1;
            }

            if !inconsistent_vectors && inconsistent_payload {
                println!("  payload {i}: {:?}", a.payload);
                println!("  payload {}: {:?}", i + 1, b.payload);
            }
        }
    }

    // Report inconsistent counts
    println!("\n### CONSISTENCY COUNTS ###");
    for (i, (vector_counts, payload_counts)) in wrong_vector_counts
        .iter()
        .zip(wrong_payload_counts)
        .enumerate()
    {
        println!(
            "Node {i} vs {} - inconsistent vectors: {vector_counts} / {}",
            i + 1,
            RANGE.count(),
        );
        println!(
            "Node {i} vs {} - inconsistent payloads: {payload_counts} / {}",
            i + 1,
            RANGE.count(),
        );
    }
}

fn point_num(id: &PointId) -> u64 {
    match id.point_id_options.as_ref().unwrap() {
        PointIdOptions::Num(num) => *num,
        PointIdOptions::Uuid(_) => todo!(),
    }
}
