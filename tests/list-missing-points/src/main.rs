use std::ops::Range;

use anyhow::Result;
use qdrant_client::qdrant::{point_id::PointIdOptions, RetrievedPoint};
#[allow(unused_imports)]
use qdrant_client::qdrant::{PointId, ScrollPoints, WithPayloadSelector};
use qdrant_client::{prelude::*, qdrant::WithVectorsSelector};
use took::Timer;

const COLLECTION_NAME: &str = "benchmark";
const BATCH_SIZE: usize = 5000;
const RANGE: Range<u64> = 0..200000;

const HOSTS: &[&str] = &[
    "http://127.0.0.1:6334",
    "http://127.0.0.2:6334",
    "http://127.0.0.3:6334",
];

const API_KEY: Option<&str> = None;

#[tokio::main]
async fn main() -> Result<()> {
    let mut host_points = Vec::new();

    for host in HOSTS {
        println!("\n### CHECKING HOST {host} ###");
        let points = check_host(host).await?;
        host_points.push(points);
    }

    for (i, points) in host_points.windows(2).enumerate() {
        println!("\n### CHECKING POINTS FOR NODES {i}, {} ###", i + 1);
        for (a, b) in points[0].iter().zip(points[1].iter()) {
            if a != b {
                // println!(
                //     ">>> Point {:?} on node {i} and {} differs\n{:#?}\n{:#?}",
                //     a.id.as_ref().unwrap(),
                //     i + 1,
                //     a,
                //     b,
                // );
                println!(
                    ">>> Point {:?} on node {i} and {} differs",
                    a.id.as_ref().unwrap(),
                    i + 1,
                );
            }
        }
    }

    Ok(())
}

async fn check_host(host: &str) -> Result<Vec<RetrievedPoint>> {
    let mut client = QdrantClient::from_url(host);
    if let Some(api_key) = API_KEY {
        client = client.with_api_key(api_key);
    }
    let client = client.build()?;

    let ids = RANGE.collect::<Vec<_>>();
    let mut missing_ids = Vec::new();
    let mut points = Vec::new();

    for ids in ids.chunks(BATCH_SIZE) {
        // let points = ids
        //     .iter()
        //     .map(|id| PointId {
        //         point_id_options: Some(PointIdOptions::Num(*id)),
        //     })
        //     .collect::<Vec<_>>();
        // let response = client
        //     .get_points(
        //         COLLECTION_NAME,
        //         None,
        //         &points,
        //         Some(false),
        //         Some(false),
        //         None,
        //     )
        //     .await?;

        let scroll = ScrollPoints {
            collection_name: COLLECTION_NAME.into(),
            filter: None,
            offset: Some(PointId {
                point_id_options: Some(PointIdOptions::Num(*ids.first().unwrap())),
            }),
            limit: Some(ids.len() as u32),
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

        let mut tmp_missing = Vec::new();

        let timer = Timer::new();

        ids.iter()
            .filter(|&id| {
                !response.result.iter().any(|point| {
                    point.id.as_ref().unwrap()
                        == &PointId {
                            point_id_options: Some(PointIdOptions::Num(*id)),
                        }
                })
            })
            .for_each(|id| {
                println!("missing: {id}");
                missing_ids.push(id);
                tmp_missing.push(id);
            });

        while !tmp_missing.is_empty() {
            let ids = tmp_missing.clone();
            tmp_missing.clear();

            let points = ids
                .iter()
                .map(|id| PointId {
                    point_id_options: Some(PointIdOptions::Num(**id)),
                })
                .collect::<Vec<_>>();

            print!(" retrying for {}", points.len());

            let response = client
                .get_points(
                    COLLECTION_NAME,
                    None,
                    &points,
                    Some(false),
                    Some(false),
                    None,
                )
                .await?;

            timer.took().describe(", retry");

            ids.iter()
                .filter(|&id| {
                    !response.result.iter().any(|point| {
                        point.id.as_ref().unwrap()
                            == &PointId {
                                point_id_options: Some(PointIdOptions::Num(**id)),
                            }
                    })
                })
                .for_each(|id| {
                    println!("RETRY MISSING: {id}");
                    tmp_missing.push(id);
                });

            println!(" retry done");
        }

        points.extend(response.result);
    }

    println!("Missing {} points: {missing_ids:?}", missing_ids.len());

    Ok(points)
}
