use std::ops::Range;

use anyhow::Result;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{Condition, Filter, ScrollPointsBuilder};
#[allow(unused_imports)]
use qdrant_client::qdrant::{PointId, ScrollPoints, WithPayloadSelector};
use qdrant_client::Qdrant;

const COLLECTION_NAME: &str = "workload-crasher";
const RANGE: Range<u64> = 0..10000;

const HOSTS: &[&str] = &["http://127.0.0.1:6334"];

const API_KEY: Option<&str> = None;

const PAYLOAD_KEY: &str = "mandatory-payload-timestamp";

#[tokio::main]
async fn main() -> Result<()> {
    for host in HOSTS {
        println!("\n### CHECKING HOST {host} ###");
        check_host(host).await?;
    }

    Ok(())
}

async fn check_host(host: &str) -> Result<()> {
    let mut client = Qdrant::from_url(host);
    if let Some(api_key) = API_KEY {
        client = client.api_key(api_key);
    }
    let client = client.build()?;

    let ids = RANGE.collect::<Vec<_>>();

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

    let response = client
        .scroll(
            ScrollPointsBuilder::new(COLLECTION_NAME)
                .filter(Filter::must([Condition::is_empty(PAYLOAD_KEY)]))
                .offset(PointIdOptions::Num(*ids.first().unwrap()))
                .limit(u32::MAX)
                .with_payload(true)
                .with_vectors(true),
        )
        .await?;

    let missing_ids: Vec<_> = response
        .result
        .into_iter()
        .map(|point| point.id.and_then(|pid| pid.point_id_options))
        .collect();

    println!("Missing {} points: {missing_ids:?}", missing_ids.len());

    Ok(())
}
