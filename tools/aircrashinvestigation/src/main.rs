use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use qdrant_client::{
    qdrant::{
        quantization_config::Quantization, BinaryQuantizationBuilder, CreateCollectionBuilder,
        DeleteCollectionBuilder, QuantizationType, VectorParams, VectorParamsBuilder,
        VectorsConfigBuilder,
    },
    Qdrant,
};
use serde::Deserialize;
use serde_json::Value;

const HOST: &str = "http://localhost:6334";
const API_KEY: Option<&str> = None;

const DRY_RUN: bool = true;
const DELETE_COLLECTIONS: bool = true;
const CREATE_COLLECTIONS: bool = false;

const TIMEOUT_SECS: u64 = 60;

const BAD_PEER: u64 = 123456789;

const FILES: &'static [&'static [u8]] = &[
    include_bytes!("../res/telemetry-0.json"),
    // include_bytes!("../res/telemetry-1.json"),
    // include_bytes!("../res/telemetry-2.json"),
];

#[tokio::main]
async fn main() {
    let telemetries: Vec<Response> = FILES
        .into_iter()
        .map(|data| serde_json::from_slice(data).unwrap())
        .collect();
    let mut collections: Vec<Vec<Collection>> = telemetries
        .iter()
        .map(|telemetry| telemetry.result.collections.collections.clone())
        .collect();

    // Enrich payload schema
    for collections in &mut collections {
        for collection in collections {
            let schema = collection
                .shards
                .iter()
                .flat_map(|shard| &shard.local)
                .flat_map(|shard| &shard.segments)
                .map(|segment| &segment.info.index_schema)
                .find(|schema| !schema.is_empty());
            if let Some(schema) = schema {
                collection.payload_schema = schema.clone();
            }
        }
    }

    let bad_collections: HashSet<String> = collections[0]
        .iter()
        // Keep collections that have the bad peer
        .filter(|collection| {
            collection
                .shards
                .iter()
                .any(|shard| shard.replicas.keys().any(|peer_id| *peer_id == BAD_PEER))
        })
        .map(|collection| collection.name.clone())
        .collect();

    // Ensure we have zero points and no payload indices on bad collections
    for collections in &collections {
        for collection in collections {
            if !bad_collections.contains(&collection.name) {
                continue;
            }

            for shard in &collection.shards {
                if let Some(local) = &shard.local {
                    assert_eq!(local.num_points, 0, "bad collections MUST have zero points");
                }
            }

            assert!(
                collection.payload_schema.is_empty(),
                "bad collections MUST have zero payload indices",
            );
        }
    }

    let client = Qdrant::from_url(HOST)
        .api_key(API_KEY)
        .timeout(Duration::from_secs(TIMEOUT_SECS))
        .build()
        .expect("failed to connect to Qdrant host");

    if DELETE_COLLECTIONS {
        for name in &bad_collections {
            delete_collection(&client, name).await;
        }
    }

    if CREATE_COLLECTIONS {
        for name in &bad_collections {
            let collection = collections[0]
                .iter()
                .find(|c| &c.name == name)
                .expect("failed to find collection by name");
            create_collection(&client, collection).await;
        }
    }
}

async fn create_collection(client: &Qdrant, collection: &Collection) {
    let name = &collection.name;

    println!("Creating collection: {name}");

    let CollectionConfig {
        uuid: _,
        params,
        hnsw_config,
        optimizer_config,
        _wal_config,
        quantization_config,
        strict_mode_config,
    } = &collection.config;

    let ParamsConfig {
        vectors,
        sparse_vectors,
        shard_number,
        replication_factor,
        write_consistency_factor,
        on_disk_payload,
    } = params;

    let mut create_collection = CreateCollectionBuilder::new(name)
        .vectors_config(vectors.into_api())
        .shard_number(*shard_number)
        .replication_factor(*replication_factor)
        .write_consistency_factor(*write_consistency_factor)
        .on_disk_payload(*on_disk_payload)
        .hnsw_config(hnsw_config.into_api())
        .optimizers_config(optimizer_config.into_api())
        .strict_mode_config(strict_mode_config.into_api());

    if !sparse_vectors.0.is_empty() {
        create_collection = create_collection.sparse_vectors_config(sparse_vectors.into_api());
    }

    if let Some(quantization) = quantization_config {
        create_collection = create_collection.quantization_config(quantization.into_api());
    }

    let create_collection = create_collection.timeout(TIMEOUT_SECS).build();

    if DRY_RUN {
        println!("DRY RUN: create collection: {name}");
        dbg!(&create_collection);
        for (payload_key, _config) in &collection.payload_schema {
            println!("DRY RUN: - create payload index {payload_key}");
        }
        return;
    }

    client
        .create_collection(create_collection)
        .await
        .expect("failed to create collection");

    println!("Created collection: {name}");
}

async fn delete_collection(client: &Qdrant, name: &str) {
    if DRY_RUN {
        println!("DRY RUN: delete collection: {name}");
        return;
    }

    println!("Deleting collection: {name}");
    client
        .delete_collection(DeleteCollectionBuilder::new(name).timeout(TIMEOUT_SECS))
        .await
        .expect("failed to delete collection");
    println!("Deleted collection: {name}");
}

#[derive(Debug, Deserialize, Clone)]
struct Response {
    result: Telemetry,
}

#[derive(Debug, Deserialize, Clone)]
struct Telemetry {
    #[allow(unused)]
    id: String,
    collections: CollectionSet,
}

#[derive(Debug, Deserialize, Clone)]
struct CollectionSet {
    #[allow(unused)]
    number_of_collections: u64,
    collections: Vec<Collection>,
}

#[derive(Debug, Deserialize, Clone)]
struct Collection {
    #[serde(alias = "id")]
    name: String,
    config: CollectionConfig,
    shards: Vec<Shard>,
    #[serde(skip)]
    payload_schema: HashMap<String, IndexSchema>,
}

#[derive(Debug, Deserialize, Clone)]
struct Shard {
    #[allow(unused)]
    id: u64,
    #[allow(unused)]
    key: Value,
    local: Option<LocalShard>,
    #[serde(rename = "replicate_states")]
    replicas: HashMap<u64, String>,
    #[allow(unused)]
    #[serde(default)]
    resharding: Option<Value>,
}

#[derive(Debug, Deserialize, Clone)]
struct LocalShard {
    #[allow(unused)]
    variant_name: String,
    #[allow(unused)]
    status: String,
    num_points: u64,
    #[allow(unused)]
    num_vectors: u64,
    segments: Vec<Segment>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct CollectionConfig {
    #[allow(unused)]
    uuid: Value,
    params: ParamsConfig,
    hnsw_config: HnswConfig,
    optimizer_config: OptimizerConfig,
    #[serde(rename = "wal_config")]
    _wal_config: Value,
    quantization_config: Option<QuantizationConfig>,
    #[serde(default)]
    strict_mode_config: StrictModeConfig,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct ParamsConfig {
    vectors: SingleOrMultipleVectors,
    #[serde(default)]
    sparse_vectors: SparseVectorsConfig,
    shard_number: u32,
    replication_factor: u32,
    write_consistency_factor: u32,
    on_disk_payload: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
enum SingleOrMultipleVectors {
    Named(HashMap<String, VectorConfig>),
    Default(VectorConfig),
}

impl SingleOrMultipleVectors {
    fn into_api(&self) -> VectorsConfigBuilder {
        let mut api_config = qdrant_client::qdrant::VectorsConfigBuilder::default();

        match self {
            Self::Named(vectors) => {
                for (name, vector) in vectors {
                    api_config.add_named_vector_params(name, vector.into_api());
                }
            }
            Self::Default(vector) => {
                api_config.add_vector_params(vector.into_api());
            }
        }

        api_config
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct VectorConfig {
    size: u64,
    distance: Distance,
    on_disk: Option<bool>,
}

impl VectorConfig {
    fn into_api(&self) -> VectorParams {
        let Self {
            size,
            distance,
            on_disk,
        } = self.clone();

        let mut params = VectorParamsBuilder::new(size, distance.into_api());

        if let Some(on_disk) = on_disk {
            params = params.on_disk(on_disk);
        }

        params.build()
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
enum Distance {
    Cosine,
}

impl Distance {
    fn into_api(&self) -> qdrant_client::qdrant::Distance {
        match self {
            Self::Cosine => qdrant_client::qdrant::Distance::Cosine,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
struct SparseVectorsConfig(#[serde(default)] HashMap<String, SparseVectorConfig>);

impl SparseVectorsConfig {
    fn into_api(&self) -> qdrant_client::qdrant::SparseVectorConfig {
        let mut api_config = HashMap::new();

        for (name, vector) in &self.0 {
            api_config.insert(name.clone(), vector.into_api());
        }

        qdrant_client::qdrant::SparseVectorConfig { map: api_config }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct SparseVectorConfig {
    index: Option<SparseIndex>,
    modifier: Option<Modifier>,
}

impl SparseVectorConfig {
    fn into_api(&self) -> qdrant_client::qdrant::SparseVectorParams {
        let Self { index, modifier } = self.clone();

        let index = index.map(|index| index.into_api());

        let modifier = modifier.map(|modifier| modifier.into_api());

        qdrant_client::qdrant::SparseVectorParams { index, modifier }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct SparseIndex {
    on_disk: bool,
    datatype: SparseIndexType,
}

impl SparseIndex {
    fn into_api(&self) -> qdrant_client::qdrant::SparseIndexConfig {
        let Self { on_disk, datatype } = self.clone();

        qdrant_client::qdrant::SparseIndexConfig {
            on_disk: Some(on_disk),
            datatype: Some(datatype.into_api()),
            full_scan_threshold: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
enum SparseIndexType {
    Uint8,
}

impl SparseIndexType {
    fn into_api(&self) -> i32 {
        match self {
            Self::Uint8 => qdrant_client::qdrant::Datatype::Uint8 as i32,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
enum Modifier {
    Idf,
}

impl Modifier {
    fn into_api(&self) -> i32 {
        match self {
            Self::Idf => qdrant_client::qdrant::Modifier::Idf as i32,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct HnswConfig {
    m: u64,
    payload_m: Option<u64>,
    ef_construct: u64,
    full_scan_threshold: u64,
    max_indexing_threads: u64,
    on_disk: bool,
}

impl HnswConfig {
    fn into_api(&self) -> qdrant_client::qdrant::HnswConfigDiff {
        let Self {
            m,
            payload_m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            on_disk,
        } = self.clone();

        qdrant_client::qdrant::HnswConfigDiff {
            m: Some(m),
            payload_m,
            ef_construct: Some(ef_construct),
            full_scan_threshold: Some(full_scan_threshold),
            max_indexing_threads: Some(max_indexing_threads),
            on_disk: Some(on_disk),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct OptimizerConfig {
    deleted_threshold: f64,
    vacuum_min_vector_number: u64,
    default_segment_number: u64,
    max_segment_size: Option<u64>,
    memmap_threshold: Option<u64>,
    indexing_threshold: u64,
    flush_interval_sec: u64,
    max_optimization_threads: Option<u64>,
}

impl OptimizerConfig {
    fn into_api(&self) -> qdrant_client::qdrant::OptimizersConfigDiff {
        let Self {
            deleted_threshold,
            vacuum_min_vector_number,
            default_segment_number,
            max_segment_size,
            memmap_threshold,
            indexing_threshold,
            flush_interval_sec,
            max_optimization_threads,
        } = self.clone();

        let threads = match max_optimization_threads {
            Some(0) => {
                Some(qdrant_client::qdrant::MaxOptimizationThreadsBuilder::disabled().build())
            }
            Some(n) => {
                Some(qdrant_client::qdrant::MaxOptimizationThreadsBuilder::threads(n).build())
            }
            None => None,
        };

        qdrant_client::qdrant::OptimizersConfigDiff {
            deleted_threshold: Some(deleted_threshold),
            vacuum_min_vector_number: Some(vacuum_min_vector_number),
            default_segment_number: Some(default_segment_number),
            max_segment_size,
            memmap_threshold,
            indexing_threshold: Some(indexing_threshold),
            flush_interval_sec: Some(flush_interval_sec),
            max_optimization_threads: threads,
            deprecated_max_optimization_threads: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
enum QuantizationConfig {
    Binary {
        always_ram: bool,
    },
    Scalar {
        #[serde(rename = "type")]
        q_type: ScalarType,
        always_ram: bool,
    },
}

impl QuantizationConfig {
    fn into_api(&self) -> qdrant_client::qdrant::quantization_config::Quantization {
        match self {
            QuantizationConfig::Scalar { q_type, always_ram } => {
                Quantization::Scalar(qdrant_client::qdrant::ScalarQuantization {
                    r#type: q_type.into_api(),
                    always_ram: Some(*always_ram),
                    quantile: None,
                })
            }
            QuantizationConfig::Binary { always_ram } => {
                Quantization::Binary(BinaryQuantizationBuilder::new(*always_ram).build())
            }
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
enum ScalarType {
    Int8,
}

impl ScalarType {
    fn into_api(&self) -> i32 {
        match self {
            Self::Int8 => QuantizationType::Int8 as i32,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
struct StrictModeConfig {
    enabled: Option<bool>,
    unindexed_filtering_retrieve: Option<bool>,
    unindexed_filtering_update: Option<bool>,
}

impl StrictModeConfig {
    fn into_api(&self) -> qdrant_client::qdrant::StrictModeConfig {
        let Self {
            enabled,
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
        } = self.clone();

        qdrant_client::qdrant::StrictModeConfig {
            enabled,
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            ..Default::default()
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
struct Segment {
    info: SegmentInfo,
}

#[derive(Debug, Deserialize, Clone)]
struct SegmentInfo {
    index_schema: HashMap<String, IndexSchema>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(tag = "data_type", rename_all = "snake_case")]
#[allow(unused)]
enum IndexSchema {
    Integer {
        points: usize,
    },
    Geo {
        points: usize,
    },
    Keyword {
        points: usize,
    },
    Bool {
        points: usize,
    },
    Float {
        points: usize,
    },
    Datetime {
        points: usize,
    },
    Text {
        points: usize,
        params: Option<TextParams>,
    },
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[allow(unused)]
struct TextParams {
    #[serde(rename = "type")]
    text_type: TextType,
    tokenizer: String,
    min_token_len: usize,
    max_token_len: usize,
    lowercase: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
enum TextType {
    Text,
}
