use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use remem_server::{
    engine::{
        attr::{select::AttrPred, value::AttrValue},
        query::{HybridQuery, QueryEngine, QueryEngineConfig},
        storage::{
            engine::{EngineConfig, StorageEngine, VectorConfig},
            partition::PartitionScope,
        },
        util::DistanceMetric,
    },
    services::{
        attrs::{memory_schema, project, SLOT_ARCHIVED, SLOT_MEMORY_TYPE},
        types::{MemoryType, StoredMemory, StoredMetadata},
    },
};

const RECORDS: u32 = 200;
const PAGE_SIZE: usize = 10;

#[derive(Clone, Copy)]
enum Workload {
    Normal,
    PhantomHeavy,
    SelectiveFilter,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::PhantomHeavy => "phantom_heavy",
            Self::SelectiveFilter => "selective_filter",
        }
    }
}

struct Fixture {
    query_engine: QueryEngine,
    query: HybridQuery,
    scope: PartitionScope,
}

fn benchmark_data_dir() -> PathBuf {
    env::var_os("REMEM_BENCHMARK_DATA_DIR")
        .map(PathBuf::from)
        .expect("REMEM_BENCHMARK_DATA_DIR must name an empty persistent benchmark data directory")
}

fn memory(i: u32, long_term: bool) -> StoredMemory {
    StoredMemory {
        id: uuid::Uuid::from_u128(i as u128 + 1),
        content: format!("Remem vector retrieval benchmark record {i}"),
        memory_type: if long_term {
            MemoryType::LongTerm
        } else {
            MemoryType::ShortTerm
        },
        metadata: StoredMetadata {
            created_at: 1_700_000_000_000 + i as u64,
            updated_at: 1_700_000_000_000 + i as u64,
            accessed_at: 1_700_000_000_000 + i as u64,
            access_count: 0,
            source: None,
            tags: vec!["benchmark".to_owned()],
            importance: 0.5,
            emotional_valence: 0.0,
            arousal: 0.0,
            health: 100.0,
            last_recalled_at: None,
            flashbulb_until: None,
            ttl: None,
            last_decay_at: None,
            last_health_check_at: None,
        },
        archived: false,
    }
}

async fn build_fixture(dir: &Path, workload: Workload) -> Fixture {
    let engine = Arc::new(
        StorageEngine::new(EngineConfig {
            data_dir: dir.to_path_buf(),
            sync_writes: false,
            checkpoint_interval: Duration::from_secs(86_400),
            vector: VectorConfig {
                enabled: true,
                dimension: 4,
                hnsw_m: 8,
                hnsw_ef_construction: 64,
                hnsw_ef_search: 64,
                metric: DistanceMetric::L2,
                hnsw_resident_budget_bytes: None,
            },
            attr_schema: Some(memory_schema()),
            ..Default::default()
        })
        .await
        .expect("benchmark storage opens"),
    );

    for i in 0..RECORDS {
        let stored = memory(
            i,
            matches!(workload, Workload::SelectiveFilter) && i % 5 == 0,
        );
        let key = format!("memory:{}", stored.id);
        engine
            .store_memory_core(
                key.clone(),
                serde_json::to_vec(&stored).expect("benchmark memory serializes"),
                Some(vec![1.0, i as f32 * 0.001, 0.0, 0.0]),
                stored.metadata.created_at,
                &stored.metadata.tags,
                Some(&project(&stored)),
            )
            .await
            .expect("benchmark memory stores");

        if matches!(workload, Workload::PhantomHeavy) && i % 5 != 0 {
            engine.delete(key).await.expect("benchmark phantom deletes");
        }
    }

    let query = match workload {
        Workload::SelectiveFilter => HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], PAGE_SIZE)
            .with_limit(PAGE_SIZE)
            .with_preds(vec![
                AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false)),
                AttrPred::Eq(SLOT_MEMORY_TYPE, AttrValue::U8(1)),
            ]),
        Workload::Normal | Workload::PhantomHeavy => {
            HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], PAGE_SIZE).with_limit(PAGE_SIZE)
        }
    };

    Fixture {
        query_engine: QueryEngine::new(engine, QueryEngineConfig::default()),
        query,
        scope: PartitionScope::legacy_default(),
    }
}

async fn execute(fixture: &Fixture) {
    let result = fixture
        .query_engine
        .execute_partitioned(fixture.query.clone(), &fixture.scope)
        .await
        .expect("benchmark query executes");
    black_box(result);
}

fn vector_retrieval_benchmark(criterion: &mut Criterion) {
    let revision = env::var("REMEM_BENCHMARK_REVISION").unwrap_or_else(|_| "unknown".to_owned());
    eprintln!(
        "remem vector-retrieval benchmark revision={revision} records={RECORDS} page_size={PAGE_SIZE} ef_search=64 execution=in-process-query-engine"
    );

    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime");
    let root = benchmark_data_dir();
    let mut group = criterion.benchmark_group("vector_retrieval");
    group.throughput(Throughput::Elements(PAGE_SIZE as u64));

    for workload in [
        Workload::Normal,
        Workload::PhantomHeavy,
        Workload::SelectiveFilter,
    ] {
        let fixture = runtime.block_on(build_fixture(&root.join(workload.name()), workload));
        group.bench_function(workload.name(), |bencher| {
            bencher.to_async(&runtime).iter(|| execute(&fixture));
        });
    }

    group.finish();
}

criterion_group!(benches, vector_retrieval_benchmark);
criterion_main!(benches);
