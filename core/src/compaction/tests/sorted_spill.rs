/*
 * Copyright 2025 iceberg-compaction
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Sorted compaction of an upsert table whose decoded input is several times
//! larger than `max_memory_bytes`.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::disk_manager::DiskManager;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{NullOrder, SortDirection, SortField, SortOrder, Transform};
use iceberg::writer::IcebergWriter;
use iceberg::writer::delta_writer::{DELETE_OP, INSERT_OP};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{
    append_and_commit, build_equality_delta_writer, build_simple_data_writer, create_namespace,
    create_test_env, simple_table_schema, simple_table_schema_with_pos,
};
use crate::compaction::CompactionBuilder;
use crate::config::{
    CompactionConfigBuilder, CompactionExecutionConfigBuilder, CompactionPlanningConfig,
    FullCompactionConfigBuilder, SpillCompression,
};
use crate::executor::DataFusionExecutor;
use crate::executor::datafusion::datafusion_processor::build_spilling_runtime_env;

const BASE_ROWS: i32 = 800_000;
const BASE_FILES: i32 = 4;
const WRITE_BATCH_ROWS: i32 = 50_000;
/// Rows inserted and deleted again by the upsert writer, so they become
/// position deletes.
const TRANSIENT_ROWS: i32 = 2_000;
/// Prefix of `TRANSIENT_ROWS` re-inserted after their position delete.
const REINSERTED_ROWS: i32 = 1_000;

#[tokio::test]
async fn test_sorted_compaction_with_deletes_spills_within_memory_budget() {
    assert_sorted_compaction_spills_within_budget(1, 32 * 1024 * 1024).await;
}

/// Four scan and four sort partitions add `RepartitionExec`, partitioned
/// joins, and concurrent sort merges that compete for the pool. The budget
/// covers the four 10 MiB merge reservations of `DataFusion`'s default
/// `sort_spill_reservation_bytes`.
#[tokio::test]
async fn test_parallel_sorted_compaction_with_deletes_spills_within_memory_budget() {
    assert_sorted_compaction_spills_within_budget(4, 64 * 1024 * 1024).await;
}

/// Upserts and deletes rows of a table sorted by `id`, then compacts it with
/// `parallelism` scan and sort partitions under a `memory_budget_bytes` pool
/// while the decoded input is several times larger.
///
/// The rewrite runs the merge-on-read anti joins for equality and position
/// deletes before the `SortExec`. It must succeed, write files sorted by `id`
/// with exactly the live rows, keep every pool reservation within the budget,
/// and spill to disk.
async fn assert_sorted_compaction_spills_within_budget(
    parallelism: usize,
    memory_budget_bytes: usize,
) {
    let env = create_test_env().await;
    let namespace_ident = NamespaceIdent::new("sorted_spill".into());
    create_namespace(env.catalog.as_ref(), &namespace_ident).await;
    let table_ident = TableIdent::new(namespace_ident, "sorted_upsert".into());
    let sort_order = SortOrder::builder()
        .with_sort_field(SortField {
            source_id: 1,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        })
        .build(&simple_table_schema())
        .unwrap();
    env.catalog
        .create_table(
            &table_ident.namespace,
            TableCreation::builder()
                .name(table_ident.name().into())
                .sort_order(sort_order)
                .schema(simple_table_schema())
                .build(),
        )
        .await
        .unwrap();
    let table = env.catalog.load_table(&table_ident).await.unwrap();

    // Base snapshot: ids 0..BASE_ROWS in a scrambled order, so the sort has real work.
    let mut expected = BTreeMap::new();
    let mut decoded_input_bytes = 0;
    let mut base_files = Vec::new();
    let rows_per_file = BASE_ROWS / BASE_FILES;
    for file_index in 0..BASE_FILES {
        let mut writer = build_simple_data_writer(
            &table,
            env.warehouse_location.clone(),
            &format!("base_{file_index}"),
        )
        .await;
        let file_rows = file_index * rows_per_file..(file_index + 1) * rows_per_file;
        for batch_start in file_rows.step_by(WRITE_BATCH_ROWS as usize) {
            let rows = (batch_start..batch_start + WRITE_BATCH_ROWS)
                .map(|i| {
                    // 7919 is prime and does not divide BASE_ROWS, so this is a permutation.
                    let id = ((i64::from(i) * 7919) % i64::from(BASE_ROWS)) as i32;
                    (id, row_name(id, "base"))
                })
                .collect::<Vec<_>>();
            expected.extend(rows.iter().cloned());
            let batch = data_batch(&rows);
            decoded_input_bytes += batch.get_array_memory_size();
            writer.write(batch).await.unwrap();
        }
        base_files.extend(writer.close().await.unwrap());
    }
    let table = append_and_commit(&table, env.catalog.as_ref(), base_files).await;
    assert!(
        decoded_input_bytes > 4 * memory_budget_bytes,
        "the decoded input ({decoded_input_bytes} B) must dwarf the budget"
    );

    // Upsert snapshot: equality deletes for rewritten and deleted base rows, and
    // position deletes for rows this writer inserted and deleted again.
    let mut delta_writer =
        build_equality_delta_writer(&table, env.warehouse_location.clone(), vec![1]).await;
    let updated_ids = (0..BASE_ROWS).filter(|id| id % 10 == 0).collect::<Vec<_>>();
    let deleted_ids = (0..BASE_ROWS)
        .filter(|id| id % 7 == 0 && id % 10 != 0)
        .collect::<Vec<_>>();
    let transient_ids = (BASE_ROWS..BASE_ROWS + TRANSIENT_ROWS).collect::<Vec<_>>();
    let reinserted_ids = (BASE_ROWS..BASE_ROWS + REINSERTED_ROWS).collect::<Vec<_>>();
    let steps: [(&[i32], &str, i32); 5] = [
        (&updated_ids, "base", DELETE_OP),
        (&deleted_ids, "base", DELETE_OP),
        (&updated_ids, "updated", INSERT_OP),
        (&transient_ids, "transient", INSERT_OP),
        (&transient_ids, "transient", DELETE_OP),
    ];
    for (ids, version, op) in steps {
        for chunk in ids.chunks(WRITE_BATCH_ROWS as usize) {
            delta_writer
                .write(delta_batch(chunk, version, op))
                .await
                .unwrap();
        }
    }
    delta_writer
        .write(delta_batch(&reinserted_ids, "reinserted", INSERT_OP))
        .await
        .unwrap();
    let delta_files = delta_writer.close().await.unwrap();
    append_and_commit(&table, env.catalog.as_ref(), delta_files).await;
    for id in &deleted_ids {
        expected.remove(id);
    }
    expected.extend(updated_ids.iter().map(|&id| (id, row_name(id, "updated"))));
    expected.extend(
        reinserted_ids
            .iter()
            .map(|&id| (id, row_name(id, "reinserted"))),
    );

    // Run the production runtime (`build_spilling_runtime_env`) behind an
    // observing pool, so the test sees every reservation and the spill files.
    let runtime_env =
        Arc::into_inner(build_spilling_runtime_env(memory_budget_bytes, None, None).unwrap())
            .expect("a new runtime is not shared");
    let observed_pool = Arc::new(ObservedPool::new(
        runtime_env.memory_pool.clone(),
        runtime_env.disk_manager.clone(),
    ));
    let runtime_env = Arc::new(RuntimeEnv {
        memory_pool: observed_pool.clone(),
        ..runtime_env
    });

    let execution_config = CompactionExecutionConfigBuilder::default()
        .max_memory_bytes(Some(memory_budget_bytes))
        .spill_compression(SpillCompression::Zstd)
        .build()
        .unwrap();
    // One file per input partition and 1 MiB target files make the planner
    // use exactly `parallelism` input and output partitions.
    let planning_config = FullCompactionConfigBuilder::default()
        .target_file_size_bytes(1024 * 1024_u64)
        .min_size_per_partition(1_u64)
        .max_file_count_per_partition(1_usize)
        .max_input_parallelism(parallelism)
        .max_output_parallelism(parallelism)
        .enable_heuristic_output_parallelism(false)
        .build()
        .unwrap();
    let mut compaction = CompactionBuilder::new(env.catalog.clone(), table_ident.clone())
        .with_config(Arc::new(
            CompactionConfigBuilder::default()
                .planning(CompactionPlanningConfig::Full(planning_config))
                .execution(execution_config)
                .build()
                .unwrap(),
        ))
        .build();
    let plans = compaction.plan_compaction().await.unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].recommended_executor_parallelism(), parallelism);
    assert_eq!(plans[0].recommended_output_parallelism(), parallelism);
    compaction.executor = Box::new(DataFusionExecutor::with_shared_runtime(runtime_env));

    let result = compaction.compact().await.unwrap().unwrap();

    let mut actual = Vec::with_capacity(expected.len());
    for data_file in &result.data_files {
        assert_eq!(data_file.sort_order_id(), Some(1));
        let content = table
            .file_io()
            .new_input(data_file.file_path())
            .unwrap()
            .read()
            .await
            .unwrap();
        let file_start = actual.len();
        for batch in ParquetRecordBatchReaderBuilder::try_new(content)
            .unwrap()
            .build()
            .unwrap()
        {
            let batch = batch.unwrap();
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            actual.extend(
                ids.values()
                    .iter()
                    .zip(names.iter())
                    .map(|(id, name)| (*id, name.unwrap().to_owned())),
            );
        }
        let file_ids = actual[file_start..].iter().map(|(id, _)| *id);
        assert!(
            file_ids.clone().zip(file_ids.skip(1)).all(|(a, b)| a < b),
            "{} is not sorted by id",
            data_file.file_path()
        );
    }
    actual.sort_unstable();
    assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());

    let consumers = observed_pool.consumer_peaks();
    let peak_reserved = observed_pool.peak_reserved.load(Ordering::Relaxed);
    let peak_spilled = observed_pool.peak_spilled_bytes.load(Ordering::Relaxed);
    let stats = format!(
        "parallelism {parallelism}, decoded input {decoded_input_bytes} B, budget \
         {memory_budget_bytes} B, peak reserved {peak_reserved} B, peak spill {peak_spilled} B, \
         consumers (name, can_spill, peak B): {consumers:?}"
    );
    assert!(peak_reserved <= memory_budget_bytes, "{stats}");
    assert!(
        peak_spilled > 0,
        "the sort must spill when its input exceeds the budget: {stats}"
    );
    // The anti joins against the delete files hold their build side in the same
    // pool, as unspillable reservations next to the sorter.
    assert!(
        consumers
            .iter()
            .any(|(name, can_spill, _)| name.starts_with("HashJoinInput") && !can_spill),
        "{stats}"
    );
    assert!(
        consumers
            .iter()
            .any(|(name, can_spill, _)| name.starts_with("ExternalSorter[") && *can_spill),
        "{stats}"
    );
}

fn row_name(id: i32, version: &str) -> String {
    // A wide value keeps the row count (and test time) low for a large decoded size.
    format!("{id:08}-{version}-{}", "x".repeat(180))
}

fn data_batch(rows: &[(i32, String)]) -> RecordBatch {
    let arrow_schema = schema_to_arrow_schema(&simple_table_schema()).unwrap();
    RecordBatch::try_new(Arc::new(arrow_schema), vec![
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|(_, name)| name),
        )),
    ])
    .unwrap()
}

fn delta_batch(ids: &[i32], version: &str, op: i32) -> RecordBatch {
    let arrow_schema = schema_to_arrow_schema(&simple_table_schema_with_pos()).unwrap();
    RecordBatch::try_new(Arc::new(arrow_schema), vec![
        Arc::new(Int32Array::from(ids.to_vec())),
        Arc::new(StringArray::from_iter_values(
            ids.iter().map(|&id| row_name(id, version)),
        )),
        Arc::new(Int32Array::from(vec![op; ids.len()])),
    ])
    .unwrap()
}

/// Delegates to the runtime's pool and records the peak pool reservation, the
/// peak reservation of each consumer, and the peak spill-file usage.
#[derive(Debug)]
struct ObservedPool {
    inner: Arc<dyn MemoryPool>,
    disk_manager: Arc<DiskManager>,
    peak_reserved: AtomicUsize,
    peak_spilled_bytes: AtomicU64,
    consumers: Mutex<HashMap<String, (bool, usize)>>,
}

impl ObservedPool {
    fn new(inner: Arc<dyn MemoryPool>, disk_manager: Arc<DiskManager>) -> Self {
        Self {
            inner,
            disk_manager,
            peak_reserved: AtomicUsize::new(0),
            peak_spilled_bytes: AtomicU64::new(0),
            consumers: Mutex::new(HashMap::new()),
        }
    }

    /// Runs after the pool accepted `additional`, before `reservation` records it.
    fn observe(&self, reservation: &MemoryReservation, additional: usize) {
        self.peak_reserved
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        self.peak_spilled_bytes
            .fetch_max(self.disk_manager.used_disk_space(), Ordering::Relaxed);
        let consumer = reservation.consumer();
        let mut consumers = self.consumers.lock().unwrap();
        let peak = consumers
            .entry(consumer.name().to_owned())
            .or_insert((consumer.can_spill(), 0));
        peak.1 = peak.1.max(reservation.size() + additional);
    }

    fn consumer_peaks(&self) -> Vec<(String, bool, usize)> {
        let mut peaks = self
            .consumers
            .lock()
            .unwrap()
            .iter()
            .map(|(name, (can_spill, peak))| (name.clone(), *can_spill, *peak))
            .collect::<Vec<_>>();
        peaks.sort();
        peaks
    }
}

impl std::fmt::Display for ObservedPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Observed({})", self.inner)
    }
}

impl MemoryPool for ObservedPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.observe(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DataFusionResult<()> {
        self.inner.try_grow(reservation, additional)?;
        self.observe(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}
