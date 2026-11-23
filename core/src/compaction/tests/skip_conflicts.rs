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

//! The skipping commit must never change the logical content of the table: it leaves out the
//! results whose inputs received deletes after planning, and nothing else.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, Literal, MAIN_BRANCH, PartitionKey,
    Schema, Struct, Transform, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::position_delete_file_writer::{
    POSITION_DELETE_SCHEMA, PositionDeleteFileWriterBuilder, PositionDeleteInput,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation,
    TableIdent,
};
use itertools::Itertools;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use uuid::Uuid;

use super::{
    TestEnv, append_and_commit, create_namespace, simple_table_schema,
    write_puffin_deletion_vectors,
};
use crate::CompactionError;
use crate::compaction::{
    CommitManagerRetryConfig, Compaction, CompactionBuilder, CompactionPlan, RewriteCommit,
    RewriteResult, conflicting_data_file_paths,
};
use crate::config::{CompactionConfigBuilder, CompactionExecutionConfigBuilder};
use crate::file_selection::{FileGroup, FileSelector};

type UpdateHook = Pin<Box<dyn Future<Output = ()> + Send>>;

enum UpdateBehavior {
    Delegate,
    AlwaysConflict,
}

/// Wraps the memory catalog to inject a concurrent writer, or a permanent catalog conflict,
/// into `update_table`.
struct InterceptingCatalog {
    inner: Arc<MemoryCatalog>,
    behavior: UpdateBehavior,
    before_first_update: Mutex<Option<UpdateHook>>,
    update_calls: AtomicUsize,
}

impl std::fmt::Debug for InterceptingCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InterceptingCatalog")
            .field("update_calls", &self.update_calls)
            .finish_non_exhaustive()
    }
}

impl InterceptingCatalog {
    fn new(
        inner: Arc<MemoryCatalog>,
        behavior: UpdateBehavior,
        before_first_update: Option<UpdateHook>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            behavior,
            before_first_update: Mutex::new(before_first_update),
            update_calls: AtomicUsize::new(0),
        })
    }

    fn update_calls(&self) -> usize {
        self.update_calls.load(Ordering::SeqCst)
    }

    fn hook_pending(&self) -> bool {
        self.before_first_update.lock().unwrap().is_some()
    }
}

#[async_trait::async_trait]
impl Catalog for InterceptingCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.update_calls.fetch_add(1, Ordering::SeqCst);
        let hook = self.before_first_update.lock().unwrap().take();
        if let Some(hook) = hook {
            hook.await;
        }
        match self.behavior {
            UpdateBehavior::Delegate => self.inner.update_table(commit).await,
            UpdateBehavior::AlwaysConflict => Err(iceberg::Error::new(
                ErrorKind::CatalogCommitConflicts,
                "injected catalog conflict",
            )
            .with_retryable(true)),
        }
    }
}

async fn create_env(
    format_version: FormatVersion,
    partitioned_by_name: bool,
    properties: &[(&str, &str)],
) -> TestEnv {
    let temp_dir = TempDir::new().unwrap();
    let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    warehouse_location.clone(),
                )]),
            )
            .await
            .unwrap(),
    );

    let namespace_ident = NamespaceIdent::new("skip_conflicts_namespace".into());
    create_namespace(catalog.as_ref(), &namespace_ident).await;

    let partition_spec = if partitioned_by_name {
        UnboundPartitionSpec::builder()
            .add_partition_field(2, "name", Transform::Identity)
            .unwrap()
            .build()
    } else {
        UnboundPartitionSpec::builder().build()
    };
    let table_ident = TableIdent::new(namespace_ident, "skip_conflicts_table".into());
    catalog
        .create_table(
            &table_ident.namespace,
            TableCreation::builder()
                .name(table_ident.name().into())
                .schema(simple_table_schema())
                .partition_spec(partition_spec)
                .format_version(format_version)
                .properties(
                    properties
                        .iter()
                        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
                )
                .build(),
        )
        .await
        .unwrap();
    let table = catalog.load_table(&table_ident).await.unwrap();
    assert_eq!(table.metadata().format_version(), format_version);

    TestEnv {
        temp_dir,
        warehouse_location,
        catalog,
        table_ident,
        table,
    }
}

async fn latest(env: &TestEnv) -> Table {
    env.catalog.load_table(&env.table_ident).await.unwrap()
}

/// `None` for an unpartitioned table, otherwise the partition whose `name` is `name`.
fn partition_key_for(table: &Table, name: &str) -> Option<PartitionKey> {
    let spec = table.metadata().default_partition_spec();
    (!spec.is_unpartitioned()).then(|| {
        PartitionKey::new(
            spec.as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::string(name))]),
        )
    })
}

fn rolling_writer_builder(
    table: &Table,
    warehouse_location: &str,
    schema: Arc<Schema>,
) -> RollingFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
> {
    RollingFileWriterBuilder::new_with_default_file_size(
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema),
        table.file_io().clone(),
        DefaultLocationGenerator::with_data_location(warehouse_location.to_owned()),
        DefaultFileNameGenerator::new(
            "data".to_owned(),
            Some(Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        ),
    )
}

fn rows_batch(schema: &Schema, rows: &[(i32, &str)]) -> RecordBatch {
    RecordBatch::try_new(Arc::new(schema_to_arrow_schema(schema).unwrap()), vec![
        Arc::new(Int32Array::from(rows.iter().map(|row| row.0).collect_vec())),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.1).collect_vec(),
        )),
    ])
    .unwrap()
}

/// On a partitioned table all rows must share one name, which is the partition value.
async fn write_data_file(
    table: &Table,
    warehouse_location: &str,
    rows: &[(i32, &str)],
) -> DataFile {
    let schema = table.metadata().current_schema().clone();
    let partition_key = partition_key_for(table, rows[0].1);
    assert!(partition_key.is_none() || rows.iter().map(|row| row.1).all_equal());
    let mut writer = DataFileWriterBuilder::new(rolling_writer_builder(
        table,
        warehouse_location,
        schema.clone(),
    ))
    .build(partition_key)
    .await
    .unwrap();
    writer.write(rows_batch(&schema, rows)).await.unwrap();
    writer
        .close()
        .await
        .unwrap()
        .into_iter()
        .exactly_one()
        .unwrap()
}

/// `deletes` are `(data file, position)` pairs; the partition is that of the data files.
async fn write_position_delete_file(
    table: &Table,
    warehouse_location: &str,
    partition_name: &str,
    deletes: &[(&DataFile, i64)],
) -> DataFile {
    let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder(
        table,
        warehouse_location,
        Arc::new(POSITION_DELETE_SCHEMA.clone()),
    ))
    .build(partition_key_for(table, partition_name))
    .await
    .unwrap();
    let inputs = deletes
        .iter()
        .map(|(file, position)| PositionDeleteInput::new(Arc::from(file.file_path()), *position))
        .sorted()
        .collect_vec();
    writer.write(inputs).await.unwrap();
    writer
        .close()
        .await
        .unwrap()
        .into_iter()
        .exactly_one()
        .unwrap()
}

/// Deletes every row whose `name` is in `names`, from the data files that are older.
async fn write_equality_delete_file(
    table: &Table,
    warehouse_location: &str,
    names: &[&str],
) -> DataFile {
    let schema = table.metadata().current_schema().clone();
    let name_only = Arc::new(
        Schema::builder()
            .with_fields(vec![schema.field_by_id(2).unwrap().clone()])
            .build()
            .unwrap(),
    );
    let mut writer = EqualityDeleteFileWriterBuilder::new(
        rolling_writer_builder(table, warehouse_location, name_only),
        EqualityDeleteWriterConfig::new(vec![2], schema.clone()).unwrap(),
    )
    .build(None)
    .await
    .unwrap();
    let rows = names.iter().map(|name| (0, *name)).collect_vec();
    writer.write(rows_batch(&schema, &rows)).await.unwrap();
    writer
        .close()
        .await
        .unwrap()
        .into_iter()
        .exactly_one()
        .unwrap()
}

/// All deletion vectors land in one Puffin file.
async fn write_deletion_vectors(
    table: &Table,
    warehouse_location: &str,
    deletes: &[(&DataFile, u64)],
) -> Vec<DataFile> {
    let partitions: HashMap<String, (Struct, i32)> = deletes
        .iter()
        .map(|(file, _)| {
            (
                file.file_path().to_owned(),
                (file.partition().clone(), file.partition_spec_id()),
            )
        })
        .collect();
    let by_path = deletes
        .iter()
        .map(|(file, position)| (file.file_path().to_owned(), *position))
        .collect_vec();
    write_puffin_deletion_vectors(table, warehouse_location, &by_path, |path| {
        partitions[path].clone()
    })
    .await
}

async fn commit_deletion_vectors(
    catalog: &MemoryCatalog,
    table_ident: &TableIdent,
    warehouse_location: &str,
    deletes: &[(&DataFile, u64)],
) -> Table {
    let table = catalog.load_table(table_ident).await.unwrap();
    let deletion_vectors = write_deletion_vectors(&table, warehouse_location, deletes).await;
    append_and_commit(&table, catalog, deletion_vectors).await
}

/// Full rows (every column), sorted, so that duplicated or missing rows show up.
async fn scan_rows_at(table: &Table, snapshot_id: i64) -> Vec<String> {
    let batches: Vec<RecordBatch> = table
        .scan()
        .snapshot_id(snapshot_id)
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let formatters = batch
                .columns()
                .iter()
                .map(|column| {
                    ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default()).unwrap()
                })
                .collect_vec();
            (0..batch.num_rows())
                .map(|row| {
                    formatters
                        .iter()
                        .map(|formatter| formatter.value(row).to_string())
                        .join("|")
                })
                .collect_vec()
        })
        .sorted()
        .collect()
}

fn current_snapshot_id(table: &Table) -> i64 {
    table.metadata().current_snapshot_id().unwrap()
}

async fn scan_rows(table: &Table) -> Vec<String> {
    scan_rows_at(table, current_snapshot_id(table)).await
}

async fn live_files_at(table: &Table, snapshot_id: i64) -> Vec<DataFile> {
    let snapshot = table.metadata().snapshot_by_id(snapshot_id).unwrap();
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &table.metadata_ref())
        .await
        .unwrap();
    let mut files = Vec::new();
    for manifest in manifest_list.entries() {
        let manifest = manifest.load_manifest(table.file_io()).await.unwrap();
        files.extend(
            manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive())
                .map(|entry| entry.data_file().clone()),
        );
    }
    files
}

async fn live_files(table: &Table) -> Vec<DataFile> {
    live_files_at(table, current_snapshot_id(table)).await
}

fn paths(files: &[&DataFile]) -> Vec<String> {
    files
        .iter()
        .map(|file| file.file_path().to_owned())
        .collect()
}

fn input_paths(result: &RewriteResult) -> Vec<String> {
    result
        .plan
        .file_group
        .data_files
        .iter()
        .map(|task| task.data_file_path().to_owned())
        .collect()
}

fn output_paths(result: &RewriteResult) -> Vec<String> {
    result
        .output_data_files
        .iter()
        .map(|file| file.file_path().to_owned())
        .collect()
}

fn group_inputs(results: &[RewriteResult]) -> Vec<Vec<String>> {
    results.iter().map(input_paths).collect()
}

fn live_data_paths(files: &[DataFile]) -> HashSet<String> {
    files
        .iter()
        .filter(|file| file.content_type() == DataContentType::Data)
        .map(|file| file.file_path().to_owned())
        .collect()
}

fn live_delete_blobs(files: &[DataFile]) -> HashSet<(String, Option<i64>)> {
    files
        .iter()
        .filter(|file| file.content_type() != DataContentType::Data)
        .map(|file| (file.file_path().to_owned(), file.content_offset()))
        .collect()
}

fn summary_count(table: &Table, key: &str) -> usize {
    table
        .metadata()
        .current_snapshot()
        .unwrap()
        .summary()
        .additional_properties
        .get(key)
        .map_or(0, |value| value.parse().unwrap())
}

fn build_compaction(
    catalog: Arc<dyn Catalog>,
    table_ident: &TableIdent,
    retry_config: Option<CommitManagerRetryConfig>,
) -> Compaction {
    let builder = CompactionBuilder::new(catalog, table_ident.clone()).with_config(Arc::new(
        CompactionConfigBuilder::default().build().unwrap(),
    ));
    match retry_config {
        Some(retry_config) => builder.with_retry_config(retry_config),
        None => builder,
    }
    .build()
}

fn fast_retry_config(max_retries: u32) -> CommitManagerRetryConfig {
    CommitManagerRetryConfig {
        max_retries,
        retry_initial_delay: std::time::Duration::from_millis(1),
        retry_max_delay: std::time::Duration::from_millis(1),
    }
}

enum DeleteCleanupBound {
    WholePlan,
    OnlyGroup(usize),
}

/// Plans one group per entry of `groups`, as the planner does (including the delete-cleanup bound
/// over every live data file), and rewrites each group.
async fn rewrite_groups(
    compaction: &Compaction,
    table: &Table,
    groups: &[Vec<&DataFile>],
) -> Vec<RewriteResult> {
    rewrite_groups_with_bound(compaction, table, groups, DeleteCleanupBound::WholePlan).await
}

async fn rewrite_groups_with_bound(
    compaction: &Compaction,
    table: &Table,
    groups: &[Vec<&DataFile>],
    bound: DeleteCleanupBound,
) -> Vec<RewriteResult> {
    let snapshot_id = current_snapshot_id(table);
    let tasks = FileSelector::scan_data_files(table, snapshot_id)
        .await
        .unwrap();
    let group_tasks = groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|file| {
                    tasks
                        .iter()
                        .find(|task| task.data_file_path() == file.file_path())
                        .expect("every grouped file must be live in the planned snapshot")
                        .clone()
                })
                .collect_vec()
        })
        .collect_vec();
    let delete_cleanup_bound = match bound {
        DeleteCleanupBound::WholePlan => {
            FileSelector::delete_cleanup_min_data_sequence_number(&tasks)
        }
        DeleteCleanupBound::OnlyGroup(index) => {
            FileSelector::delete_cleanup_min_data_sequence_number(&group_tasks[index])
        }
    };
    let execution_config = CompactionExecutionConfigBuilder::default().build().unwrap();

    let mut results = Vec::new();
    for tasks in group_tasks {
        let plan = CompactionPlan::new(FileGroup::new(tasks), MAIN_BRANCH, snapshot_id)
            .with_delete_cleanup_min_data_sequence_number(delete_cleanup_bound);
        results.push(
            compaction
                .rewrite_plan(plan, &execution_config, table)
                .await
                .unwrap(),
        );
    }
    results
}

/// The strict commit must reject exactly the groups that the skipping commit leaves out, which
/// shows that each scenario really conflicts before it is judged on what the skipping commit does.
async fn assert_strict_commit_rejects(
    compaction: &Compaction,
    results: &[RewriteResult],
    hit_inputs: &[&DataFile],
) {
    let err = compaction
        .commit_rewrite_results(results.to_vec())
        .await
        .expect_err("the strict commit must reject the conflicting groups");
    let CompactionError::Iceberg(err) = err else {
        panic!("expected an iceberg conflict error, got {err:?}");
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(
        conflicting_data_file_paths(&err),
        hit_inputs.iter().map(|file| file.file_path()).collect()
    );
}

/// Checks that `commit` changed nothing about the logical content of the table and that the
/// files match the committed/skipped split. `before_snapshot_id` is the snapshot that the
/// compaction commit was applied to.
async fn assert_commit_preserves_table(
    env: &TestEnv,
    before_snapshot_id: i64,
    commit: &RewriteCommit,
) {
    let after = latest(env).await;
    let committed_snapshot = after.metadata().current_snapshot().unwrap();
    assert_eq!(
        committed_snapshot.snapshot_id(),
        current_snapshot_id(&commit.table)
    );
    assert_eq!(
        committed_snapshot.parent_snapshot_id(),
        Some(before_snapshot_id)
    );

    // (a) same full rows
    assert_eq!(
        scan_rows(&after).await,
        scan_rows_at(&after, before_snapshot_id).await
    );

    let live_before = live_files_at(&after, before_snapshot_id).await;
    let live_after = live_files(&after).await;
    let live_paths: HashSet<String> = live_after
        .iter()
        .map(|file| file.file_path().to_owned())
        .collect();
    let live_data = live_data_paths(&live_after);

    // (b) left-out results: inputs still live, outputs unreferenced
    for result in &commit.skipped {
        assert!(
            input_paths(result)
                .iter()
                .all(|path| live_data.contains(path)),
            "an input of a left-out result is no longer live"
        );
        assert!(
            output_paths(result)
                .iter()
                .all(|path| !live_paths.contains(path)),
            "an output of a left-out result is referenced by the table"
        );
    }

    // (c) committed results: inputs gone, outputs live
    for result in &commit.committed {
        assert!(
            input_paths(result)
                .iter()
                .all(|path| !live_data.contains(path)),
            "an input of a committed result is still live"
        );
        assert!(
            output_paths(result)
                .iter()
                .all(|path| live_data.contains(path)),
            "an output of a committed result is not live"
        );
    }

    // No data file other than the committed results' inputs and outputs changed.
    let committed_inputs: HashSet<String> = commit.committed.iter().flat_map(input_paths).collect();
    let committed_outputs: HashSet<String> =
        commit.committed.iter().flat_map(output_paths).collect();
    let expected_live: HashSet<String> = live_data_paths(&live_before)
        .difference(&committed_inputs)
        .cloned()
        .chain(committed_outputs.iter().cloned())
        .collect();
    assert_eq!(live_data, expected_live);

    // (d) snapshot summary matches the committed results
    assert_eq!(
        summary_count(&after, "added-data-files"),
        committed_outputs.len()
    );
    assert_eq!(
        summary_count(&after, "deleted-data-files"),
        committed_inputs.len()
    );
}

/// A deletion vector on one input of one of three groups across two partitions: only that group
/// is left out.
#[tokio::test]
async fn test_skipping_commit_leaves_out_group_hit_by_deletion_vector() {
    let env = create_env(FormatVersion::V3, true, &[]).await;
    let wh = &env.warehouse_location;
    let a0 = write_data_file(&env.table, wh, &[(1, "a"), (2, "a"), (3, "a")]).await;
    let a1 = write_data_file(&env.table, wh, &[(4, "a"), (5, "a")]).await;
    let a2 = write_data_file(&env.table, wh, &[(6, "a"), (7, "a"), (8, "a")]).await;
    let b0 = write_data_file(&env.table, wh, &[(11, "b"), (12, "b")]).await;
    let b1 = write_data_file(&env.table, wh, &[(13, "b"), (14, "b")]).await;
    let planned = append_and_commit(&env.table, env.catalog.as_ref(), vec![
        a0.clone(),
        a1.clone(),
        a2.clone(),
        b0.clone(),
        b1.clone(),
    ])
    .await;

    let compaction = build_compaction(env.catalog.clone(), &env.table_ident, None);
    let results = rewrite_groups(&compaction, &planned, &[vec![&a0, &a1], vec![&a2], vec![
        &b0, &b1,
    ]])
    .await;

    commit_deletion_vectors(&env.catalog, &env.table_ident, wh, &[(&a2, 1)]).await;
    let before = latest(&env).await;
    assert!(!scan_rows(&before).await.contains(&"7|a".to_owned()));

    assert_strict_commit_rejects(&compaction, &results, &[&a2]).await;

    let commit = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .unwrap();

    assert_eq!(group_inputs(&commit.skipped), vec![paths(&[&a2])]);
    assert_eq!(commit.committed.len(), 2);
    assert_commit_preserves_table(&env, current_snapshot_id(&before), &commit).await;
}

/// A position delete file without a referenced data file conflicts with every replaced file in its
/// partition, so both groups of that partition are left out and the other partition commits.
#[tokio::test]
async fn test_skipping_commit_leaves_out_partition_groups_for_unreferenced_position_delete() {
    let env = create_env(FormatVersion::V2, true, &[]).await;
    let wh = &env.warehouse_location;
    let a0 = write_data_file(&env.table, wh, &[(1, "a"), (2, "a")]).await;
    let a1 = write_data_file(&env.table, wh, &[(3, "a"), (4, "a")]).await;
    let a2 = write_data_file(&env.table, wh, &[(5, "a"), (6, "a")]).await;
    let b0 = write_data_file(&env.table, wh, &[(11, "b"), (12, "b")]).await;
    let planned = append_and_commit(&env.table, env.catalog.as_ref(), vec![
        a0.clone(),
        a1.clone(),
        a2.clone(),
        b0.clone(),
    ])
    .await;

    let compaction = build_compaction(env.catalog.clone(), &env.table_ident, None);
    let results = rewrite_groups(&compaction, &planned, &[vec![&a0, &a1], vec![&a2], vec![
        &b0,
    ]])
    .await;

    let position_delete =
        write_position_delete_file(&env.table, wh, "a", &[(&a0, 0), (&a1, 0)]).await;
    assert_eq!(position_delete.referenced_data_file(), None);
    append_and_commit(&planned, env.catalog.as_ref(), vec![position_delete]).await;
    let before = latest(&env).await;

    assert_strict_commit_rejects(&compaction, &results, &[&a0, &a1, &a2]).await;

    let commit = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .unwrap();

    assert_eq!(group_inputs(&commit.committed), vec![paths(&[&b0])]);
    assert_eq!(group_inputs(&commit.skipped), vec![
        paths(&[&a0, &a1]),
        paths(&[&a2])
    ]);
    assert_commit_preserves_table(&env, current_snapshot_id(&before), &commit).await;
}

/// A position delete file that applies to two groups' inputs and predates planning: when a
/// referenced position delete then hits only one of the groups, the left-out group keeps it.
#[tokio::test]
async fn test_skipping_commit_keeps_position_delete_of_left_out_group() {
    let env = create_env(FormatVersion::V2, false, &[]).await;
    let wh = &env.warehouse_location;
    let a = write_data_file(&env.table, wh, &[(1, "x"), (2, "x"), (3, "x")]).await;
    let b1 = write_data_file(&env.table, wh, &[(11, "x"), (12, "x"), (13, "x")]).await;
    let b2 = write_data_file(&env.table, wh, &[(21, "x"), (22, "x"), (23, "x")]).await;
    let appended = append_and_commit(&env.table, env.catalog.as_ref(), vec![
        a.clone(),
        b1.clone(),
        b2.clone(),
    ])
    .await;
    let shared_delete = write_position_delete_file(&env.table, wh, "", &[(&a, 0), (&b1, 0)]).await;
    assert_eq!(shared_delete.referenced_data_file(), None);
    let shared_delete_path = shared_delete.file_path().to_owned();
    let planned = append_and_commit(&appended, env.catalog.as_ref(), vec![shared_delete]).await;
    assert_eq!(scan_rows(&planned).await.len(), 7);

    let compaction = build_compaction(env.catalog.clone(), &env.table_ident, None);
    let results = rewrite_groups(&compaction, &planned, &[vec![&a], vec![&b1, &b2]]).await;

    let hit = write_position_delete_file(&env.table, wh, "", &[(&b2, 1)]).await;
    assert_eq!(hit.referenced_data_file(), Some(b2.file_path().to_owned()));
    append_and_commit(&planned, env.catalog.as_ref(), vec![hit]).await;
    let before = latest(&env).await;
    assert_eq!(scan_rows(&before).await.len(), 6);

    assert_strict_commit_rejects(&compaction, &results, &[&b2]).await;

    let commit = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .unwrap();

    assert_eq!(group_inputs(&commit.skipped), vec![paths(&[&b1, &b2])]);
    assert_eq!(group_inputs(&commit.committed), vec![paths(&[&a])]);
    assert_commit_preserves_table(&env, current_snapshot_id(&before), &commit).await;
    let live = live_files(&latest(&env).await).await;
    assert!(live_delete_blobs(&live).contains(&(shared_delete_path, None)));
}

/// Deletion vectors of two groups' inputs share one Puffin. The kept group's rewrite makes its own
/// blob dangling, which must not take the left-out group's blob with it.
#[tokio::test]
async fn test_skipping_commit_keeps_shared_puffin_deletion_vector_of_left_out_group() {
    let env = create_env(FormatVersion::V3, false, &[]).await;
    let wh = &env.warehouse_location;
    let a = write_data_file(&env.table, wh, &[(1, "x"), (2, "x"), (3, "x")]).await;
    let b1 = write_data_file(&env.table, wh, &[(11, "x"), (12, "x"), (13, "x")]).await;
    let b2 = write_data_file(&env.table, wh, &[(21, "x"), (22, "x"), (23, "x")]).await;
    append_and_commit(&env.table, env.catalog.as_ref(), vec![
        a.clone(),
        b1.clone(),
        b2.clone(),
    ])
    .await;
    let planned =
        commit_deletion_vectors(&env.catalog, &env.table_ident, wh, &[(&a, 0), (&b1, 0)]).await;
    assert_eq!(scan_rows(&planned).await.len(), 7);
    let planned_blobs = live_delete_blobs(&live_files(&planned).await);
    assert_eq!(planned_blobs.len(), 2);

    let compaction = build_compaction(env.catalog.clone(), &env.table_ident, None);
    let results = rewrite_groups(&compaction, &planned, &[vec![&a], vec![&b1, &b2]]).await;

    commit_deletion_vectors(&env.catalog, &env.table_ident, wh, &[(&b2, 1)]).await;
    let before = latest(&env).await;
    assert_eq!(scan_rows(&before).await.len(), 6);

    assert_strict_commit_rejects(&compaction, &results, &[&b2]).await;

    let commit = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .unwrap();

    assert_eq!(group_inputs(&commit.skipped), vec![paths(&[&b1, &b2])]);
    assert_eq!(group_inputs(&commit.committed), vec![paths(&[&a])]);
    assert_commit_preserves_table(&env, current_snapshot_id(&before), &commit).await;

    let left_out_blob = live_files(&planned)
        .await
        .into_iter()
        .find(|file| file.referenced_data_file().as_deref() == Some(b1.file_path()))
        .map(|file| (file.file_path().to_owned(), file.content_offset()))
        .unwrap();
    let live = live_files(&latest(&env).await).await;
    assert!(live_delete_blobs(&live).contains(&left_out_blob));
}

struct EqualityDeleteTable {
    env: TestEnv,
    a: DataFile,
    b: DataFile,
    c: DataFile,
    equality_delete_path: String,
    planned: Table,
}

/// An equality delete that predates planning and applies to `A` (when `with_a`) and `B`, plus a
/// newer file `C` with a deletion vector that the equality delete does not apply to.
async fn equality_delete_table(with_a: bool) -> EqualityDeleteTable {
    let env = create_env(FormatVersion::V3, false, &[]).await;
    let wh = &env.warehouse_location;
    let a = write_data_file(&env.table, wh, &[(1, "Alice"), (2, "Bob"), (3, "Cleo")]).await;
    let b = write_data_file(&env.table, wh, &[(11, "Alice"), (12, "Bob"), (13, "Dave")]).await;
    let older = if with_a {
        vec![a.clone(), b.clone()]
    } else {
        vec![b.clone()]
    };
    let appended = append_and_commit(&env.table, env.catalog.as_ref(), older).await;
    let equality_delete = write_equality_delete_file(&env.table, wh, &["Bob"]).await;
    let equality_delete_path = equality_delete.file_path().to_owned();
    let with_equality_delete =
        append_and_commit(&appended, env.catalog.as_ref(), vec![equality_delete]).await;
    let c = write_data_file(&env.table, wh, &[(21, "Alice"), (22, "Bob"), (23, "Erin")]).await;
    append_and_commit(&with_equality_delete, env.catalog.as_ref(), vec![c.clone()]).await;
    let planned = commit_deletion_vectors(&env.catalog, &env.table_ident, wh, &[(&c, 0)]).await;

    let planned_rows = scan_rows(&planned).await;
    assert!(!planned_rows.contains(&"12|Bob".to_owned()));
    assert!(planned_rows.contains(&"22|Bob".to_owned()));
    assert!(!planned_rows.contains(&"21|Alice".to_owned()));

    EqualityDeleteTable {
        env,
        a,
        b,
        c,
        equality_delete_path,
        planned,
    }
}

/// A deletion vector then hits `B`, which is left out.
///
/// Without `A` the committed groups hold no file the equality delete applies to, so a cleanup
/// bound computed over the committed groups alone would retire the delete that `B` still needs.
async fn assert_equality_delete_survives_left_out_group(with_a: bool) {
    let EqualityDeleteTable {
        env,
        a,
        b,
        c,
        equality_delete_path,
        planned,
    } = equality_delete_table(with_a).await;
    let compaction = build_compaction(env.catalog.clone(), &env.table_ident, None);
    let mut groups = vec![vec![&b], vec![&c]];
    if with_a {
        groups.insert(0, vec![&a]);
    }
    let results = rewrite_groups(&compaction, &planned, &groups).await;

    commit_deletion_vectors(&env.catalog, &env.table_ident, &env.warehouse_location, &[
        (&b, 0),
    ])
    .await;
    let before = latest(&env).await;
    assert!(!scan_rows(&before).await.contains(&"11|Alice".to_owned()));
    assert_strict_commit_rejects(&compaction, &results, &[&b]).await;

    let commit = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .unwrap();

    assert_eq!(group_inputs(&commit.skipped), vec![paths(&[&b])]);
    assert_eq!(commit.committed.len(), if with_a { 2 } else { 1 });
    assert_commit_preserves_table(&env, current_snapshot_id(&before), &commit).await;
    let live = live_files(&latest(&env).await).await;
    assert!(live_delete_blobs(&live).contains(&(equality_delete_path, None)));
}

#[tokio::test]
async fn test_skipping_commit_keeps_equality_delete_for_left_out_group() {
    assert_equality_delete_survives_left_out_group(true).await;
}

#[tokio::test]
async fn test_skipping_commit_keeps_equality_delete_when_only_newer_groups_commit() {
    assert_equality_delete_survives_left_out_group(false).await;
}

/// Control for the two tests above: a cleanup bound taken from the committed group alone retires
/// the equality delete and brings the deleted rows of the left-out group back, so those tests do
/// fail for a wrong bound.
#[tokio::test]
async fn test_skipping_commit_fixture_detects_cleanup_bound_over_committed_groups_only() {
    let EqualityDeleteTable {
        env,
        b,
        c,
        equality_delete_path,
        planned,
        ..
    } = equality_delete_table(false).await;
    let compaction = build_compaction(env.catalog.clone(), &env.table_ident, None);
    let results = rewrite_groups_with_bound(
        &compaction,
        &planned,
        &[vec![&b], vec![&c]],
        DeleteCleanupBound::OnlyGroup(1),
    )
    .await;
    commit_deletion_vectors(&env.catalog, &env.table_ident, &env.warehouse_location, &[
        (&b, 0),
    ])
    .await;

    let commit = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .unwrap();

    assert_eq!(group_inputs(&commit.skipped), vec![paths(&[&b])]);
    let after = latest(&env).await;
    assert!(scan_rows(&after).await.contains(&"12|Bob".to_owned()));
    let live = live_files(&after).await;
    assert!(!live_delete_blobs(&live).contains(&(equality_delete_path, None)));
}

struct ThreeFileTable {
    env: TestEnv,
    files: Vec<DataFile>,
    planned: Table,
}

async fn three_file_table(properties: &[(&str, &str)]) -> ThreeFileTable {
    let env = create_env(FormatVersion::V3, false, properties).await;
    let wh = &env.warehouse_location;
    let files = vec![
        write_data_file(&env.table, wh, &[(1, "x"), (2, "x"), (3, "x")]).await,
        write_data_file(&env.table, wh, &[(11, "x"), (12, "x"), (13, "x")]).await,
        write_data_file(&env.table, wh, &[(21, "x"), (22, "x"), (23, "x")]).await,
    ];
    let planned = append_and_commit(&env.table, env.catalog.as_ref(), files.clone()).await;
    ThreeFileTable {
        env,
        files,
        planned,
    }
}

fn one_group_per_file(files: &[DataFile]) -> Vec<Vec<&DataFile>> {
    files.iter().map(|file| vec![file]).collect()
}

fn concurrent_deletion_vector_hook(env: &TestEnv, target: &DataFile, position: u64) -> UpdateHook {
    let catalog = env.catalog.clone();
    let table_ident = env.table_ident.clone();
    let warehouse_location = env.warehouse_location.clone();
    let target = target.clone();
    Box::pin(async move {
        commit_deletion_vectors(&catalog, &table_ident, &warehouse_location, &[(
            &target, position,
        )])
        .await;
    })
}

/// The delete arrives after the commit loaded the table but before its catalog commit. The
/// catalog rejects that commit; the next attempt finds the delete and the hit group is left out.
async fn assert_conflict_during_commit_is_resolved(
    table_properties: &[(&str, &str)],
    retry_config: CommitManagerRetryConfig,
) {
    let ThreeFileTable {
        env,
        files,
        planned,
    } = three_file_table(table_properties).await;
    let hook = concurrent_deletion_vector_hook(&env, &files[1], 1);
    let catalog =
        InterceptingCatalog::new(env.catalog.clone(), UpdateBehavior::Delegate, Some(hook));
    let compaction = build_compaction(catalog.clone(), &env.table_ident, Some(retry_config));
    let results = rewrite_groups(&compaction, &planned, &one_group_per_file(&files)).await;

    let commit = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .unwrap();

    assert!(!catalog.hook_pending());
    assert_eq!(catalog.update_calls(), 2);
    assert_eq!(group_inputs(&commit.skipped), vec![paths(&[&files[1]])]);
    assert_eq!(commit.committed.len(), 2);

    let after = latest(&env).await;
    let before_snapshot_id = after
        .metadata()
        .current_snapshot()
        .unwrap()
        .parent_snapshot_id()
        .unwrap();
    assert_ne!(before_snapshot_id, current_snapshot_id(&planned));
    assert!(
        !scan_rows_at(&after, before_snapshot_id)
            .await
            .contains(&"12|x".to_owned())
    );
    assert_commit_preserves_table(&env, before_snapshot_id, &commit).await;
}

#[tokio::test]
async fn test_skipping_commit_resolves_conflict_during_commit_with_retries_enabled() {
    assert_conflict_during_commit_is_resolved(
        &[
            ("commit.retry.min-wait-ms", "1"),
            ("commit.retry.max-wait-ms", "1"),
        ],
        fast_retry_config(3),
    )
    .await;
}

/// With both retry layers off, the first catalog conflict ends the commit, and the one extra
/// attempt is what finds and leaves out the hit group.
#[tokio::test]
async fn test_skipping_commit_resolves_conflict_during_commit_with_retries_disabled() {
    assert_conflict_during_commit_is_resolved(
        &[("commit.retry.num-retries", "0")],
        fast_retry_config(0),
    )
    .await;
}

/// Control for the retries-disabled test above: the strict commit gets the same catalog conflict
/// and has no further attempt, so only the skipping commit's extra attempt resolves it.
#[tokio::test]
async fn test_skipping_commit_fixture_strict_commit_fails_on_conflict_during_commit_without_retries()
 {
    let ThreeFileTable {
        env,
        files,
        planned,
    } = three_file_table(&[("commit.retry.num-retries", "0")]).await;
    let hook = concurrent_deletion_vector_hook(&env, &files[1], 1);
    let catalog =
        InterceptingCatalog::new(env.catalog.clone(), UpdateBehavior::Delegate, Some(hook));
    let compaction = build_compaction(
        catalog.clone(),
        &env.table_ident,
        Some(fast_retry_config(0)),
    );
    let results = rewrite_groups(&compaction, &planned, &one_group_per_file(&files)).await;

    let err = compaction
        .commit_rewrite_results(results)
        .await
        .expect_err("the strict commit has no attempt left after the catalog conflict");

    assert!(
        matches!(&err, CompactionError::Iceberg(err) if err.kind() == ErrorKind::CatalogCommitConflicts),
        "unexpected error {err:?}"
    );
    assert_eq!(catalog.update_calls(), 1);
}

#[tokio::test]
async fn test_skipping_commit_fails_and_leaves_table_unchanged_when_every_group_is_hit() {
    let ThreeFileTable {
        env,
        files,
        planned,
    } = three_file_table(&[]).await;
    let compaction = build_compaction(env.catalog.clone(), &env.table_ident, None);
    let results = rewrite_groups(&compaction, &planned, &one_group_per_file(&files)).await;

    let before =
        commit_deletion_vectors(&env.catalog, &env.table_ident, &env.warehouse_location, &[
            (&files[0], 0),
            (&files[1], 0),
            (&files[2], 0),
        ])
        .await;
    let rows_before = scan_rows(&before).await;
    let files_before = live_data_paths(&live_files(&before).await);

    let err = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .err()
        .expect("a commit where every group conflicts must fail");

    let CompactionError::Iceberg(err) = err else {
        panic!("expected an iceberg conflict error, got {err:?}");
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(
        conflicting_data_file_paths(&err),
        files.iter().map(DataFile::file_path).collect()
    );
    let after = latest(&env).await;
    assert_eq!(
        after.metadata().current_snapshot_id(),
        before.metadata().current_snapshot_id()
    );
    assert_eq!(after.metadata_location(), before.metadata_location());
    assert_eq!(scan_rows(&after).await, rows_before);
    assert_eq!(live_data_paths(&live_files(&after).await), files_before);
}

/// A permanent catalog conflict names no files. Each commit makes `(num_retries + 1)` catalog
/// calls through the transaction layer and `(max_retries + 1)` transaction runs through the
/// commit manager, and the skipping commit adds exactly one more commit.
#[tokio::test]
async fn test_skipping_commit_bounds_attempts_on_permanent_catalog_conflict() {
    for (num_retries, max_retries) in [(0_u32, 0_u32), (2, 1)] {
        let ThreeFileTable {
            env,
            files,
            planned,
        } = three_file_table(&[
            ("commit.retry.num-retries", &num_retries.to_string()),
            ("commit.retry.min-wait-ms", "1"),
            ("commit.retry.max-wait-ms", "1"),
        ])
        .await;
        let catalog =
            InterceptingCatalog::new(env.catalog.clone(), UpdateBehavior::AlwaysConflict, None);
        let compaction = build_compaction(
            catalog.clone(),
            &env.table_ident,
            Some(fast_retry_config(max_retries)),
        );
        let results = rewrite_groups(&compaction, &planned, &one_group_per_file(&files)).await;
        let before = latest(&env).await;

        let err = compaction
            .commit_rewrite_results_skipping_conflicts(results)
            .await
            .err()
            .expect("a permanent catalog conflict must fail the commit");

        assert!(
            matches!(&err, CompactionError::Iceberg(err) if err.kind() == ErrorKind::CatalogCommitConflicts),
            "unexpected error {err:?}"
        );
        assert_eq!(
            catalog.update_calls(),
            ((num_retries + 1) * (max_retries + 1) * 2) as usize,
            "num_retries={num_retries}, max_retries={max_retries}"
        );
        let after = latest(&env).await;
        assert_eq!(after.metadata_location(), before.metadata_location());
        assert_eq!(scan_rows(&after).await, scan_rows(&before).await);
    }
}

/// A new data file and a deletion vector for a file that no plan replaces do not conflict.
#[tokio::test]
async fn test_skipping_commit_commits_everything_for_unrelated_concurrent_writes() {
    let ThreeFileTable {
        env,
        files,
        planned,
    } = three_file_table(&[]).await;
    let wh = &env.warehouse_location;
    let compaction = build_compaction(env.catalog.clone(), &env.table_ident, None);
    let results = rewrite_groups(&compaction, &planned, &one_group_per_file(&files[..2])).await;

    let new_file = write_data_file(&env.table, wh, &[(31, "x"), (32, "x")]).await;
    append_and_commit(&planned, env.catalog.as_ref(), vec![new_file]).await;
    commit_deletion_vectors(&env.catalog, &env.table_ident, wh, &[(&files[2], 0)]).await;
    let before = latest(&env).await;
    assert_eq!(scan_rows(&before).await.len(), 10);

    let commit = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .unwrap();

    assert!(commit.skipped.is_empty());
    assert_eq!(commit.committed.len(), 2);
    assert_commit_preserves_table(&env, current_snapshot_id(&before), &commit).await;
}

/// With the starting sequence number pinned, the rewritten files are older than a new equality
/// delete, so the delete applies to them and nothing has to be left out.
#[tokio::test]
async fn test_skipping_commit_commits_everything_for_new_equality_delete() {
    let env = create_env(FormatVersion::V3, false, &[]).await;
    let wh = &env.warehouse_location;
    let a = write_data_file(&env.table, wh, &[(1, "Alice"), (2, "Bob")]).await;
    let b = write_data_file(&env.table, wh, &[(11, "Bob"), (12, "Cleo")]).await;
    let planned =
        append_and_commit(&env.table, env.catalog.as_ref(), vec![a.clone(), b.clone()]).await;
    let compaction = build_compaction(env.catalog.clone(), &env.table_ident, None);
    let results = rewrite_groups(&compaction, &planned, &[vec![&a], vec![&b]]).await;

    let equality_delete = write_equality_delete_file(&env.table, wh, &["Bob"]).await;
    append_and_commit(&planned, env.catalog.as_ref(), vec![equality_delete]).await;
    let before = latest(&env).await;
    assert_eq!(scan_rows(&before).await, vec!["12|Cleo", "1|Alice"]);

    let commit = compaction
        .commit_rewrite_results_skipping_conflicts(results)
        .await
        .unwrap();

    assert!(commit.skipped.is_empty());
    assert_eq!(commit.committed.len(), 2);
    assert_commit_preserves_table(&env, current_snapshot_id(&before), &commit).await;
}
