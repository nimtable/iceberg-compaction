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

use iceberg::io::FileIO;
use iceberg::spec::{DataFile, Snapshot};
use iceberg::{Catalog, ErrorKind, TableIdent};
use mixtrics::metrics::BoxedRegistry;
use mixtrics::registry::noop::NoopMetricsRegistry;

use crate::common::{CompactionMetricsRecorder, Metrics};
use crate::compaction::validator::CompactionValidator;
use crate::config::{
    CompactionExecutionConfig, CompactionPlanningConfig, RuntimeConfig, RuntimeConfigBuilder,
};
use crate::executor::{
    create_compaction_executor, ExecutorType, InputFileScanTasks, RewriteFilesRequest,
    RewriteFilesResponse, RewriteFilesStat,
};
use crate::file_selection::FileSelector;
use crate::CompactionError;
use crate::Result;
use crate::{CompactionConfig, CompactionExecutor};
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use std::sync::Arc;
use std::time::Duration;

use backon::ExponentialBuilder;
use backon::Retryable;

mod validator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionType {
    Full,
    MergeSmallDataFiles,
}

/// Builder for creating Compaction instances with flexible configuration
pub struct CompactionBuilder {
    config: Option<Arc<CompactionConfig>>,
    executor_type: ExecutorType,
    catalog: Option<Arc<dyn Catalog>>,
    registry: BoxedRegistry,
    table_ident: Option<TableIdent>,
    compaction_type: Option<CompactionType>,
    catalog_name: Option<String>,
    commit_retry_config: RewriteDataFilesCommitManagerRetryConfig,
}

impl CompactionBuilder {
    /// Create a new `CompactionBuilder` with default settings
    pub fn new() -> Self {
        Self {
            config: None,
            executor_type: ExecutorType::DataFusion, // Default executor type
            catalog: None,
            registry: Box::new(NoopMetricsRegistry),
            table_ident: None,
            compaction_type: None,
            catalog_name: None,
            commit_retry_config: RewriteDataFilesCommitManagerRetryConfig::default(),
        }
    }

    /// Set the compaction configuration
    pub fn with_config(mut self, config: Arc<CompactionConfig>) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the executor type (defaults to `DataFusion`)
    pub fn with_executor_type(mut self, executor_type: ExecutorType) -> Self {
        self.executor_type = executor_type;
        self
    }

    /// Set the catalog
    pub fn with_catalog(mut self, catalog: Arc<dyn Catalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Set the metrics registry (optional, defaults to `NoopMetricsRegistry`)
    pub fn with_registry(mut self, registry: BoxedRegistry) -> Self {
        self.registry = registry;
        self
    }

    pub fn with_table_ident(mut self, table_ident: TableIdent) -> Self {
        self.table_ident = Some(table_ident);
        self
    }

    pub fn with_compaction_type(mut self, compaction_type: CompactionType) -> Self {
        self.compaction_type = Some(compaction_type);
        self
    }

    pub fn with_catalog_name(mut self, catalog_name: String) -> Self {
        self.catalog_name = Some(catalog_name);
        self
    }

    pub fn with_retry_config(
        mut self,
        retry_config: RewriteDataFilesCommitManagerRetryConfig,
    ) -> Self {
        self.commit_retry_config = retry_config;
        self
    }

    /// Build the Compaction instance
    pub async fn build(self) -> Result<Compaction> {
        let config = self.config.ok_or_else(|| {
            crate::error::CompactionError::Execution("CompactionConfig is required".to_owned())
        })?;

        let catalog = self.catalog.ok_or_else(|| {
            crate::error::CompactionError::Execution("Catalog is required".to_owned())
        })?;

        let table_ident = self.table_ident.ok_or_else(|| {
            crate::error::CompactionError::Execution("TableIdent is required".to_owned())
        })?;

        let compaction_type = self.compaction_type.unwrap_or(CompactionType::Full);

        if !catalog.table_exists(&table_ident).await? {
            return Err(crate::error::CompactionError::Execution(
                "Table does not exist".to_owned(),
            ));
        }

        let executor = create_compaction_executor(self.executor_type);

        let metrics = Arc::new(Metrics::new(self.registry));

        let catalog_name = self.catalog_name.unwrap_or_default();

        let commit_retry_config = self.commit_retry_config;

        Ok(Compaction {
            config,
            executor,
            catalog,
            metrics,
            table_ident,
            compaction_type,
            catalog_name,
            commit_retry_config,
        })
    }
}

impl Default for CompactionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Compaction implementation with support for both managed and plan-driven workflows
///
/// # Usage Examples
///
/// ## Managed Workflow (Simple and Automatic)
///
/// For most use cases, use the simple one-step compaction that automatically handles
/// planning, execution, and commit:
///
/// ```rust,no_run
/// use iceberg_compaction_core::compaction::{CompactionBuilder, CompactionType};
/// use iceberg_compaction_core::config::CompactionConfig;
/// use std::sync::Arc;
/// use iceberg::TableIdent;
/// use iceberg_catalog_memory::MemoryCatalog;
/// use iceberg::io::FileIOBuilder;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let file_io = FileIOBuilder::new_fs_io().build()?;
/// # let catalog = Arc::new(MemoryCatalog::new(file_io, Some("/tmp/warehouse".to_string())));
/// # let table_ident = TableIdent::from_strs(["default", "test_table"])?;
/// # let config = CompactionConfig::default();
/// let compaction = CompactionBuilder::new()
///     .with_catalog(catalog)
///     .with_table_ident(table_ident)
///     .with_compaction_type(CompactionType::MergeSmallDataFiles)
///     .with_config(Arc::new(config))
///     .build()
///     .await?;
///
/// // Simple one-step execution - system handles everything
/// let stats = compaction.compact().await?;
/// println!("Compacted {} files into {} files",
///          stats.input_files_count, stats.output_files_count);
/// # Ok(())
/// # }
/// ```
///
/// ## Plan-Driven Workflow (Preview and Control)
///
/// For scenarios requiring preview or fine-grained control, use the two-step approach:
///
/// ```rust,no_run
/// use iceberg_compaction_core::compaction::{CompactionBuilder, CompactionType, CompactionPlanner};
/// use iceberg_compaction_core::config::{CompactionConfig, CompactionPlanningConfigBuilder};
/// use std::sync::Arc;
/// use iceberg::{TableIdent, Catalog};
/// use iceberg_catalog_memory::MemoryCatalog;
/// use iceberg::io::FileIOBuilder;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let file_io = FileIOBuilder::new_fs_io().build()?;
/// # let catalog = Arc::new(MemoryCatalog::new(file_io, Some("/tmp/warehouse".to_string())));
/// # let table_ident = TableIdent::from_strs(["default", "test_table"])?;
/// # let config = CompactionConfig::default();
/// let compaction = CompactionBuilder::new()
///     .with_catalog(catalog.clone())
///     .with_table_ident(table_ident.clone())
///     .with_compaction_type(CompactionType::Full)
///     .with_config(Arc::new(config.clone()))
///     .build()
///     .await?;
///
/// // Step 1: Create a planner and generate a plan (preview what will be processed)
/// let planner = CompactionPlanner::new(config.planning.clone());
/// let table = catalog.load_table(&table_ident).await?;
/// let plan = planner.plan_compaction(&table, CompactionType::Full).await?;
///
/// // Preview the plan details
/// println!("Plan will process {} files ({} bytes)",
///          plan.file_count(), plan.total_bytes());
/// println!("Recommended parallelism: executor={}, output={}",
///          plan.recommended_executor_parallelism(),
///          plan.recommended_output_parallelism());
///
/// // Step 2: Execute with the plan (commit is still automatic)
/// if !plan.is_empty() {
///     let stats = compaction.compact_with_plan(plan).await?;
///     println!("Compaction completed: {} -> {} files",
///              stats.input_files_count, stats.output_files_count);
/// }
/// # Ok(())
/// # }
/// ```
pub struct Compaction {
    pub config: Arc<CompactionConfig>,
    pub executor: Box<dyn CompactionExecutor>,
    pub catalog: Arc<dyn Catalog>,
    pub metrics: Arc<Metrics>,
    pub table_ident: TableIdent,
    pub compaction_type: CompactionType,
    pub catalog_name: String,
    pub commit_retry_config: RewriteDataFilesCommitManagerRetryConfig,
}

impl std::fmt::Debug for Compaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compaction")
            .field("config", &self.config)
            .field("executor", &"<dyn CompactionExecutor>")
            .field("catalog", &"<dyn Catalog>")
            .field("metrics", &"<Arc<Metrics>>")
            .field("table_ident", &self.table_ident)
            .field("compaction_type", &self.compaction_type)
            .field("catalog_name", &self.catalog_name)
            .field("commit_retry_config", &self.commit_retry_config)
            .finish()
    }
}

struct CompactionResult {
    stats: RewriteFilesStat,
    compaction_validator: Option<CompactionValidator>,
}

impl Compaction {
    /// Create a new `CompactionBuilder` for flexible configuration
    pub fn builder() -> CompactionBuilder {
        CompactionBuilder::new()
    }

    pub async fn compact(&self) -> Result<RewriteFilesStat> {
        let table = self.catalog.load_table(&self.table_ident).await?;
        // 1. plan the compaction
        let compaction_planner = CompactionPlanner::new(self.config.planning.clone());

        let plan = compaction_planner
            .plan_compaction(&table, self.compaction_type)
            .await?;

        // 2. execute the compaction with the plan
        self.compact_with_plan(plan, &self.config.execution).await
    }

    /// Standard compaction implementation for simple file-based compaction types
    /// This works well for Full and `SmallFiles` compaction where the main difference
    /// is the `FileStrategy` used for file selection
    async fn execute_standard_compaction(
        &self,
        plan: CompactionPlan,
        execution_config: &CompactionExecutionConfig,
    ) -> Result<CompactionResult> {
        let now = std::time::Instant::now();
        let metrics_recorder = CompactionMetricsRecorder::new(
            self.metrics.clone(),
            self.catalog_name.clone(),
            self.table_ident.to_string(),
        );

        let table = self.catalog.load_table(&self.table_ident).await?;

        // check if the current snapshot exists
        if table.metadata().current_snapshot().is_none() {
            return Ok(CompactionResult {
                stats: RewriteFilesStat::default(),
                compaction_validator: None,
            });
        }

        let current_snapshot = table.metadata().current_snapshot().unwrap();
        // Check if any input files were selected
        if plan.files_to_compact.input_files_count() == 0 {
            return Ok(CompactionResult {
                stats: RewriteFilesStat::default(),
                compaction_validator: None,
            });
        }

        let mut input_tasks_for_validation = if execution_config.enable_validate_compaction {
            Some(plan.files_to_compact.clone())
        } else {
            None
        };

        // Step 2: Create rewrite request
        let rewrite_files_request = self.create_rewrite_request(&table, &plan)?;

        // Step 3: Execute rewrite
        let RewriteFilesResponse {
            data_files: mut output_data_files,
            stats,
        } = match self.executor.rewrite_files(rewrite_files_request).await {
            Ok(response) => response,
            Err(e) => {
                metrics_recorder.record_executor_error();
                return Err(e);
            }
        };

        let commit_now = std::time::Instant::now();
        let output_data_files_for_commit = if execution_config.enable_validate_compaction {
            output_data_files.clone()
        } else {
            std::mem::take(&mut output_data_files)
        };

        // Step 4: Commit results (extensible)

        let committed_table = match self
            .commit_compaction_results(
                &table,
                output_data_files_for_commit,
                &plan.files_to_compact,
                current_snapshot,
                table.file_io(),
            )
            .await
        {
            Ok(table) => table,
            Err(e) => {
                metrics_recorder.record_executor_error();
                return Err(e);
            }
        };

        // Step 5: Update metrics
        self.update_metrics(&metrics_recorder, &stats, now, commit_now);

        // Step 6: Setup validation if enabled
        let compaction_validator = if execution_config.enable_validate_compaction {
            Some(
                CompactionValidator::new(
                    input_tasks_for_validation.take().unwrap(),
                    output_data_files,
                    plan.runtime_config,
                    table.metadata().current_schema().clone(),
                    table.metadata().current_schema().clone(),
                    committed_table,
                    self.catalog_name.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        Ok(CompactionResult {
            stats,
            compaction_validator,
        })
    }

    /// Helper method to update metrics
    fn update_metrics(
        &self,
        metrics_recorder: &CompactionMetricsRecorder,
        stats: &RewriteFilesStat,
        start_time: std::time::Instant,
        commit_start_time: std::time::Instant,
    ) {
        metrics_recorder.record_commit_duration(commit_start_time.elapsed().as_secs_f64());
        metrics_recorder.record_compaction_duration(start_time.elapsed().as_secs_f64());
        metrics_recorder.record_compaction_complete(stats);
    }

    /// Hook for customizing the rewrite request configuration
    /// Default implementation creates a standard request, but can be customized
    fn create_rewrite_request(
        &self,
        table: &Table,
        plan: &CompactionPlan,
    ) -> Result<RewriteFilesRequest> {
        let schema = table.metadata().current_schema();
        let default_location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let metrics_recorder = CompactionMetricsRecorder::new(
            self.metrics.clone(),
            self.catalog_name.clone(),
            self.table_ident.to_string(),
        );

        Ok(RewriteFilesRequest {
            file_io: table.file_io().clone(),
            schema: schema.clone(),
            input_file_scan_tasks: plan.files_to_compact.clone(),
            execution_config: Arc::new(self.config.execution.clone()),
            dir_path: default_location_generator.dir_path,
            partition_spec: table.metadata().default_partition_spec().clone(),
            metrics_recorder: Some(metrics_recorder),
            runtime_config: plan.runtime_config.clone(),
        })
    }

    /// Hook for customizing the commit strategy
    /// Default implementation commits all files at once, but can be customized for batch commits
    async fn commit_compaction_results(
        &self,
        table: &Table,
        output_data_files: Vec<DataFile>,
        input_file_scan_tasks: &InputFileScanTasks,
        snapshot: &Arc<Snapshot>,
        file_io: &FileIO,
    ) -> Result<Table> {
        let consistency_params = CommitConsistencyParams {
            starting_snapshot_id: table.metadata().current_snapshot_id().unwrap(),
            use_starting_sequence_number: true,
            basic_schema_id: table.metadata().current_schema().schema_id(),
        };

        let commit_manager = RewriteDataFilesCommitManager::new(
            self.commit_retry_config.clone(),
            self.catalog.clone(),
            self.table_ident.clone(),
            self.catalog_name.clone(),
            self.metrics.clone(),
            consistency_params,
        );

        let (all_data_files, all_delete_files) =
            get_all_files_from_snapshot(snapshot, file_io, table.metadata()).await?;

        // Collect file paths from all input scan tasks
        let input_file_paths: std::collections::HashSet<&str> = input_file_scan_tasks
            .data_files
            .iter()
            .chain(&input_file_scan_tasks.position_delete_files)
            .chain(&input_file_scan_tasks.equality_delete_files)
            .map(|task| task.data_file_path())
            .collect();

        // Filter all files to only include those from input scan tasks
        let input_files: Vec<DataFile> = all_data_files
            .into_iter()
            .chain(all_delete_files.into_iter())
            .filter(|file| input_file_paths.contains(file.file_path()))
            .collect();

        commit_manager
            .rewrite_files(output_data_files, input_files)
            .await
    }

    pub async fn compact_with_plan(
        &self,
        plan: CompactionPlan,
        execution_config: &CompactionExecutionConfig,
    ) -> Result<RewriteFilesStat> {
        let CompactionResult {
            stats,
            compaction_validator,
        } = match self.compaction_type {
            CompactionType::Full | CompactionType::MergeSmallDataFiles => {
                // Use the generic implementation for simple compaction types
                self.execute_standard_compaction(plan, execution_config)
                    .await?
            }
        };

        // validate
        if let Some(mut compaction_validator) = compaction_validator {
            compaction_validator.validate().await?;

            // Todo: log the successful validation with more context
            tracing::info!(
                "Compaction validation completed successfully for table '{}'",
                self.table_ident
            );
        }

        Ok(stats)
    }
}

async fn get_all_files_from_snapshot(
    snapshot: &Arc<Snapshot>,
    file_io: &FileIO,
    table_metadata: &iceberg::spec::TableMetadata,
) -> Result<(Vec<DataFile>, Vec<DataFile>)> {
    let manifest_list = snapshot
        .load_manifest_list(file_io, table_metadata)
        .await
        .unwrap();

    let mut data_file = vec![];
    let mut delete_file = vec![];
    for manifest_file in manifest_list.entries() {
        let a = manifest_file.load_manifest(file_io).await.unwrap();
        let (entry, _) = a.into_parts();
        for i in entry {
            match i.content_type() {
                iceberg::spec::DataContentType::Data => {
                    data_file.push(i.data_file().clone());
                }
                iceberg::spec::DataContentType::EqualityDeletes => {
                    delete_file.push(i.data_file().clone());
                }
                iceberg::spec::DataContentType::PositionDeletes => {
                    delete_file.push(i.data_file().clone());
                }
            }
        }
    }
    Ok((data_file, delete_file))
}

/// Configuration for the commit manager, including retry strategies.
#[derive(Debug, Clone)]
pub struct RewriteDataFilesCommitManagerRetryConfig {
    pub max_retries: u32, // This can be used to configure the backon strategy
    pub retry_initial_delay: Duration, // For exponential backoff
    pub retry_max_delay: Duration, // For exponential backoff
}

impl Default for RewriteDataFilesCommitManagerRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_initial_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

// Manages the commit process with retries
pub struct RewriteDataFilesCommitManager {
    config: RewriteDataFilesCommitManagerRetryConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    starting_snapshot_id: i64, // The snapshot ID to start from, used for consistency
    use_starting_sequence_number: bool, // Whether to use the starting sequence number for commits

    metrics_recorder: CompactionMetricsRecorder, // Metrics recorder for tracking commit operations

    basic_schema_id: i32, // Schema ID for the table, used for validation
}

pub struct CommitConsistencyParams {
    pub starting_snapshot_id: i64,
    pub use_starting_sequence_number: bool,
    pub basic_schema_id: i32,
}

/// Manages the commit process with retries
impl RewriteDataFilesCommitManager {
    pub fn new(
        config: RewriteDataFilesCommitManagerRetryConfig,
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        catalog_name: String,
        metrics: Arc<Metrics>,
        consistency_params: CommitConsistencyParams,
    ) -> Self {
        let metrics_recorder =
            CompactionMetricsRecorder::new(metrics, catalog_name, table_ident.to_string());

        Self {
            config,
            catalog,
            table_ident,
            starting_snapshot_id: consistency_params.starting_snapshot_id,
            use_starting_sequence_number: consistency_params.use_starting_sequence_number,
            metrics_recorder,
            basic_schema_id: consistency_params.basic_schema_id,
        }
    }

    /// Rewrites files in the table, handling retries and errors.
    pub async fn rewrite_files(
        &self,
        data_files: impl IntoIterator<Item = DataFile>,
        delete_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Table> {
        let data_files: Vec<DataFile> = data_files.into_iter().collect();
        let delete_files: Vec<DataFile> = delete_files.into_iter().collect();

        let operation = || {
            let catalog = self.catalog.clone();
            let table_ident = self.table_ident.clone();
            let data_files = data_files.clone();
            let delete_files = delete_files.clone();
            let use_starting_sequence_number = self.use_starting_sequence_number;
            let starting_snapshot_id = self.starting_snapshot_id;
            let metrics_recorder = self.metrics_recorder.clone();

            async move {
                // reload the table to get the latest state
                let table = catalog.load_table(&table_ident).await?;

                let schema_id = table.metadata().current_schema().schema_id();
                if schema_id != self.basic_schema_id {
                    return Err(iceberg::Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Schema ID mismatch: expected {}, found {}",
                            self.basic_schema_id, schema_id
                        ),
                    ));
                }

                let txn = Transaction::new(&table);

                // TODO: support validation of data files and delete files with starting snapshot before applying the rewrite
                let rewrite_action = if use_starting_sequence_number {
                    // TODO: avoid retry if the snapshot_id is not found
                    if let Some(snapshot) = table.metadata().snapshot_by_id(starting_snapshot_id) {
                        txn.rewrite_files(None, vec![])?
                            .add_data_files(data_files)?
                            .delete_files(delete_files)?
                            .new_data_file_sequence_number(snapshot.sequence_number())?
                    } else {
                        return Err(iceberg::Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "No snapshot found with the given snapshot_id {starting_snapshot_id}"
                            ),
                        ));
                    }
                } else {
                    txn.rewrite_files(None, vec![])?
                        .add_data_files(data_files)?
                        .delete_files(delete_files)?
                };

                let txn = rewrite_action.apply().await?;
                match txn.commit(catalog.as_ref()).await {
                    Ok(table) => {
                        // Update metrics after a successful commit
                        metrics_recorder.record_commit_success();
                        Ok(table)
                    }
                    Err(commit_err) => {
                        metrics_recorder.record_commit_failure();

                        tracing::error!(
                            "Commit attempt failed for table '{}': {:?}. Will retry if applicable.",
                            table_ident,
                            commit_err
                        );
                        Err(commit_err)
                    }
                }
            }
        };

        let retry_strategy = ExponentialBuilder::default()
            .with_min_delay(self.config.retry_initial_delay)
            .with_max_delay(self.config.retry_max_delay)
            .with_max_times(self.config.max_retries as usize);

        operation
            .retry(retry_strategy)
            .when(|e| {
                matches!(e.kind(), iceberg::ErrorKind::DataInvalid)
                    || matches!(e.kind(), iceberg::ErrorKind::Unexpected)
            })
            .notify(|e, d| {
                // Notify the user about the error
                // TODO: add metrics
                tracing::info!("Retrying Compaction failed {:?} after {:?}", e, d);
            })
            .await
            .map_err(|e: iceberg::Error| CompactionError::from(e)) // Convert backon::Error to your CompactionError
    }
}

pub struct CompactionPlan {
    pub files_to_compact: InputFileScanTasks,
    pub runtime_config: RuntimeConfig,
}

impl CompactionPlan {
    pub fn new(files_to_compact: InputFileScanTasks, runtime_config: RuntimeConfig) -> Self {
        Self {
            files_to_compact,
            runtime_config,
        }
    }

    pub fn dummy() -> Self {
        Self {
            files_to_compact: InputFileScanTasks::default(),
            runtime_config: RuntimeConfig::default(),
        }
    }

    /// Get the total number of files to be compacted
    pub fn file_count(&self) -> usize {
        self.files_to_compact.input_files_count()
    }

    /// Get the total size in bytes of files to be compacted
    pub fn total_bytes(&self) -> u64 {
        self.files_to_compact.input_total_bytes()
    }

    /// Get the recommended executor parallelism
    pub fn recommended_executor_parallelism(&self) -> usize {
        self.runtime_config.executor_parallelism
    }

    /// Get the recommended output parallelism
    pub fn recommended_output_parallelism(&self) -> usize {
        self.runtime_config.output_parallelism
    }

    /// Check if there are any files to compact
    pub fn is_empty(&self) -> bool {
        self.file_count() == 0
    }
}

pub struct DefaultParallelismCalculator;

impl DefaultParallelismCalculator {
    fn calculate_parallelism(
        &self,
        files_to_compact: &InputFileScanTasks,
        config: &CompactionPlanningConfig,
    ) -> Result<(usize, usize)> {
        let total_file_size_for_partitioning = files_to_compact.input_total_bytes();
        if total_file_size_for_partitioning == 0 {
            // If the total data file size is 0, we cannot partition by size.
            // This means there are no data files to compact.
            return Err(CompactionError::Execution(
                "No files to calculate_task_parallelism".to_owned(),
            ));
        }

        let partition_by_size = total_file_size_for_partitioning
            .div_ceil(config.min_size_per_partition)
            .max(1) as usize; // Ensure at least one partition.

        let total_files_count_for_partitioning = files_to_compact.input_files_count();

        let partition_by_count = total_files_count_for_partitioning
            .div_ceil(config.max_file_count_per_partition)
            .max(1); // Ensure at least one partition.

        let input_parallelism = partition_by_size
            .max(partition_by_count)
            .min(config.max_parallelism);

        // `output_parallelism` should not exceed `input_parallelism`
        // and should also not exceed max_parallelism.
        // It's primarily driven by size to avoid small output files.
        let mut output_parallelism = partition_by_size
            .min(input_parallelism)
            .min(config.max_parallelism);

        // Heuristic: If the total task data size is very small (less than target_file_size_bytes),
        // force output_parallelism to 1 to encourage merging into a single, larger output file.
        if config.enable_heuristic_output_parallelism {
            let total_data_file_size = files_to_compact
                .data_files
                .iter()
                .map(|f| f.file_size_in_bytes)
                .sum::<u64>();

            if total_data_file_size > 0 // Only apply if there's data
            && total_data_file_size < config.base.target_file_size
            && output_parallelism > 1
            {
                output_parallelism = 1;
            }
        }

        Ok((input_parallelism, output_parallelism))
    }
}

pub struct CompactionPlanner {
    config: CompactionPlanningConfig,
}

impl CompactionPlanner {
    pub fn new(config: CompactionPlanningConfig) -> Self {
        Self { config }
    }

    /// Plan a compaction based on the provided table and compaction type
    pub async fn plan_compaction(
        &self,
        table: &Table,
        compaction_type: CompactionType,
    ) -> Result<CompactionPlan> {
        // check if the current snapshot exists
        if table.metadata().current_snapshot().is_none() {
            return Ok(CompactionPlan::dummy());
        }

        let current_snapshot = table.metadata().current_snapshot().unwrap();

        // Step 1: Select files for compaction (extensible)
        let input_file_scan_tasks = self
            .select_files_for_compaction(table, current_snapshot.snapshot_id(), compaction_type)
            .await?;

        let (executor_parallelism, output_parallelism) = DefaultParallelismCalculator
            .calculate_parallelism(&input_file_scan_tasks, &self.config)
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        let runtime_config = RuntimeConfigBuilder::default()
            .executor_parallelism(executor_parallelism)
            .output_parallelism(output_parallelism)
            .build()
            .map_err(|e| CompactionError::Config(e.to_string()))?;

        Ok(CompactionPlan::new(input_file_scan_tasks, runtime_config))
    }

    // Template method pattern: These methods can be overridden for specific compaction types

    /// Hook for customizing file selection logic beyond simple `FileStrategy`
    /// Default implementation uses `FileStrategy`, but complex compaction types can override
    async fn select_files_for_compaction(
        &self,
        table: &Table,
        snapshot_id: i64,
        compaction_type: CompactionType,
    ) -> Result<InputFileScanTasks> {
        use crate::file_selection::FileStrategyFactory;

        let strategy = FileStrategyFactory::create_files_strategy(compaction_type, &self.config);
        FileSelector::get_scan_tasks_with_strategy(table, snapshot_id, strategy).await
    }
}

#[cfg(test)]
mod tests {
    use crate::compaction::{CompactionBuilder, CompactionPlanner};
    use crate::config::{
        CompactionConfigBuilder, CompactionExecutionConfigBuilder, CompactionPlanningConfigBuilder,
    };
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use iceberg::arrow::schema_to_arrow_schema;
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::table::Table;
    use iceberg::transaction::Transaction;
    use iceberg::writer::base_writer::equality_delete_writer::{
        EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
    };
    use iceberg::writer::base_writer::sort_position_delete_writer::{
        SortPositionDeleteWriterBuilder, POSITION_DELETE_SCHEMA,
    };
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::function_writer::equality_delta_writer::{
        EqualityDeltaWriterBuilder, DELETE_OP, INSERT_OP,
    };
    use iceberg::writer::{
        base_writer::data_file_writer::DataFileWriterBuilder, IcebergWriter, IcebergWriterBuilder,
    };
    use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
    use iceberg_catalog_memory::MemoryCatalog;
    use itertools::Itertools;
    use parquet::file::properties::WriterProperties;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;
    use uuid::Uuid;

    async fn create_namespace<C: Catalog>(catalog: &C, namespace_ident: &NamespaceIdent) {
        let _ = catalog
            .create_namespace(namespace_ident, HashMap::new())
            .await
            .unwrap();
    }

    fn simple_table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    fn simple_table_schema_with_pos() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "pos", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap()
    }

    async fn create_table<C: Catalog>(catalog: &C, table_ident: &TableIdent) {
        let _ = catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(simple_table_schema())
                    .build(),
            )
            .await
            .unwrap();
    }

    fn create_test_record_batch_with_pos(iceberg_schema: &Schema, insert: bool) -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let op = if insert { INSERT_OP } else { DELETE_OP };
        let pos_array = Int32Array::from(vec![op, op, op]);

        // Convert iceberg schema to arrow schema to ensure field ID consistency
        let arrow_schema = schema_to_arrow_schema(iceberg_schema).unwrap();

        RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(pos_array),
            ],
        )
        .unwrap()
    }

    fn create_test_record_batch(iceberg_schema: &Schema) -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        // Convert iceberg schema to arrow schema to ensure field ID consistency
        let arrow_schema = schema_to_arrow_schema(iceberg_schema).unwrap();

        RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    async fn build_equality_delta_writer(
        table: &Table,
        warehouse_location: String,
        unique_column_ids: Vec<i32>,
    ) -> impl IcebergWriter {
        let table_schema = table.metadata().current_schema();

        // Set up writer
        let location_generator = DefaultLocationGenerator {
            dir_path: warehouse_location,
        };

        let file_name_generator = DefaultFileNameGenerator::new(
            "data".to_owned(),
            Some("test".to_owned()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            table_schema.clone(),
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        let data_file_builder = DataFileWriterBuilder::new(
            parquet_writer_builder,
            None,
            table.metadata().default_partition_spec().spec_id(),
        );

        let config = EqualityDeleteWriterConfig::new(
            unique_column_ids.clone(),
            table_schema.clone(),
            None,
            0,
        )
        .unwrap();

        let unique_uuid_suffix = Uuid::now_v7();

        let equality_delete_fields = unique_column_ids
            .iter()
            .map(|id| table_schema.field_by_id(*id).unwrap().clone())
            .collect_vec();

        let equality_delete_builder = EqualityDeleteFileWriterBuilder::new(
            ParquetWriterBuilder::new(
                WriterProperties::new(),
                Arc::new(
                    Schema::builder()
                        .with_fields(equality_delete_fields)
                        .build()
                        .unwrap(),
                ),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone()).unwrap(),
                DefaultFileNameGenerator::new(
                    "123".to_owned(),
                    Some(format!("eq-del-{unique_uuid_suffix}")),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            ),
            config,
        );

        let position_delete_builder = SortPositionDeleteWriterBuilder::new(
            ParquetWriterBuilder::new(
                WriterProperties::new(),
                POSITION_DELETE_SCHEMA.clone(),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone()).unwrap(),
                DefaultFileNameGenerator::new(
                    "123".to_owned(),
                    Some(format!("pos-del-{unique_uuid_suffix}")),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            ),
            1024 * 1024, // 1MB
            None,
            None,
        );

        let delta_builder = EqualityDeltaWriterBuilder::new(
            data_file_builder,
            position_delete_builder,
            equality_delete_builder,
            unique_column_ids,
        );

        delta_builder.build().await.unwrap()
    }

    async fn build_simple_data_writer(
        table: &Table,
        warehouse_location: String,
        file_name_suffix: &str,
    ) -> impl IcebergWriter {
        let table_schema = table.metadata().current_schema();

        // Set up writer
        let location_generator = DefaultLocationGenerator {
            dir_path: warehouse_location,
        };

        let file_name_generator = DefaultFileNameGenerator::new(
            "data".to_owned(),
            Some(file_name_suffix.to_owned()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            table_schema.clone(),
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        let data_file_builder = DataFileWriterBuilder::new(
            parquet_writer_builder,
            None,
            table.metadata().default_partition_spec().spec_id(),
        );

        data_file_builder.build().await.unwrap()
    }

    #[tokio::test]
    async fn test_write_commit_and_compaction() {
        // Create a temporary directory for the warehouse location
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        // Create a memory catalog with the file IO and warehouse location
        let catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        // Load the table
        let table = catalog.load_table(&table_ident).await.unwrap();

        let unique_column_ids = vec![1];
        let mut writer =
            build_equality_delta_writer(&table, warehouse_location.clone(), unique_column_ids)
                .await;

        let insert_batch = create_test_record_batch_with_pos(&simple_table_schema_with_pos(), true);

        let delete_batch =
            create_test_record_batch_with_pos(&simple_table_schema_with_pos(), false);

        // Write data (insert): generate data files
        writer.write(insert_batch.clone()).await.unwrap();

        // Write data (delete) generate position delete files
        writer.write(delete_batch).await.unwrap();

        // Write data (insert) generate data files
        writer.write(insert_batch).await.unwrap();

        let data_files = writer.close().await.unwrap();

        // Start transaction and commit
        let transaction = Transaction::new(&table);
        let mut append_action = transaction.fast_append(None, None, vec![]).unwrap();
        append_action.add_data_files(data_files).unwrap();
        let tx = append_action.apply().await.unwrap();

        // Commit the transaction
        let updated_table = tx.commit(&catalog).await.unwrap();

        // Verify the snapshot was created
        let snapshots = updated_table.metadata().snapshots();
        assert!(snapshots.len() > 0, "Should have at least one snapshot");

        let latest_snapshot = updated_table.metadata().current_snapshot().unwrap();

        // Verify we can load the table again and see the data
        let reloaded_table = catalog.load_table(&table_ident).await.unwrap();
        let current_snapshot = reloaded_table.metadata().current_snapshot().unwrap();
        assert_eq!(
            current_snapshot.snapshot_id(),
            latest_snapshot.snapshot_id()
        );

        let execution_config = CompactionExecutionConfigBuilder::default()
            .enable_validate_compaction(true)
            .build()
            .unwrap();

        let rewrite_files_stat = CompactionBuilder::new()
            .with_catalog(Arc::new(catalog))
            .with_table_ident(table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default()
                    .execution(execution_config)
                    .build()
                    .unwrap(),
            ))
            .build()
            .await
            .unwrap()
            .compact()
            .await
            .unwrap();

        assert_eq!(rewrite_files_stat.input_files_count, 2);
    }

    #[tokio::test]
    async fn test_full_compaction() {
        // Create a temporary directory for the warehouse location
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        // Create a memory catalog with the file IO and warehouse location
        let catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        // Load the table
        let table = catalog.load_table(&table_ident).await.unwrap();

        let unique_column_ids = vec![1];
        let mut writer =
            build_equality_delta_writer(&table, warehouse_location.clone(), unique_column_ids)
                .await;

        let insert_batch = create_test_record_batch_with_pos(&simple_table_schema_with_pos(), true);

        // Write multiple batches to create multiple files
        writer.write(insert_batch.clone()).await.unwrap();
        writer.write(insert_batch.clone()).await.unwrap();
        writer.write(insert_batch).await.unwrap();

        let data_files = writer.close().await.unwrap();
        let initial_file_count = data_files.len();

        // Start transaction and commit
        let transaction = Transaction::new(&table);
        let mut append_action = transaction.fast_append(None, None, vec![]).unwrap();
        append_action.add_data_files(data_files).unwrap();
        let tx = append_action.apply().await.unwrap();

        // Commit the transaction
        let _updated_table = tx.commit(&catalog).await.unwrap();

        // Test full compaction - should compact all files
        let rewrite_files_stat = CompactionBuilder::new()
            .with_catalog(Arc::new(catalog))
            .with_table_ident(table_ident.clone())
            .with_compaction_type(super::CompactionType::Full)
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .build()
            .await
            .unwrap()
            .compact()
            .await
            .unwrap();

        // Full compaction should rewrite all existing files
        assert_eq!(rewrite_files_stat.input_files_count, initial_file_count);
        // Should create at least 1 new file from all the data (might be more depending on size)
        assert!(rewrite_files_stat.output_files_count >= 1);
    }

    #[tokio::test]
    async fn test_small_files_compaction_with_validation() {
        // Create a temporary directory for the warehouse location
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        // Create a memory catalog with the file IO and warehouse location
        let catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        // Load the table
        let table = catalog.load_table(&table_ident).await.unwrap();

        // Create multiple small data files
        let mut small_writer1 =
            build_simple_data_writer(&table, warehouse_location.clone(), "small1").await;
        let batch = create_test_record_batch(&simple_table_schema());
        small_writer1.write(batch.clone()).await.unwrap();
        let small_files1 = small_writer1.close().await.unwrap();

        let mut small_writer2 =
            build_simple_data_writer(&table, warehouse_location.clone(), "small2").await;
        small_writer2.write(batch.clone()).await.unwrap();
        let small_files2 = small_writer2.close().await.unwrap();

        // Create a larger file by writing multiple batches
        let mut large_writer =
            build_simple_data_writer(&table, warehouse_location.clone(), "large").await;
        // Write multiple batches to make it larger
        for _ in 0..10 {
            large_writer.write(batch.clone()).await.unwrap();
        }
        let large_files = large_writer.close().await.unwrap();

        // Commit all files
        let mut all_data_files = Vec::new();
        all_data_files.extend(small_files1);
        all_data_files.extend(small_files2);
        all_data_files.extend(large_files);

        let transaction = Transaction::new(&table);
        let mut append_action = transaction.fast_append(None, None, vec![]).unwrap();
        append_action.add_data_files(all_data_files).unwrap();
        let tx = append_action.apply().await.unwrap();

        // Commit the transaction
        let updated_table = tx.commit(&catalog).await.unwrap();

        // Get files before compaction for validation
        let snapshot_before = updated_table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot_before
            .load_manifest_list(updated_table.file_io(), updated_table.metadata())
            .await
            .unwrap();

        let mut data_files_before = Vec::new();
        for manifest in manifest_list.entries() {
            let manifest_file = manifest
                .load_manifest(updated_table.file_io())
                .await
                .unwrap();

            for entry in manifest_file.entries() {
                if entry.is_alive() {
                    data_files_before.push(entry.data_file().clone());
                }
            }
        }

        // Test small files compaction with a threshold that should only select small files
        let small_file_threshold = 10_000; // 10KB threshold - should only compact really small files
        let catalog_arc = Arc::new(catalog);

        let compaction_config = CompactionConfigBuilder::default()
            .planning(
                CompactionPlanningConfigBuilder::default()
                    .small_file_threshold(small_file_threshold)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let compaction = CompactionBuilder::new()
            .with_catalog(catalog_arc.clone())
            .with_table_ident(table_ident.clone())
            .with_compaction_type(super::CompactionType::MergeSmallDataFiles)
            .with_config(Arc::new(compaction_config))
            .build()
            .await
            .unwrap();

        let planner = CompactionPlanner::new(compaction.config.planning.clone());

        // Get the files that would be selected for compaction
        let files_to_compact = planner
            .select_files_for_compaction(
                &updated_table,
                snapshot_before.snapshot_id(),
                super::CompactionType::MergeSmallDataFiles,
            )
            .await
            .unwrap();

        // Validate file selection logic
        let selected_file_paths: std::collections::HashSet<&str> = files_to_compact
            .data_files
            .iter()
            .map(|task| task.data_file_path())
            .collect();

        let small_files_count = data_files_before
            .iter()
            .filter(|file| file.file_size_in_bytes() < small_file_threshold)
            .count();

        let large_files_count = data_files_before
            .iter()
            .filter(|file| file.file_size_in_bytes() >= small_file_threshold)
            .count();

        // Verify that only small files are selected
        for data_file in &data_files_before {
            if data_file.file_size_in_bytes() < small_file_threshold {
                assert!(
                    selected_file_paths.contains(data_file.file_path()),
                    "Small file {} (size: {}) should be selected for compaction",
                    data_file.file_path(),
                    data_file.file_size_in_bytes()
                );
            } else {
                assert!(
                    !selected_file_paths.contains(data_file.file_path()),
                    "Large file {} (size: {}) should NOT be selected for compaction",
                    data_file.file_path(),
                    data_file.file_size_in_bytes()
                );
            }
        }

        // Ensure we have small files to test with
        assert!(
            small_files_count > 0,
            "Test setup should create small files to compact"
        );

        // Run the actual compaction
        let rewrite_files_stat = compaction.compact().await.unwrap();

        // Validate compaction results
        assert_eq!(rewrite_files_stat.input_files_count, small_files_count,);

        // Should create fewer files than were compacted (compaction benefit)
        assert!(rewrite_files_stat.output_files_count <= small_files_count);
        assert!(rewrite_files_stat.output_files_count > 0);

        // Verify final state: total files should be reduced
        let final_table = catalog_arc.load_table(&table_ident).await.unwrap();
        let final_snapshot = final_table.metadata().current_snapshot().unwrap();
        let final_manifest_list = final_snapshot
            .load_manifest_list(final_table.file_io(), final_table.metadata())
            .await
            .unwrap();

        let mut final_data_files = Vec::new();
        for manifest in final_manifest_list.entries() {
            let manifest_file = manifest.load_manifest(final_table.file_io()).await.unwrap();

            for entry in manifest_file.entries() {
                if entry.is_alive() {
                    final_data_files.push(entry.data_file().clone());
                }
            }
        }

        // Final file count should be: large_files + newly_created_files
        let expected_final_count =
            large_files_count + rewrite_files_stat.output_files_count as usize;
        assert_eq!(final_data_files.len(), expected_final_count);

        // Verify that large files are still present and untouched
        let final_file_paths: std::collections::HashSet<&str> = final_data_files
            .iter()
            .map(|file| file.file_path())
            .collect();

        for data_file in &data_files_before {
            if data_file.file_size_in_bytes() >= small_file_threshold {
                assert!(
                    final_file_paths.contains(data_file.file_path()),
                    "Large file {} should still be present after compaction",
                    data_file.file_path()
                );
            }
        }
    }

    /// Test the plan_compaction functionality separately
    #[tokio::test]
    async fn test_plan_compaction() {
        // Create a temporary directory for the warehouse location
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        let table = catalog.load_table(&table_ident).await.unwrap();

        let planner =
            CompactionPlanner::new(CompactionPlanningConfigBuilder::default().build().unwrap());

        // Test empty table
        let plan = planner
            .plan_compaction(&table, super::CompactionType::Full)
            .await
            .unwrap();

        assert!(plan.is_empty());
        assert_eq!(plan.file_count(), 0);
        assert_eq!(plan.total_bytes(), 0);

        // Create some data files
        let mut writer = build_simple_data_writer(&table, warehouse_location.clone(), "test").await;
        let batch = create_test_record_batch(&simple_table_schema());
        writer.write(batch).await.unwrap();
        let data_files = writer.close().await.unwrap();

        // Commit the files
        let transaction = Transaction::new(&table);
        let mut append_action = transaction.fast_append(None, None, vec![]).unwrap();
        append_action.add_data_files(data_files).unwrap();
        let tx = append_action.apply().await.unwrap();
        let updated_table = tx.commit(&catalog).await.unwrap();

        let planner =
            CompactionPlanner::new(CompactionPlanningConfigBuilder::default().build().unwrap());

        // Test plan with data
        let plan = planner
            .plan_compaction(&updated_table, super::CompactionType::Full)
            .await
            .unwrap();

        assert!(!plan.is_empty());
        assert!(plan.file_count() > 0);
        assert!(plan.total_bytes() > 0);
        assert!(plan.recommended_executor_parallelism() > 0);
        assert!(plan.recommended_output_parallelism() > 0);
    }

    /// Test the compact_with_plan functionality separately
    #[tokio::test]
    async fn test_compact_with_plan() {
        // Create test data
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        let table = catalog.load_table(&table_ident).await.unwrap();

        // Create some data files
        let mut writer = build_simple_data_writer(&table, warehouse_location.clone(), "test").await;
        let batch = create_test_record_batch(&simple_table_schema());
        writer.write(batch).await.unwrap();
        let data_files = writer.close().await.unwrap();

        // Commit the files
        let transaction = Transaction::new(&table);
        let mut append_action = transaction.fast_append(None, None, vec![]).unwrap();
        append_action.add_data_files(data_files).unwrap();
        let tx = append_action.apply().await.unwrap();
        let updated_table = tx.commit(&catalog).await.unwrap();

        // Create compaction instance
        let compaction = CompactionBuilder::new()
            .with_catalog(Arc::new(catalog))
            .with_table_ident(table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .build()
            .await
            .unwrap();

        let planner = CompactionPlanner::new(compaction.config.planning.clone());

        // Test planning separately
        let plan = planner
            .plan_compaction(&updated_table, super::CompactionType::Full)
            .await
            .unwrap();

        assert!(!plan.is_empty());

        // Test execution with the plan
        let stats = compaction
            .compact_with_plan(plan, &compaction.config.execution)
            .await
            .unwrap();

        assert!(stats.input_files_count > 0);
        assert!(stats.output_files_count > 0);
    }
}
