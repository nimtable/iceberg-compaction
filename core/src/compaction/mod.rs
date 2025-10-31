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
use iceberg::spec::{DataFile, Snapshot, MAIN_BRANCH, UNASSIGNED_SNAPSHOT_ID};
use iceberg::{Catalog, ErrorKind, TableIdent};
use mixtrics::metrics::BoxedRegistry;
use mixtrics::registry::noop::NoopMetricsRegistry;

use crate::common::{CompactionMetricsRecorder, Metrics};
use crate::compaction::validator::CompactionValidator;
use crate::config::{CompactionExecutionConfig, CompactionPlanningConfig};
use crate::executor::{
    create_compaction_executor, ExecutorType, RewriteFilesRequest, RewriteFilesResponse,
    RewriteFilesStat,
};
use crate::file_selection::{FileGroup, FileSelector};
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
use std::borrow::Cow;

mod validator;

/// Validates that all rewrite results target the same snapshot and branch.
///
/// # Errors
///
/// Returns `CompactionError::InvalidInput` if any result has mismatched `to_branch` or `snapshot_id`.
fn validate_rewrite_results_consistency(
    rewrite_results: &[RewriteResult],
    expected_snapshot_id: i64,
    expected_branch: &str,
) -> Result<()> {
    for result in rewrite_results {
        if result.plan.to_branch != expected_branch {
            return Err(CompactionError::Execution(format!(
                "Compaction plan branch '{}' does not match configured branch '{}'",
                result.plan.to_branch, expected_branch
            )));
        }

        if result.plan.snapshot_id != expected_snapshot_id {
            return Err(CompactionError::Execution(format!(
                "Compaction plan snapshot '{}' does not match other plans snapshot '{}'",
                result.plan.snapshot_id, expected_snapshot_id
            )));
        }
    }
    Ok(())
}

/// Builder for `Compaction` with optional configuration.
///
/// # Examples
///
/// ```ignore
/// let compaction = CompactionBuilder::new(catalog, table_ident)
///     .with_config(config)
///     .with_executor_type(ExecutorType::DataFusion)
///     .build();
/// ```
pub struct CompactionBuilder {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,

    catalog_name: Option<Cow<'static, str>>,
    config: Option<Arc<CompactionConfig>>,
    executor_type: Option<ExecutorType>,
    registry: Option<BoxedRegistry>,
    commit_retry_config: Option<CommitManagerRetryConfig>,
    to_branch: Option<Cow<'static, str>>,
}

impl CompactionBuilder {
    /// Creates a new builder with required catalog and table identifier.
    pub fn new(catalog: Arc<dyn Catalog>, table_ident: TableIdent) -> Self {
        Self {
            catalog,
            table_ident,

            catalog_name: None,
            config: None,
            executor_type: None,
            registry: None,
            commit_retry_config: None,
            to_branch: None,
        }
    }

    /// Sets the compaction configuration.
    pub fn with_config(mut self, config: Arc<CompactionConfig>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the executor type. Defaults to `ExecutorType::DataFusion`.
    pub fn with_executor_type(mut self, executor_type: ExecutorType) -> Self {
        self.executor_type = Some(executor_type);
        self
    }

    /// Sets the catalog name for metrics labels.
    pub fn with_catalog_name(mut self, catalog_name: impl Into<Cow<'static, str>>) -> Self {
        self.catalog_name = Some(catalog_name.into());
        self
    }

    /// Sets the metrics registry. Defaults to `NoopMetricsRegistry`.
    pub fn with_registry(mut self, registry: BoxedRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Sets commit retry configuration for transient failures.
    pub fn with_retry_config(mut self, retry_config: CommitManagerRetryConfig) -> Self {
        self.commit_retry_config = Some(retry_config);
        self
    }

    /// Sets the target branch for compaction commits. Defaults to `main`.
    pub fn with_to_branch(mut self, to_branch: impl Into<Cow<'static, str>>) -> Self {
        self.to_branch = Some(to_branch.into());
        self
    }

    /// Builds the `Compaction` instance with configured values.
    pub fn build(self) -> Compaction {
        let executor_type = self.executor_type.unwrap_or(ExecutorType::DataFusion);
        let executor = create_compaction_executor(executor_type);

        let metrics = if let Some(registry) = self.registry {
            Arc::new(Metrics::new(registry))
        } else {
            Arc::new(Metrics::new(Box::new(NoopMetricsRegistry)))
        };

        let commit_retry_config = self.commit_retry_config.unwrap_or_default();

        let to_branch = self
            .to_branch
            .unwrap_or_else(|| MAIN_BRANCH.to_owned().into());

        let catalog_name = self
            .catalog_name
            .unwrap_or_else(|| "default".to_owned().into());

        let table_ident_name = Cow::Owned(self.table_ident.name().to_owned());

        Compaction {
            config: self.config,
            executor,
            catalog: self.catalog,
            metrics,
            table_ident: self.table_ident,
            table_ident_name,
            catalog_name,
            commit_retry_config,
            to_branch,
        }
    }
}

/// Iceberg table compaction orchestrator supporting managed and plan-driven workflows.
///
/// # Workflows
///
/// **Managed workflow**: [`compact()`](Self::compact) handles planning, execution, and commit atomically.
///
/// **Plan-driven workflow**: Caller controls each phase:
/// 1. [`plan_compaction()`](Self::plan_compaction) → generate plans
/// 2. [`rewrite_plan()`](Self::rewrite_plan) → execute rewrites
/// 3. [`commit_rewrite_results()`](Self::commit_rewrite_results) → commit transaction
///
/// # Fields
///
/// - `config`: Optional global config for managed workflow. Plan-driven workflow provides config per-plan.
pub struct Compaction {
    /// Optional global configuration for managed workflows
    pub config: Option<Arc<CompactionConfig>>,
    pub executor: Box<dyn CompactionExecutor>,
    pub catalog: Arc<dyn Catalog>,
    pub metrics: Arc<Metrics>,
    pub table_ident: TableIdent,
    pub table_ident_name: Cow<'static, str>,
    pub catalog_name: Cow<'static, str>,

    pub commit_retry_config: CommitManagerRetryConfig,
    pub to_branch: Cow<'static, str>,
}

/// Intermediate result from `rewrite_plan()` before commit.
#[derive(Debug, Clone)]
pub struct RewriteResult {
    pub output_data_files: Vec<DataFile>,
    pub stats: RewriteFilesStat,
    pub plan: CompactionPlan,
    /// Validation info for creating `CompactionValidator` later
    pub validation_info: Option<ValidationInfo>,
}

/// Information for deferred `CompactionValidator` creation.
#[derive(Debug, Clone)]
pub struct ValidationInfo {
    pub file_group: FileGroup,
    pub executor_parallelism: usize,
}

/// Result of a successful compaction containing rewritten files and metadata.
#[derive(Default)]
pub struct CompactionResult {
    /// Newly written data files from compaction
    pub data_files: Vec<DataFile>,
    /// Statistics about the compaction operation
    pub stats: RewriteFilesStat,
    /// Updated table metadata after commit (if available)
    pub table: Option<Table>,
}

impl Compaction {
    /// Runs managed compaction: planning, execution, commit, and optional validation.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(CompactionResult))` if files were compacted
    /// - `Ok(None)` if no files needed compaction
    /// - `Err(_)` if `config` is `None` or operation failed
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `self.config` is `None`
    /// - Planning, execution, commit, or validation fails
    pub async fn compact(&self) -> Result<Option<CompactionResult>> {
        if let Some(config) = &self.config {
            let overall_start_time = std::time::Instant::now();

            // 1. Get all compaction plans
            let plans = self.plan_compaction().await?;

            if plans.is_empty() {
                return Ok(None);
            }

            let table = self.catalog.load_table(&self.table_ident).await?;

            // 2. Concurrently execute rewrite for all plans
            let rewrite_results = self
                .concurrent_rewrite_plans(plans, &config.execution, &table)
                .await?;

            if rewrite_results.is_empty() {
                return Ok(None);
            }

            // 3. Commit all rewrite results in a single transaction
            let commit_start_time = std::time::Instant::now();
            let final_table = self.commit_rewrite_results(rewrite_results.clone()).await?;

            // 4. Run validations if enabled
            if config.execution.enable_validate_compaction {
                self.run_validations(rewrite_results.clone(), &final_table)
                    .await?;
            }

            // 6. Update metrics for the entire compaction operation
            self.record_overall_metrics(&rewrite_results, overall_start_time, commit_start_time);

            // 7. Merge results for response
            let merged_result =
                self.merge_rewrite_results_to_compaction_result(rewrite_results, Some(final_table));
            Ok(Some(merged_result))
        } else {
            Err(crate::error::CompactionError::Execution(
                "CompactionConfig is required".to_owned(),
            ))
        }
    }

    /// Records metrics for overall compaction duration and statistics.
    fn record_overall_metrics(
        &self,
        rewrite_results: &[RewriteResult],
        overall_start_time: std::time::Instant,
        commit_start_time: std::time::Instant,
    ) {
        let metrics_recorder = CompactionMetricsRecorder::new(
            self.metrics.clone(),
            self.catalog_name.clone(),
            self.table_ident_name.clone(),
        );

        // Record commit duration
        metrics_recorder.record_commit_duration(commit_start_time.elapsed().as_millis() as _);

        // Record total compaction duration
        metrics_recorder.record_compaction_duration(overall_start_time.elapsed().as_millis() as _);

        // Record plan-level metrics for each rewrite result
        for result in rewrite_results {
            metrics_recorder.record_plan_file_count(result.stats.input_files_count);
            metrics_recorder.record_plan_size_bytes(result.stats.input_total_bytes);
        }

        // Merge all stats and record completion
        let merged_stats = self.merge_rewrite_stats(rewrite_results);
        metrics_recorder.record_compaction_complete(&merged_stats);
    }

    /// Merges statistics from multiple rewrite results into a single aggregate.
    fn merge_rewrite_stats(&self, rewrite_results: &[RewriteResult]) -> RewriteFilesStat {
        let mut merged_stats = RewriteFilesStat::default();

        for result in rewrite_results {
            merged_stats.input_files_count += result.stats.input_files_count;
            merged_stats.output_files_count += result.stats.output_files_count;
            merged_stats.input_total_bytes += result.stats.input_total_bytes;
            merged_stats.output_total_bytes += result.stats.output_total_bytes;
            merged_stats.input_data_file_count += result.stats.input_data_file_count;
            merged_stats.input_position_delete_file_count +=
                result.stats.input_position_delete_file_count;
            merged_stats.input_equality_delete_file_count +=
                result.stats.input_equality_delete_file_count;
            merged_stats.input_data_file_total_bytes += result.stats.input_data_file_total_bytes;
            merged_stats.input_position_delete_file_total_bytes +=
                result.stats.input_position_delete_file_total_bytes;
            merged_stats.input_equality_delete_file_total_bytes +=
                result.stats.input_equality_delete_file_total_bytes;
        }

        merged_stats
    }

    /// Executes rewrite for a single plan without committing.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `plan.to_branch != self.to_branch`
    /// - Snapshot with `plan.snapshot_id` does not exist
    /// - Executor rewrite operation fails
    pub async fn rewrite_plan(
        &self,
        plan: CompactionPlan,
        execution_config: &CompactionExecutionConfig,
        table: &Table,
    ) -> Result<RewriteResult> {
        if plan.to_branch != *self.to_branch {
            return Err(CompactionError::Execution(format!(
                "Compaction plan branch '{}' does not match configured branch '{}'",
                plan.to_branch, self.to_branch
            )));
        }

        // Check if the current snapshot exists
        if let Some(_branch_snapshot) = table.metadata().snapshot_by_id(plan.snapshot_id) {
            let now = std::time::Instant::now();
            let metrics_recorder = CompactionMetricsRecorder::new(
                self.metrics.clone(),
                self.catalog_name.clone(),
                self.table_ident_name.clone(),
            );

            // Step 1: Create rewrite request
            let rewrite_files_request =
                self.create_rewrite_request(table, &plan.file_group, execution_config)?;

            // Step 2: Execute rewrite
            let RewriteFilesResponse {
                data_files: output_data_files,
                stats,
            } = match self.executor.rewrite_files(rewrite_files_request).await {
                Ok(response) => response,
                Err(e) => {
                    metrics_recorder.record_executor_error();
                    return Err(e);
                }
            };

            // Step 3: (Delayed) Input file collection moved to commit phase to avoid duplicate IO

            // Step 4: Setup validation info if enabled
            let validation_info = if execution_config.enable_validate_compaction {
                Some(ValidationInfo {
                    file_group: plan.file_group.clone(),
                    executor_parallelism: plan.file_group.executor_parallelism,
                })
            } else {
                None
            };

            // Step 5: Update metrics - record plan-level metrics
            metrics_recorder.record_plan_execution_duration(now.elapsed().as_millis() as _);
            metrics_recorder.record_plan_file_count(stats.input_files_count);
            metrics_recorder.record_plan_size_bytes(stats.input_total_bytes);

            Ok(RewriteResult {
                output_data_files,
                stats,
                plan,
                validation_info,
            })
        } else {
            Err(CompactionError::Execution(format!(
                "Snapshot {} not found",
                plan.snapshot_id
            )))
        }
    }

    /// Generates compaction plans without executing them.
    ///
    /// # Returns
    ///
    /// Vector of `CompactionPlan` based on `self.config.planning`.
    ///
    /// # Errors
    ///
    /// Returns error if `self.config` is `None` or planning fails.
    pub async fn plan_compaction(&self) -> Result<Vec<CompactionPlan>> {
        if let Some(config) = &self.config {
            let table = self.catalog.load_table(&self.table_ident).await?;
            let compaction_planner = CompactionPlanner::new(config.planning.clone());

            compaction_planner
                .plan_compaction_with_branch(&table, &self.to_branch)
                .await
        } else {
            Err(crate::error::CompactionError::Execution(
                "CompactionConfig is required for planning".to_owned(),
            ))
        }
    }

    /// Commits multiple rewrite results in a single Iceberg transaction.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `rewrite_results` is empty
    /// - Results have inconsistent `to_branch` or `snapshot_id`
    /// - Snapshot does not exist
    /// - Commit fails
    pub async fn commit_rewrite_results(
        &self,
        rewrite_results: Vec<RewriteResult>,
    ) -> Result<Table> {
        if rewrite_results.is_empty() {
            return Err(CompactionError::Execution(
                "No rewrite results to commit".to_owned(),
            ));
        }

        let table = self.catalog.load_table(&self.table_ident).await?;
        let snapshot_id = rewrite_results[0].plan.snapshot_id;

        // verify all rewrite results are from the same branch and snapshot
        validate_rewrite_results_consistency(&rewrite_results, snapshot_id, &self.to_branch)?;

        // Create commit manager and delegate the complex logic to it
        if let Some(snapshot) = table.metadata().snapshot_by_id(snapshot_id) {
            let consistency_params = CommitConsistencyParams {
                starting_snapshot_id: snapshot.snapshot_id(),
                use_starting_sequence_number: true,
                basic_schema_id: table.metadata().current_schema().schema_id(),
            };

            let commit_manager = CommitManager::new(
                self.commit_retry_config.clone(),
                self.catalog.clone(),
                self.table_ident.clone(),
                self.table_ident_name.clone(),
                self.catalog_name.clone(),
                self.metrics.clone(),
                consistency_params,
            );

            // Delegate to CommitManager's high-level interface
            commit_manager
                .rewrite_files_from_results(rewrite_results, &self.to_branch)
                .await
        } else {
            Err(CompactionError::Execution(format!(
                "Snapshot {} not found",
                snapshot_id
            )))
        }
    }

    /// Executes multiple plans concurrently using `futures::stream`.
    ///
    /// # Performance
    ///
    /// Uses buffered stream for concurrent execution.
    async fn concurrent_rewrite_plans(
        &self,
        plans: Vec<CompactionPlan>,
        execution_config: &CompactionExecutionConfig,
        table: &Table,
    ) -> Result<Vec<RewriteResult>> {
        use futures::stream::{self, StreamExt};

        let results: Result<Vec<RewriteResult>> = stream::iter(plans.into_iter())
            .map(|plan| async move { self.rewrite_plan(plan, execution_config, table).await })
            .buffer_unordered(execution_config.max_concurrent_compaction_plans) // Limit concurrency based on config
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect();

        results
    }

    /// Runs `CompactionValidator` for each result if validation info is present.
    async fn run_validations(
        &self,
        rewrite_results: Vec<RewriteResult>,
        committed_table: &Table,
    ) -> Result<()> {
        for rewrite_result in rewrite_results {
            if let Some(validation_info) = rewrite_result.validation_info {
                let mut validator = CompactionValidator::new(
                    validation_info.file_group,
                    rewrite_result.output_data_files,
                    validation_info.executor_parallelism,
                    committed_table.metadata().current_schema().clone(),
                    committed_table.metadata().current_schema().clone(),
                    committed_table.clone(),
                    self.catalog_name.clone(),
                    self.to_branch.clone(),
                )
                .await?;

                validator.validate().await?;
                tracing::info!(
                    "Compaction validation completed successfully for table '{}'",
                    self.table_ident
                );
            }
        }
        Ok(())
    }

    /// Merges multiple rewrite results into a single `CompactionResult`.
    fn merge_rewrite_results_to_compaction_result(
        &self,
        results: Vec<RewriteResult>,
        table: Option<Table>,
    ) -> CompactionResult {
        // Reuse the existing stats merger to avoid duplication
        let merged_stats = self.merge_rewrite_stats(&results);

        // Collect all output data files
        let mut merged_data_files = Vec::new();
        for result in results {
            merged_data_files.extend(result.output_data_files);
        }

        CompactionResult {
            data_files: merged_data_files,
            stats: merged_stats,
            table,
        }
    }

    /// Creates a `RewriteFilesRequest` for the executor.
    ///
    /// Default implementation creates standard request. Override for customization.
    fn create_rewrite_request(
        &self,
        table: &Table,
        file_group: &FileGroup,
        execution_config: &CompactionExecutionConfig,
    ) -> Result<RewriteFilesRequest> {
        let schema = table.metadata().current_schema();
        let default_location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let metrics_recorder = CompactionMetricsRecorder::new(
            self.metrics.clone(),
            self.catalog_name.clone(),
            self.table_ident_name.clone(),
        );

        Ok(RewriteFilesRequest {
            file_io: table.file_io().clone(),
            schema: schema.clone(),
            file_group: file_group.clone(),
            execution_config: Arc::new(execution_config.clone()),
            dir_path: default_location_generator.dir_path,
            partition_spec: table.metadata().default_partition_spec().clone(),
            metrics_recorder: Some(metrics_recorder),
        })
    }

    /// Compacts the table using a single provided plan.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(_))` if files were compacted
    /// - `Ok(None)` if plan has no files
    ///
    /// # Errors
    ///
    /// Returns error if rewrite, commit, or validation fails.
    pub async fn compact_with_plan(
        &self,
        plan: CompactionPlan,
        execution_config: &CompactionExecutionConfig,
    ) -> Result<Option<CompactionResult>> {
        // Check if there are files to compact
        if plan.file_count() == 0 {
            return Ok(None);
        }

        let overall_start_time = std::time::Instant::now();

        let table = self.catalog.load_table(&self.table_ident).await?;

        // Use the new rewrite_plan method
        let rewrite_result = self.rewrite_plan(plan, execution_config, &table).await?;

        // Commit the single rewrite result
        let commit_start_time = std::time::Instant::now();
        let final_table = self
            .commit_rewrite_results(vec![rewrite_result.clone()])
            .await?;

        // Run validation if enabled
        if execution_config.enable_validate_compaction {
            if let Some(validation_info) = &rewrite_result.validation_info {
                let mut validator = CompactionValidator::new(
                    validation_info.file_group.clone(),
                    rewrite_result.output_data_files.clone(),
                    validation_info.executor_parallelism,
                    final_table.metadata().current_schema().clone(),
                    final_table.metadata().current_schema().clone(),
                    final_table.clone(),
                    self.catalog_name.clone(),
                    self.to_branch.clone(),
                )
                .await?;

                validator.validate().await?;
                tracing::info!(
                    "Compaction validation completed successfully for table '{}'",
                    self.table_ident
                );
            }
        }

        // Record metrics for single plan compaction
        self.record_overall_metrics(
            &[rewrite_result.clone()],
            overall_start_time,
            commit_start_time,
        );

        // Convert to CompactionResult
        let result = CompactionResult {
            data_files: rewrite_result.output_data_files,
            stats: rewrite_result.stats,
            table: Some(final_table),
        };

        Ok(Some(result))
    }

    /// Returns the metrics registry for this compaction instance.
    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Builds a `CommitManager` with the given consistency parameters.
    pub fn build_commit_manager(
        &self,
        consistency_params: CommitConsistencyParams,
    ) -> CommitManager {
        CommitManager::new(
            self.commit_retry_config.clone(),
            self.catalog.clone(),
            self.table_ident.clone(),
            self.table_ident_name.clone(),
            self.catalog_name.clone(),
            self.metrics.clone(),
            consistency_params,
        )
    }
}

/// Loads all data and delete files from a snapshot.
///
/// # Errors
///
/// Returns error if manifest list or manifest loading fails.
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

/// Configuration for commit retry behavior with exponential backoff.
#[derive(Debug, Clone)]
pub struct CommitManagerRetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial delay before the first retry
    pub retry_initial_delay: Duration,
    /// Maximum delay between retries (for exponential backoff)
    pub retry_max_delay: Duration,
}

impl Default for CommitManagerRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_initial_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

/// Manages commit operations with retry logic and consistency validation.
///
/// Uses exponential backoff for transient failures (e.g., optimistic lock conflicts).
pub struct CommitManager {
    config: CommitManagerRetryConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    /// Snapshot ID for consistency checks during commit
    starting_snapshot_id: i64,
    /// Enable sequence number validation during commit
    use_starting_sequence_number: bool,
    /// Metrics recorder for commit operations
    metrics_recorder: CompactionMetricsRecorder,
    /// Schema ID for validation
    basic_schema_id: i32,
}

/// Parameters for commit consistency validation.
pub struct CommitConsistencyParams {
    /// Base snapshot ID for consistency validation
    pub starting_snapshot_id: i64,
    /// Enable sequence number validation
    pub use_starting_sequence_number: bool,
    /// Table schema ID for validation
    pub basic_schema_id: i32,
}

impl CommitManager {
    /// Creates a new `CommitManager` with retry configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: CommitManagerRetryConfig,
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        table_ident_name: impl Into<Cow<'static, str>>,
        catalog_name: impl Into<Cow<'static, str>>,
        metrics: Arc<Metrics>,
        consistency_params: CommitConsistencyParams,
    ) -> Self {
        let catalog_name = catalog_name.into();
        let table_ident_name = table_ident_name.into();

        let metrics_recorder =
            CompactionMetricsRecorder::new(metrics, catalog_name.clone(), table_ident_name.clone());

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

    /// Collects added and rewritten files from rewrite results by loading snapshot.
    ///
    /// # Performance
    ///
    /// Loads snapshot files once, builds `HashMap` index for efficient lookup.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `rewrite_results` is empty
    /// - Results have inconsistent `to_branch` or `snapshot_id`
    /// - Snapshot or file loading fails
    async fn collect_files_from_results(
        &self,
        rewrite_results: &[RewriteResult],
        to_branch: &str,
    ) -> Result<(Vec<DataFile>, Vec<DataFile>)> {
        if rewrite_results.is_empty() {
            return Err(CompactionError::Execution(
                "No rewrite results to process".to_owned(),
            ));
        }

        let snapshot_id = rewrite_results[0].plan.snapshot_id;

        // Validate consistency across all rewrite results
        validate_rewrite_results_consistency(rewrite_results, snapshot_id, to_branch)?;

        // Load table and get snapshot
        let table = self.catalog.load_table(&self.table_ident).await?;
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                CompactionError::Execution(format!("Snapshot {} not found", snapshot_id))
            })?;

        // --- Batch collect input files from all plans ---
        use std::collections::HashMap;

        // 1. Load all files from snapshot once
        let (all_data_files, _all_delete_files) =
            get_all_files_from_snapshot(snapshot, table.file_io(), table.metadata()).await?;

        // 2. Build efficient path -> DataFile index (only for data files)
        let data_file_index: HashMap<&str, &DataFile> =
            all_data_files.iter().map(|f| (f.file_path(), f)).collect();

        // 3. Collect rewritten data files (to be replaced) from plans using the index
        // Note: Only data files are collected, delete files are excluded
        let rewritten_data_files: Vec<DataFile> = rewrite_results
            .iter()
            .flat_map(|rr| {
                rr.plan
                    .file_group
                    .data_files
                    .iter()
                    .map(|task| task.data_file_path.as_str())
            })
            .filter_map(|path| data_file_index.get(path).map(|&f| f.clone()))
            .collect();

        // 4. Collect added data files (newly written) from all plans
        let added_data_files: Vec<DataFile> = rewrite_results
            .iter()
            .flat_map(|rr| rr.output_data_files.iter().cloned())
            .collect();

        Ok((added_data_files, rewritten_data_files))
    }

    /// Rewrites files from results: file collection, validation, and commit.
    ///
    /// # Errors
    ///
    /// Propagates errors from `collect_files_from_results()` and `rewrite_files()`.
    pub async fn rewrite_files_from_results(
        &self,
        rewrite_results: Vec<RewriteResult>,
        to_branch: &str,
    ) -> Result<Table> {
        let (added_data_files, rewritten_data_files) = self
            .collect_files_from_results(&rewrite_results, to_branch)
            .await?;
        self.rewrite_files(added_data_files, rewritten_data_files, to_branch)
            .await
    }

    /// Overwrites files from results: file collection, validation, and commit.
    ///
    /// # Errors
    ///
    /// Propagates errors from `collect_files_from_results()` and `overwrite_files()`.
    pub async fn overwrite_files_from_results(
        &self,
        rewrite_results: Vec<RewriteResult>,
        to_branch: &str,
    ) -> Result<Table> {
        let (added_data_files, rewritten_data_files) = self
            .collect_files_from_results(&rewrite_results, to_branch)
            .await?;
        self.overwrite_files(added_data_files, rewritten_data_files, to_branch)
            .await
    }

    /// Rewrites files with retry on transient failures (e.g., optimistic lock).
    ///
    /// # Errors
    ///
    /// Returns error if all retries exhausted or non-retryable error occurs.
    pub async fn rewrite_files(
        &self,
        added_data_files: Vec<DataFile>,
        rewritten_data_files: Vec<DataFile>,
        to_branch: &str,
    ) -> Result<Table> {
        let data_files = added_data_files;
        let delete_files = rewritten_data_files;

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
                            .with_delete_filter_manager_enabled()
                            .add_data_files(data_files)?
                            .delete_files(delete_files)?
                            .with_to_branch(to_branch.to_owned())
                            .with_starting_sequence_number(snapshot.sequence_number())?
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
                        .with_delete_filter_manager_enabled()
                        .add_data_files(data_files)?
                        .delete_files(delete_files)?
                        .with_to_branch(to_branch.to_owned())
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

    /// Overwrites files with retry on transient failures (e.g., optimistic lock).
    ///
    /// # Errors
    ///
    /// Returns error if all retries exhausted or non-retryable error occurs.
    pub async fn overwrite_files(
        &self,
        added_data_files: Vec<DataFile>,
        rewritten_data_files: Vec<DataFile>,
        to_branch: &str,
    ) -> Result<Table> {
        let data_files = added_data_files;
        let delete_files = rewritten_data_files;

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
                let overwrite_action = if use_starting_sequence_number {
                    // TODO: avoid retry if the snapshot_id is not found
                    if let Some(snapshot) = table.metadata().snapshot_by_id(starting_snapshot_id) {
                        txn.overwrite_files(None, vec![])?
                            .add_data_files(data_files)?
                            .delete_files(delete_files)?
                            .with_to_branch(to_branch.to_owned())
                            .with_starting_sequence_number(snapshot.sequence_number())?
                    } else {
                        return Err(iceberg::Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "No snapshot found with the given snapshot_id {starting_snapshot_id}"
                            ),
                        ));
                    }
                } else {
                    txn.overwrite_files(None, vec![])?
                        .add_data_files(data_files)?
                        .delete_files(delete_files)?
                        .with_to_branch(to_branch.to_owned())
                };

                let txn = overwrite_action.apply().await?;
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
            .map_err(|e: iceberg::Error| CompactionError::from(e))
    }
}

/// Compaction plan describing files to rewrite and target commit location.
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    /// Group of files to be compacted together
    pub file_group: FileGroup,
    /// Target branch for committing the compaction result
    pub to_branch: Cow<'static, str>,
    /// Snapshot ID from which files were selected
    pub snapshot_id: i64,
}

impl CompactionPlan {
    /// Creates a new compaction plan.
    pub fn new(
        file_group: FileGroup,
        to_branch: impl Into<Cow<'static, str>>,
        snapshot_id: i64,
    ) -> Self {
        Self {
            file_group,
            to_branch: to_branch.into(),
            snapshot_id,
        }
    }

    /// Creates an empty plan for testing.
    pub fn dummy() -> Self {
        Self {
            file_group: FileGroup::empty(),
            to_branch: Cow::Borrowed(MAIN_BRANCH),
            snapshot_id: UNASSIGNED_SNAPSHOT_ID,
        }
    }

    /// Returns total number of files to be compacted.
    pub fn file_count(&self) -> usize {
        self.file_group.input_files_count()
    }

    /// Returns total size in bytes of files to be compacted.
    pub fn total_bytes(&self) -> u64 {
        self.file_group.input_total_bytes()
    }

    /// Returns group count: 0 if empty, 1 otherwise.
    pub fn group_count(&self) -> usize {
        if self.file_group.is_empty() {
            0
        } else {
            1
        }
    }

    /// Returns recommended executor parallelism from file group.
    pub fn recommended_executor_parallelism(&self) -> usize {
        self.file_group.executor_parallelism
    }

    /// Returns recommended output parallelism from file group.
    pub fn recommended_output_parallelism(&self) -> usize {
        self.file_group.output_parallelism
    }
}

/// Planner for generating compaction plans from table snapshots.
pub struct CompactionPlanner {
    config: CompactionPlanningConfig,
}

impl CompactionPlanner {
    /// Creates a new planner with the given configuration.
    pub fn new(config: CompactionPlanningConfig) -> Self {
        Self { config }
    }

    /// Plans compaction for a specific branch.
    ///
    /// # Returns
    ///
    /// Vector of `CompactionPlan` based on file grouping strategy.
    ///
    /// # Errors
    ///
    /// Returns error if branch snapshot not found or file grouping fails.
    pub async fn plan_compaction_with_branch(
        &self,
        table: &Table,
        to_branch: &str,
    ) -> Result<Vec<CompactionPlan>> {
        if let Some(branch_snapshot) = table.metadata().snapshot_for_ref(to_branch) {
            // Step 1: Group files for compaction (extensible)
            let file_groups: Vec<FileGroup> = self
                .group_files_for_compaction(table, branch_snapshot.snapshot_id())
                .await?;

            // Convert each FileGroup to a separate CompactionPlan
            let plans = file_groups
                .into_iter()
                .map(|file_group| {
                    CompactionPlan::new(
                        file_group,
                        to_branch.to_owned(),
                        branch_snapshot.snapshot_id(),
                    )
                })
                .collect();

            Ok(plans)
        } else {
            Ok(vec![])
        }
    }

    /// Plans compaction for the main branch.
    pub async fn plan_compaction(&self, table: &Table) -> Result<Vec<CompactionPlan>> {
        self.plan_compaction_with_branch(table, MAIN_BRANCH).await
    }

    /// Customization point for file grouping logic.
    ///
    /// Default implementation uses `FileStrategy`. Override for custom behavior.
    async fn group_files_for_compaction(
        &self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<Vec<FileGroup>> {
        use crate::file_selection::PlanStrategy;

        let strategy = PlanStrategy::from(&self.config);
        FileSelector::get_scan_tasks_with_strategy(table, snapshot_id, strategy, &self.config).await
    }
}

#[cfg(test)]
mod tests {
    use crate::compaction::{CompactionBuilder, CompactionPlanner};
    use crate::config::{
        CompactionConfigBuilder, CompactionExecutionConfigBuilder, CompactionPlanningConfig,
        SmallFilesConfigBuilder,
    };
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use iceberg::arrow::schema_to_arrow_schema;
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type, MAIN_BRANCH};
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

    // Additional imports for new tests
    use crate::compaction::{CommitManagerRetryConfig, CompactionPlan, RewriteResult};
    use crate::executor::{ExecutorType, RewriteFilesStat};
    use iceberg::spec::{DataFile, UNASSIGNED_SNAPSHOT_ID};
    use std::time::Duration;

    // ----------------------
    // Test helpers to reduce duplication
    // ----------------------

    struct TestEnv {
        #[allow(dead_code)]
        temp_dir: TempDir,
        warehouse_location: String,
        catalog: Arc<MemoryCatalog>,
        table_ident: TableIdent,
        table: Table,
    }

    async fn create_test_env() -> TestEnv {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let catalog = Arc::new(MemoryCatalog::new(
            file_io,
            Some(warehouse_location.clone()),
        ));

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(catalog.as_ref(), &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(catalog.as_ref(), &table_ident).await;

        let table = catalog.load_table(&table_ident).await.unwrap();

        TestEnv {
            temp_dir,
            warehouse_location,
            catalog,
            table_ident,
            table,
        }
    }

    async fn append_and_commit<C: Catalog>(
        table: &Table,
        catalog: &C,
        data_files: Vec<DataFile>,
    ) -> Table {
        let transaction = Transaction::new(table);
        let mut append_action = transaction.fast_append(None, None, vec![]).unwrap();
        append_action.add_data_files(data_files).unwrap();
        let tx = append_action.apply().await.unwrap();
        tx.commit(catalog).await.unwrap()
    }

    async fn write_simple_files(
        table: &Table,
        warehouse_location: &str,
        suffix_prefix: &str,
        count: usize,
    ) -> Vec<DataFile> {
        let mut all = Vec::new();
        for i in 0..count {
            let mut writer = build_simple_data_writer(
                table,
                warehouse_location.to_owned(),
                &format!("{suffix_prefix}_{i}"),
            )
            .await;
            let batch = create_test_record_batch(&simple_table_schema());
            writer.write(batch).await.unwrap();
            let files = writer.close().await.unwrap();
            all.extend(files);
        }
        all
    }

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
        let initial_file_count = data_files.len();

        // Start transaction and commit
        let transaction = Transaction::new(&table);
        let mut append_action = transaction.fast_append(None, None, vec![]).unwrap();
        append_action.add_data_files(data_files).unwrap();
        let tx = append_action.apply().await.unwrap();

        // Commit the transaction
        let updated_table = tx.commit(&catalog).await.unwrap();

        // Verify the snapshot was created
        let snapshots = updated_table.metadata().snapshots();
        assert_eq!(
            snapshots.len(),
            1,
            "Should have exactly one snapshot after first commit"
        );

        let latest_snapshot = updated_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();

        // Verify we can load the table again and see the data
        let reloaded_table = catalog.load_table(&table_ident).await.unwrap();
        let current_snapshot = reloaded_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
        assert_eq!(
            current_snapshot.snapshot_id(),
            latest_snapshot.snapshot_id()
        );

        let execution_config = CompactionExecutionConfigBuilder::default()
            .enable_validate_compaction(true)
            .build()
            .unwrap();

        let rewrite_files_resp = CompactionBuilder::new(Arc::new(catalog), table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default()
                    .execution(execution_config)
                    .build()
                    .unwrap(),
            ))
            .build()
            .compact()
            .await
            .unwrap()
            .unwrap();

        // Verify compaction reduced file count (compacted files together)
        assert_eq!(
            rewrite_files_resp.stats.input_files_count,
            initial_file_count
        );
        assert!(
            rewrite_files_resp.stats.output_files_count <= initial_file_count,
            "Compaction should reduce or maintain file count, got {} output from {} input",
            rewrite_files_resp.stats.output_files_count,
            initial_file_count
        );
        assert!(
            rewrite_files_resp.stats.output_files_count > 0,
            "Compaction should produce at least one output file"
        );
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

        // Create multiple data files using helper and commit
        let data_files = write_simple_files(&table, &warehouse_location, "test", 3).await;
        let initial_file_count = data_files.len();
        let updated_table = append_and_commit(&table, &catalog, data_files).await;

        // Verify initial state
        let snapshot_before = updated_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
        // Verify snapshot has summary (Summary is always present, check it's not trivial)
        assert_eq!(
            snapshot_before.summary().additional_properties.len(),
            0,
            "Fresh snapshot should not have additional properties yet"
        );

        // Test full compaction - should compact all files
        let catalog_arc = Arc::new(catalog);
        let rewrite_files_resp = CompactionBuilder::new(catalog_arc.clone(), table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .build()
            .compact()
            .await
            .unwrap()
            .unwrap();

        // Full compaction should rewrite all existing files
        assert_eq!(
            rewrite_files_resp.stats.input_files_count, initial_file_count,
            "Should compact all {} files",
            initial_file_count
        );
        // Should create fewer or equal files (compaction benefit)
        assert!(
            rewrite_files_resp.stats.output_files_count > 0,
            "Should produce at least one output file"
        );
        assert!(
            rewrite_files_resp.stats.output_files_count <= initial_file_count,
            "Compaction should not increase file count"
        );

        // Verify bytes processed
        assert!(
            rewrite_files_resp.stats.input_total_bytes > 0,
            "Should have processed some input bytes"
        );
        assert!(
            rewrite_files_resp.stats.output_total_bytes > 0,
            "Should have produced some output bytes"
        );

        // Verify table was updated
        assert!(
            rewrite_files_resp.table.is_some(),
            "Should return updated table"
        );
        let final_table = rewrite_files_resp.table.unwrap();
        let snapshot_after = final_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
        assert_ne!(
            snapshot_before.snapshot_id(),
            snapshot_after.snapshot_id(),
            "Should create new snapshot after compaction"
        );
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

        // Create multiple small data files using helper
        let batch = create_test_record_batch(&simple_table_schema());
        let small_files1 = write_simple_files(&table, &warehouse_location, "small1", 1).await;
        let small_files2 = write_simple_files(&table, &warehouse_location, "small2", 1).await;

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

        let updated_table = append_and_commit(&table, &catalog, all_data_files).await;

        // Get files before compaction for validation
        let snapshot_before = updated_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
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
            .planning(CompactionPlanningConfig::SmallFiles(
                SmallFilesConfigBuilder::default()
                    .small_file_threshold_bytes(small_file_threshold)
                    .build()
                    .unwrap(),
            ))
            .build()
            .unwrap();

        let compaction = CompactionBuilder::new(catalog_arc.clone(), table_ident.clone())
            .with_config(Arc::new(compaction_config))
            .build();

        let planner = CompactionPlanner::new(compaction.config.as_ref().unwrap().planning.clone());

        // Get the files that would be grouped for compaction
        let files_to_compact = planner
            .group_files_for_compaction(&updated_table, snapshot_before.snapshot_id())
            .await
            .unwrap();

        // Validate file selection logic
        let selected_file_paths: std::collections::HashSet<&str> = files_to_compact
            .iter()
            .flat_map(|group| &group.data_files)
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
        let rewrite_files_resp = compaction.compact().await.unwrap().unwrap();

        // Validate compaction results
        assert_eq!(
            rewrite_files_resp.stats.input_files_count,
            small_files_count,
        );

        // Should create fewer files than were compacted (compaction benefit)
        assert!(rewrite_files_resp.stats.output_files_count <= small_files_count);
        assert!(rewrite_files_resp.stats.output_files_count > 0);

        // Verify final state: total files should be reduced
        let final_table = catalog_arc.load_table(&table_ident).await.unwrap();
        let final_snapshot = final_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
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
            large_files_count + rewrite_files_resp.stats.output_files_count as usize;
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
        let env = create_test_env().await;
        let catalog = env.catalog.as_ref();
        let table = &env.table;

        let planner = CompactionPlanner::new(CompactionPlanningConfig::default());

        // Test empty table - should return empty plan
        let plan = planner.plan_compaction(table).await.unwrap();
        assert!(
            plan.is_empty(),
            "Empty table should produce no compaction plans"
        );

        // Create some data files
        let data_files = write_simple_files(table, &env.warehouse_location, "test", 2).await;
        let expected_file_count = data_files.len();

        // Commit the files
        let updated_table = append_and_commit(table, catalog, data_files).await;

        let planner = CompactionPlanner::new(CompactionPlanningConfig::Full(
            crate::config::FullCompactionConfig::default(),
        ));

        // Test plan with data
        let plans = planner.plan_compaction(&updated_table).await.unwrap();

        assert!(
            !plans.is_empty(),
            "Table with data should produce compaction plans"
        );
        let plan = &plans[0];
        assert_eq!(
            plan.file_count(),
            expected_file_count,
            "Plan should include all {} files",
            expected_file_count
        );
        assert!(
            plan.total_bytes() > 0,
            "Plan should have non-zero total bytes"
        );
        assert!(
            plan.recommended_executor_parallelism() > 0,
            "Should recommend positive executor parallelism"
        );
        assert!(
            plan.recommended_output_parallelism() > 0,
            "Should recommend positive output parallelism"
        );
        assert_eq!(
            plan.to_branch, MAIN_BRANCH,
            "Should target main branch by default"
        );
        assert_eq!(
            plan.group_count(),
            1,
            "Non-empty plan should have group_count of 1"
        );
    }

    /// Test plan_compaction with non-existent branch
    #[tokio::test]
    async fn test_plan_compaction_invalid_branch() {
        let env = create_test_env().await;
        let table = &env.table;

        let planner = CompactionPlanner::new(CompactionPlanningConfig::default());

        // Test with non-existent branch - should return error or empty plan
        let result = planner
            .plan_compaction_with_branch(table, "non-existent-branch")
            .await;

        // The current implementation returns an error for non-existent branch
        // If it changes to return empty plan in the future, both are acceptable
        match result {
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("non-existent-branch")
                        || error_msg.contains("not found")
                        || error_msg.contains("snapshot"),
                    "Error should mention the branch or snapshot issue, got: {}",
                    error_msg
                );
            }
            Ok(plans) => {
                // Alternative acceptable behavior: return empty plans
                assert!(
                    plans.is_empty(),
                    "Non-existent branch should produce empty plans or error"
                );
            }
        }
    }

    /// Test the compact_with_plan functionality separately
    #[tokio::test]
    async fn test_compact_with_plan() {
        // Create test data via helper
        let env = create_test_env().await;
        let catalog = env.catalog.clone();
        let table_ident = env.table_ident.clone();
        let table = env.table.clone();

        // Create some data files
        let data_files = write_simple_files(&table, &env.warehouse_location, "test", 2).await;
        let initial_file_count = data_files.len();

        // Commit the files
        let updated_table = append_and_commit(&table, catalog.as_ref(), data_files).await;

        // Create compaction instance
        let full_compaction_config = CompactionConfigBuilder::default()
            .planning(CompactionPlanningConfig::Full(
                crate::config::FullCompactionConfig::default(),
            ))
            .build()
            .unwrap();
        let compaction = CompactionBuilder::new(catalog.clone(), table_ident.clone())
            .with_config(Arc::new(full_compaction_config))
            .build();

        let planner = CompactionPlanner::new(compaction.config.as_ref().unwrap().planning.clone());

        // Test planning separately
        let plans = planner.plan_compaction(&updated_table).await.unwrap();

        assert!(!plans.is_empty(), "Should generate at least one plan");

        let plan = &plans[0];

        // Test execution with the plan
        let rewrite_files_resp = compaction
            .compact_with_plan(plan.clone(), &compaction.config.as_ref().unwrap().execution)
            .await
            .unwrap();

        assert!(
            rewrite_files_resp.is_some(),
            "Should return compaction result"
        );
        let result = rewrite_files_resp.unwrap();
        assert_eq!(
            result.stats.input_files_count, initial_file_count,
            "Should process all {} input files",
            initial_file_count
        );
        assert!(
            result.stats.output_files_count > 0,
            "Should produce at least one output file"
        );
        assert!(
            result.stats.output_files_count <= initial_file_count,
            "Should not increase file count"
        );
    }

    /// Test compact_with_plan with empty plan
    #[tokio::test]
    async fn test_compact_with_plan_empty() {
        let env = create_test_env().await;
        let catalog = env.catalog.clone();
        let table_ident = env.table_ident.clone();

        let compaction = CompactionBuilder::new(catalog.clone(), table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .build();

        // Create an empty plan
        let empty_plan = CompactionPlan::new(
            crate::file_selection::FileGroup::empty(),
            MAIN_BRANCH,
            UNASSIGNED_SNAPSHOT_ID,
        );

        let result = compaction
            .compact_with_plan(empty_plan, &compaction.config.as_ref().unwrap().execution)
            .await
            .unwrap();

        assert!(result.is_none(), "Empty plan should return None");
    }

    /// Test compact_with_plan with branch functionality
    #[tokio::test]
    async fn test_compact_with_plan_with_branch() {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let catalog = MemoryCatalog::new(file_io, Some(warehouse_location.clone()));

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        let table = catalog.load_table(&table_ident).await.unwrap();

        // Create some data files on feature branch
        let mut writer1 =
            build_simple_data_writer(&table, warehouse_location.clone(), "branch1").await;
        let batch = create_test_record_batch(&simple_table_schema());
        writer1.write(batch.clone()).await.unwrap();
        let branch_data_files1 = writer1.close().await.unwrap();

        let mut writer2 =
            build_simple_data_writer(&table, warehouse_location.clone(), "branch2").await;
        writer2.write(batch.clone()).await.unwrap();
        let branch_data_files2 = writer2.close().await.unwrap();

        // Commit to feature branch
        let transaction = Transaction::new(&table);
        let branch_name = "feature/compaction-branch";
        let mut append_action = transaction
            .fast_append(None, None, vec![])
            .unwrap()
            .with_to_branch(branch_name.to_owned());
        append_action.add_data_files(branch_data_files1).unwrap();
        append_action.add_data_files(branch_data_files2).unwrap();
        let tx = append_action.apply().await.unwrap();
        let updated_table = tx.commit(&catalog).await.unwrap();

        // Test compaction on feature branch
        let compaction = CompactionBuilder::new(Arc::new(catalog), table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .with_to_branch(branch_name.to_owned())
            .build();

        let planner = CompactionPlanner::new(CompactionPlanningConfig::default());
        let plans = planner
            .plan_compaction_with_branch(&updated_table, branch_name)
            .await
            .unwrap();

        assert!(
            !plans.is_empty(),
            "Should generate plans for feature branch"
        );
        let plan = &plans[0];

        assert_eq!(
            plan.file_count(),
            2,
            "Should have 2 files on feature branch"
        );
        assert_eq!(
            plan.to_branch, branch_name,
            "Plan should target feature branch"
        );

        let result = compaction
            .compact_with_plan(plan.clone(), &compaction.config.as_ref().unwrap().execution)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            result.stats.input_files_count, 2,
            "Should compact 2 input files"
        );
        assert!(
            result.stats.output_files_count > 0,
            "Should produce at least one output file"
        );
        assert!(
            result.stats.output_files_count <= 2,
            "Should not increase file count beyond input"
        );
    }

    /// Test branch functionality with small files compaction
    #[tokio::test]
    async fn test_small_files_compaction_with_branch() {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let catalog = MemoryCatalog::new(file_io.clone(), Some(warehouse_location.clone()));

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        let table = catalog.load_table(&table_ident).await.unwrap();

        // test planning and compaction on a new branch
        let new_branch = "feature/small-files-compaction";

        let mut small_writer1 =
            build_simple_data_writer(&table, warehouse_location.clone(), "small-branch").await;
        let batch = create_test_record_batch(&simple_table_schema());
        small_writer1.write(batch.clone()).await.unwrap();
        let small_files1 = small_writer1.close().await.unwrap();

        let mut large_writer =
            build_simple_data_writer(&table, warehouse_location.clone(), "large-branch").await;
        for _ in 0..10 {
            large_writer.write(batch.clone()).await.unwrap();
        }
        let large_files = large_writer.close().await.unwrap();

        // Append the small files to a new branch
        let mut all_branch_files = Vec::new();
        all_branch_files.extend(small_files1);
        all_branch_files.extend(large_files);

        let transaction = Transaction::new(&table);
        let mut append_action = transaction
            .fast_append(None, None, vec![])
            .unwrap()
            .with_to_branch(new_branch.to_owned());
        append_action.add_data_files(all_branch_files).unwrap();
        let tx = append_action.apply().await.unwrap();
        let updated_table = tx.commit(&catalog).await.unwrap();

        // Test small files compaction on the branch
        let small_file_threshold = 900u64; // 900B threshold
        let planning_config = CompactionPlanningConfig::SmallFiles(
            SmallFilesConfigBuilder::default()
                .small_file_threshold_bytes(small_file_threshold)
                .build()
                .unwrap(),
        );

        let branch_planner = CompactionPlanner::new(planning_config.clone());

        let branch_plans = branch_planner
            .plan_compaction_with_branch(&updated_table, new_branch)
            .await
            .unwrap();

        assert!(
            !branch_plans.is_empty(),
            "Should generate plans for small files"
        );
        let branch_plan = &branch_plans[0];

        assert_eq!(
            branch_plan.file_count(),
            1,
            "Should select only 1 small file"
        );
        assert_eq!(
            branch_plan.to_branch, new_branch,
            "Plan should target the feature branch"
        );
        let input_file_path = branch_plan.file_group.data_files[0].data_file_path();
        assert!(
            input_file_path.contains("small-branch"),
            "Should select the small file, not the large one"
        );

        // Run the actual compaction on the branch
        let branch_compaction = CompactionBuilder::new(Arc::new(catalog), table_ident.clone())
            .with_to_branch(new_branch.to_owned())
            .build();

        let rewrite_files_resp = branch_compaction
            .compact_with_plan(
                branch_plan.clone(),
                &CompactionExecutionConfigBuilder::default().build().unwrap(),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            rewrite_files_resp.stats.input_files_count, 1,
            "Should compact 1 small file"
        );
        assert!(
            rewrite_files_resp.stats.output_files_count > 0,
            "Should produce output files"
        );
    }

    /// Consolidated commit validation scenarios to avoid repeated init
    #[tokio::test]
    async fn test_commit_validations() {
        use crate::file_selection::FileGroup;

        // Shared environment
        let env = create_test_env().await;

        // Compaction configured for main branch for consistent checks
        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_to_branch(MAIN_BRANCH.to_owned())
            .build();

        // 1) Branch mismatch
        let plan1 = CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, 1);
        let plan2 = CompactionPlan::new(FileGroup::empty(), "feature-branch", 1);
        let r1 = RewriteResult {
            output_data_files: vec![],
            stats: RewriteFilesStat::default(),
            plan: plan1,
            validation_info: None,
        };
        let r2 = RewriteResult {
            output_data_files: vec![],
            stats: RewriteFilesStat::default(),
            plan: plan2,
            validation_info: None,
        };
        let err = compaction
            .commit_rewrite_results(vec![r1, r2])
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("does not match configured branch"),
            "Branch mismatch message"
        );

        // 2) Snapshot mismatch (same branch)
        let plan1 = CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, 1);
        let plan2 = CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, 2);
        let r1 = RewriteResult {
            output_data_files: vec![],
            stats: RewriteFilesStat::default(),
            plan: plan1,
            validation_info: None,
        };
        let r2 = RewriteResult {
            output_data_files: vec![],
            stats: RewriteFilesStat::default(),
            plan: plan2,
            validation_info: None,
        };
        let err = compaction
            .commit_rewrite_results(vec![r1, r2])
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("does not match other plans snapshot"),
            "Snapshot mismatch message"
        );

        // 3) Success with consistent plans
        let data_files = write_simple_files(&env.table, &env.warehouse_location, "test", 1).await;
        let updated_table =
            append_and_commit(&env.table, env.catalog.as_ref(), data_files.clone()).await;
        let snapshot_id = updated_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap()
            .snapshot_id();
        let plan1 = CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, snapshot_id);
        let plan2 = CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, snapshot_id);
        let r1 = RewriteResult {
            output_data_files: data_files.clone(),
            stats: RewriteFilesStat::default(),
            plan: plan1,
            validation_info: None,
        };
        let r2 = RewriteResult {
            output_data_files: vec![],
            stats: RewriteFilesStat::default(),
            plan: plan2,
            validation_info: None,
        };
        let ok = compaction.commit_rewrite_results(vec![r1, r2]).await;
        assert!(ok.is_ok(), "Commit should succeed with consistent plans");

        // 4) Empty results rejection
        let err = compaction
            .commit_rewrite_results(vec![])
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("No rewrite results to commit"),
            "Empty results message"
        );
    }

    /// Test branch validation in rewrite_plan method
    #[tokio::test]
    async fn test_rewrite_plan_branch_validation() {
        use crate::config::CompactionExecutionConfigBuilder;
        use crate::file_selection::FileGroup;

        // Reuse shared env
        let env = create_test_env().await;

        // Create compaction configured for "main" branch
        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_to_branch("main".to_owned())
            .build();

        // Create a plan for a different branch
        let plan = CompactionPlan::new(FileGroup::empty(), "feature-branch", 1);

        let execution_config = CompactionExecutionConfigBuilder::default().build().unwrap();
        let table = env.catalog.load_table(&env.table_ident).await.unwrap();

        // Test should fail due to branch mismatch
        let rewrite_result = compaction
            .rewrite_plan(plan, &execution_config, &table)
            .await;
        assert!(
            rewrite_result.is_err(),
            "Branch mismatch should cause error"
        );
        let error_msg = rewrite_result.unwrap_err().to_string();
        assert!(
            error_msg.contains("does not match configured branch"),
            "Error should mention branch mismatch, got: {}",
            error_msg
        );
    }

    /// Test CompactionPlan helper methods
    #[tokio::test]
    async fn test_compaction_plan_methods() {
        // Test dummy plan
        let dummy = CompactionPlan::dummy();
        assert_eq!(dummy.file_count(), 0, "Dummy plan should have no files");
        assert_eq!(dummy.total_bytes(), 0, "Dummy plan should have zero bytes");
        assert_eq!(dummy.group_count(), 0, "Dummy plan should have zero groups");
        assert_eq!(
            dummy.to_branch, MAIN_BRANCH,
            "Dummy plan should target main branch"
        );
        assert_eq!(
            dummy.snapshot_id, UNASSIGNED_SNAPSHOT_ID,
            "Dummy plan should have unassigned snapshot"
        );

        // Test empty plan
        let empty_plan = CompactionPlan::new(
            crate::file_selection::FileGroup::empty(),
            "test-branch",
            123,
        );
        assert_eq!(empty_plan.file_count(), 0);
        assert_eq!(empty_plan.total_bytes(), 0);
        assert_eq!(empty_plan.group_count(), 0);
        assert_eq!(empty_plan.to_branch, "test-branch");
        assert_eq!(empty_plan.snapshot_id, 123);
    }

    /// Test no compaction needed scenario
    #[tokio::test]
    async fn test_compact_no_files() {
        let env = create_test_env().await;

        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .build();

        // Compact empty table
        let result = compaction.compact().await.unwrap();

        assert!(
            result.is_none(),
            "Empty table should return None from compact()"
        );
    }

    /// Test CompactionBuilder configuration
    #[tokio::test]
    async fn test_compaction_builder() {
        let env = create_test_env().await;

        // Test builder with custom settings
        let custom_registry = Box::new(mixtrics::registry::noop::NoopMetricsRegistry);
        let retry_config = CommitManagerRetryConfig {
            max_retries: 5,
            retry_initial_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(10),
        };

        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .with_executor_type(ExecutorType::DataFusion)
            .with_catalog_name("test-catalog")
            .with_registry(custom_registry)
            .with_retry_config(retry_config.clone())
            .with_to_branch("custom-branch")
            .build();

        assert_eq!(compaction.to_branch, "custom-branch");
        assert_eq!(compaction.catalog_name, "test-catalog");
        assert_eq!(
            compaction.commit_retry_config.max_retries,
            retry_config.max_retries
        );
        assert!(compaction.config.is_some());
    }

    /// Test metrics are accessible
    #[tokio::test]
    async fn test_compaction_metrics() {
        let env = create_test_env().await;

        let compaction =
            CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone()).build();

        let metrics = compaction.metrics();
        assert!(
            Arc::ptr_eq(&metrics, &compaction.metrics),
            "Should return same metrics instance"
        );
    }

    /// Test rewrite_plan with invalid snapshot
    #[tokio::test]
    async fn test_rewrite_plan_invalid_snapshot() {
        let env = create_test_env().await;

        let compaction =
            CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone()).build();

        let table = env.catalog.load_table(&env.table_ident).await.unwrap();

        // Create a plan with non-existent snapshot ID
        let invalid_plan = CompactionPlan::new(
            crate::file_selection::FileGroup::empty(),
            MAIN_BRANCH,
            999999, // Non-existent snapshot ID
        );

        let execution_config = CompactionExecutionConfigBuilder::default().build().unwrap();

        let result = compaction
            .rewrite_plan(invalid_plan, &execution_config, &table)
            .await;

        assert!(result.is_err(), "Invalid snapshot should cause error");
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("not found") || error_msg.contains("999999"),
            "Error should mention snapshot issue, got: {}",
            error_msg
        );
    }

    /// Test compact without config should fail
    #[tokio::test]
    async fn test_compact_without_config() {
        let env = create_test_env().await;

        // Create compaction WITHOUT config
        let compaction =
            CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone()).build();

        assert!(compaction.config.is_none(), "Should not have config");

        let result = compaction.compact().await;

        assert!(result.is_err(), "compact() without config should fail");
        if let Err(e) = result {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("config") || error_msg.contains("required"),
                "Error should mention missing config, got: {}",
                error_msg
            );
        }
    }

    /// Test plan_compaction without config should fail
    #[tokio::test]
    async fn test_plan_compaction_without_config() {
        let env = create_test_env().await;

        // Create compaction WITHOUT config
        let compaction =
            CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone()).build();

        let result = compaction.plan_compaction().await;

        assert!(
            result.is_err(),
            "plan_compaction() without config should fail"
        );
    }
}
