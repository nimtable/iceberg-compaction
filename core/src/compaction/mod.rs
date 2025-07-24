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
use iceberg::spec::{DataFile, Snapshot, MAIN_BRANCH};
use iceberg::{Catalog, ErrorKind, TableIdent};
use mixtrics::metrics::BoxedRegistry;
use mixtrics::registry::noop::NoopMetricsRegistry;

use crate::common::{CompactionMetricsRecorder, Metrics};
use crate::compaction::validator::CompactionValidator;
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
    commit_retry_config: CommitManagerRetryConfig,

    to_branch: Option<String>,
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
            commit_retry_config: CommitManagerRetryConfig::default(),
            to_branch: None,
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

    pub fn with_retry_config(mut self, retry_config: CommitManagerRetryConfig) -> Self {
        self.commit_retry_config = retry_config;
        self
    }

    pub fn with_to_branch(mut self, to_branch: String) -> Self {
        self.to_branch = Some(to_branch);
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

        let to_branch = self.to_branch.clone();

        Ok(Compaction {
            config,
            executor,
            catalog,
            metrics,
            table_ident,
            compaction_type,
            catalog_name,
            commit_retry_config,
            to_branch,
        })
    }
}

impl Default for CompactionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A Proxy for the compaction process, which handles the configuration, executor, and catalog.
pub struct Compaction {
    pub config: Arc<CompactionConfig>,
    pub executor: Box<dyn CompactionExecutor>,
    pub catalog: Arc<dyn Catalog>,
    pub metrics: Arc<Metrics>,
    pub table_ident: TableIdent,
    pub compaction_type: CompactionType,
    pub catalog_name: String,

    pub commit_retry_config: CommitManagerRetryConfig,
    pub to_branch: Option<String>,
}

#[derive(Default)]
pub struct CompactionResult {
    pub data_files: Vec<DataFile>,
    pub stats: RewriteFilesStat,
    pub table: Option<Table>,
}

impl Compaction {
    /// Create a new `CompactionBuilder` for flexible configuration
    pub fn builder() -> CompactionBuilder {
        CompactionBuilder::new()
    }

    pub async fn compact(&self) -> Result<CompactionResult> {
        let (compaction_result, compaction_validator) = match self.compaction_type {
            CompactionType::Full | CompactionType::MergeSmallDataFiles => {
                // Use the generic implementation for simple compaction types
                self.execute_standard_compaction().await?
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

        Ok(compaction_result)
    }

    /// Standard compaction implementation for simple file-based compaction types
    /// This works well for Full and `SmallFiles` compaction where the main difference
    /// is the `FileStrategy` used for file selection
    async fn execute_standard_compaction(
        &self,
    ) -> Result<(CompactionResult, Option<CompactionValidator>)> {
        let now = std::time::Instant::now();
        let metrics_recorder = CompactionMetricsRecorder::new(
            self.metrics.clone(),
            self.catalog_name.clone(),
            self.table_ident.to_string(),
        );

        let table = self.catalog.load_table(&self.table_ident).await?;

        // check if the current snapshot exists
        let current_snapshot = match &self.to_branch {
            Some(branch) => table.metadata().snapshot_for_ref(branch),
            None => table.metadata().current_snapshot(),
        };

        if current_snapshot.is_none() {
            return Ok((CompactionResult::default(), None));
        }

        let current_snapshot = current_snapshot.unwrap();

        // Step 1: Select files for compaction (extensible)
        let input_file_scan_tasks = self
            .select_files_for_compaction(&table, current_snapshot.snapshot_id())
            .await?;

        // Check if any input files were selected
        if input_file_scan_tasks.input_files_count() == 0 {
            return Ok((CompactionResult::default(), None));
        }

        let mut input_tasks_for_validation = if self.config.enable_validate_compaction {
            Some(input_file_scan_tasks.clone())
        } else {
            None
        };

        // Step 2: Create rewrite request
        let rewrite_files_request =
            self.create_rewrite_request(&table, input_file_scan_tasks.clone())?;

        // Step 3: Execute rewrite
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

        let commit_now = std::time::Instant::now();
        let output_data_files_for_commit = output_data_files.clone();

        // Step 4: Commit results (extensible)
        let committed_table = match self
            .commit_compaction_results(
                &table,
                output_data_files_for_commit,
                &input_file_scan_tasks,
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
        let compaction_validator = if self.config.enable_validate_compaction {
            Some(
                CompactionValidator::new(
                    input_tasks_for_validation.take().unwrap(),
                    output_data_files.clone(),
                    self.config.clone(),
                    table.metadata().current_schema().clone(),
                    table.metadata().current_schema().clone(),
                    committed_table.clone(),
                    self.catalog_name.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        Ok((
            CompactionResult {
                data_files: output_data_files,
                stats,
                table: Some(committed_table),
            },
            compaction_validator,
        ))
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

    // Template method pattern: These methods can be overridden for specific compaction types

    /// Hook for customizing file selection logic beyond simple `FileStrategy`
    /// Default implementation uses `FileStrategy`, but complex compaction types can override
    async fn select_files_for_compaction(
        &self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<InputFileScanTasks> {
        use crate::file_selection::FileStrategyFactory;

        let strategy =
            FileStrategyFactory::create_files_strategy(self.compaction_type, &self.config);
        FileSelector::get_scan_tasks_with_strategy(table, snapshot_id, strategy).await
    }

    /// Hook for customizing the rewrite request configuration
    /// Default implementation creates a standard request, but can be customized
    fn create_rewrite_request(
        &self,
        table: &Table,
        input_tasks: InputFileScanTasks,
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
            input_file_scan_tasks: input_tasks,
            config: self.config.clone(),
            dir_path: default_location_generator.dir_path,
            partition_spec: table.metadata().default_partition_spec().clone(),
            metrics_recorder: Some(metrics_recorder),
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
            starting_snapshot_id: snapshot.snapshot_id(),
            use_starting_sequence_number: true,
            basic_schema_id: table.metadata().current_schema().schema_id(),
        };

        let commit_manager = CommitManager::new(
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
            .rewrite_files(
                output_data_files,
                input_files,
                self.to_branch.as_deref().unwrap_or(MAIN_BRANCH),
            )
            .await
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn build_commit_manager(
        &self,
        consistency_params: CommitConsistencyParams,
    ) -> CommitManager {
        CommitManager::new(
            self.commit_retry_config.clone(),
            self.catalog.clone(),
            self.table_ident.clone(),
            self.catalog_name.clone(),
            self.metrics.clone(),
            consistency_params,
        )
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
pub struct CommitManagerRetryConfig {
    pub max_retries: u32, // This can be used to configure the backon strategy
    pub retry_initial_delay: Duration, // For exponential backoff
    pub retry_max_delay: Duration, // For exponential backoff
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

// Manages the commit process with retries
pub struct CommitManager {
    config: CommitManagerRetryConfig,
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
impl CommitManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: CommitManagerRetryConfig,
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
        to_branch: &str,
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

    pub async fn overwrite_files(
        &self,
        data_files: impl IntoIterator<Item = DataFile>,
        delete_files: impl IntoIterator<Item = DataFile>,
        to_branch: &str,
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

#[cfg(test)]
mod tests {
    use crate::compaction::CompactionBuilder;
    use crate::config::CompactionConfigBuilder;
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

        let rewrite_files_resp = CompactionBuilder::new()
            .with_catalog(Arc::new(catalog))
            .with_table_ident(table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default()
                    .enable_validate_compaction(true)
                    .build()
                    .unwrap(),
            ))
            .build()
            .await
            .unwrap()
            .compact()
            .await
            .unwrap();

        assert_eq!(rewrite_files_resp.stats.input_files_count, 2);
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
        let rewrite_files_resp = CompactionBuilder::new()
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
        assert_eq!(
            rewrite_files_resp.stats.input_files_count,
            initial_file_count
        );
        // Should create at least 1 new file from all the data (might be more depending on size)
        assert!(rewrite_files_resp.stats.output_files_count >= 1);
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
        let compaction = CompactionBuilder::new()
            .with_catalog(catalog_arc.clone())
            .with_table_ident(table_ident.clone())
            .with_compaction_type(super::CompactionType::MergeSmallDataFiles)
            .with_config(Arc::new(
                CompactionConfigBuilder::default()
                    .small_file_threshold(small_file_threshold)
                    .build()
                    .unwrap(),
            ))
            .build()
            .await
            .unwrap();

        // Get the files that would be selected for compaction
        let files_to_compact = compaction
            .select_files_for_compaction(&updated_table, snapshot_before.snapshot_id())
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
        let rewrite_files_resp = compaction.compact().await.unwrap();

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
}
