use bergloom_codegen::compactor::RewriteFilesStat;
use iceberg::spec::DataFile;
use iceberg::{Catalog, TableIdent};
use mixtrics::metrics::BoxedRegistry;
use mixtrics::registry::noop::NoopMetricsRegistry;

use crate::Result;
use crate::common::Metrics;
use crate::executor::{
    ExecutorType, InputFileScanTasks, RewriteFilesRequest, RewriteFilesResponse,
    create_compaction_executor,
};
use crate::{CompactionConfig, CompactionExecutor};
use futures_async_stream::for_await;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use std::collections::HashMap;
use std::sync::Arc;

use crate::executor::DataFusionExecutor;

pub enum CompactionType {
    Full,
}

/// Builder for creating Compaction instances with flexible configuration
pub struct CompactionBuilder {
    config: Option<Arc<CompactionConfig>>,
    executor_type: ExecutorType,
    catalog: Option<Arc<dyn Catalog>>,
    registry: BoxedRegistry,
    table_ident: Option<TableIdent>,
    compaction_type: Option<CompactionType>,
}

impl CompactionBuilder {
    /// Create a new CompactionBuilder with default settings
    pub fn new() -> Self {
        Self {
            config: None,
            executor_type: ExecutorType::DataFusion, // Default executor type
            catalog: None,
            registry: Box::new(NoopMetricsRegistry),
            table_ident: None,
            compaction_type: None,
        }
    }

    /// Set the compaction configuration
    pub fn with_config(mut self, config: Arc<CompactionConfig>) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the executor type (defaults to DataFusion)
    pub fn with_executor_type(mut self, executor_type: ExecutorType) -> Self {
        self.executor_type = executor_type;
        self
    }

    /// Set the catalog
    pub fn with_catalog(mut self, catalog: Arc<dyn Catalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Set the metrics registry (optional, defaults to NoopMetricsRegistry)
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

    /// Build the Compaction instance
    pub async fn build(self) -> Result<Compaction> {
        let config = self.config.ok_or_else(|| {
            crate::error::CompactionError::Execution("CompactionConfig is required".to_string())
        })?;

        let catalog = self.catalog.ok_or_else(|| {
            crate::error::CompactionError::Execution("Catalog is required".to_string())
        })?;

        let table_ident = self.table_ident.ok_or_else(|| {
            crate::error::CompactionError::Execution("TableIdent is required".to_string())
        })?;

        let compaction_type = self.compaction_type.unwrap_or(CompactionType::Full);

        if !catalog.table_exists(&table_ident).await? {
            return Err(crate::error::CompactionError::Execution(
                "Table does not exist".to_string(),
            ));
        }

        let executor = create_compaction_executor(self.executor_type);

        let metrics = Arc::new(Metrics::new(self.registry));

        Ok(Compaction {
            config,
            executor,
            catalog,
            metrics,
            table_ident,
            compaction_type,
        })
    }
}

impl Default for CompactionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Compaction {
    pub config: Arc<CompactionConfig>,
    pub executor: Box<dyn CompactionExecutor>,
    pub catalog: Arc<dyn Catalog>,
    pub metrics: Arc<Metrics>,
    pub table_ident: TableIdent,
    pub compaction_type: CompactionType,
}

impl Compaction {
    /// Create a new CompactionBuilder for flexible configuration
    pub fn builder() -> CompactionBuilder {
        CompactionBuilder::new()
    }

    pub async fn compact(&self) -> Result<RewriteFilesStat> {
        match self.compaction_type {
            CompactionType::Full => self.full_compact().await,
        }
    }

    async fn full_compact(&self) -> Result<RewriteFilesStat> {
        let table_lable: std::borrow::Cow<'static, str> = self.table_ident.to_string().into();
        let now = std::time::Instant::now();

        let table = self.catalog.load_table(&self.table_ident).await?;
        let (data_files, delete_files) = get_old_files_from_table(table.clone()).await?;
        let input_file_scan_tasks = get_tasks_from_table(table.clone()).await?;

        let file_io = table.file_io().clone();
        let schema = table.metadata().current_schema();
        let default_location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let rewrite_files_request = RewriteFilesRequest {
            file_io,
            schema: schema.clone(),
            input_file_scan_tasks,
            config: self.config.clone(),
            dir_path: default_location_generator.dir_path,
            partition_spec: table.metadata().default_partition_spec().clone(),
        };
        let RewriteFilesResponse {
            data_files: output_data_files,
            stat,
        } = DataFusionExecutor::default()
            .rewrite_files(rewrite_files_request)
            .await?;
        let txn = Transaction::new(&table);
        let mut rewrite_action = txn.rewrite_files(None, vec![])?;
        rewrite_action.add_data_files(output_data_files.clone())?;
        rewrite_action.delete_files(data_files)?;
        rewrite_action.delete_files(delete_files)?;
        let txn = rewrite_action.apply().await?;
        let commit_now = std::time::Instant::now();
        txn.commit(self.catalog.as_ref()).await?;
        self.metrics
            .compaction_commit_counter
            .counter(&[table_lable.clone()])
            .increase(1);

        self.metrics
            .compaction_commit_duration
            .histogram(&[table_lable.clone()])
            .record(commit_now.elapsed().as_secs_f64());

        self.metrics
            .compaction_duration
            .histogram(&[table_lable.clone()])
            .record(now.elapsed().as_secs_f64());

        self.metrics
            .compaction_rewritten_bytes
            .counter(&[table_lable.clone()])
            .increase(stat.rewritten_bytes);

        self.metrics
            .compaction_rewritten_files_count
            .counter(&[table_lable.clone()])
            .increase(stat.rewritten_files_count as u64);

        self.metrics
            .compaction_added_files_count
            .counter(&[table_lable.clone()])
            .increase(stat.added_files_count as u64);

        self.metrics
            .compaction_failed_data_files_count
            .counter(&[table_lable.clone()])
            .increase(stat.failed_data_files_count as u64);

        Ok(RewriteFilesStat {
            rewritten_files_count: stat.rewritten_files_count,
            added_files_count: stat.added_files_count,
            rewritten_bytes: stat.rewritten_bytes,
            failed_data_files_count: stat.failed_data_files_count,
        })
    }

    pub async fn expire_snapshot(&self, table_ident: TableIdent) -> Result<()> {
        let table = self.catalog.load_table(&table_ident).await?;
        let txn = Transaction::new(&table);
        let txn = txn.expire_snapshot().apply().await?;
        txn.commit(self.catalog.as_ref()).await?;
        Ok(())
    }
}

async fn get_old_files_from_table(table: Table) -> Result<(Vec<DataFile>, Vec<DataFile>)> {
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    let mut data_file = vec![];
    let mut delete_file = vec![];
    for manifest_file in manifest_list.entries() {
        let a = manifest_file.load_manifest(table.file_io()).await.unwrap();
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

async fn get_tasks_from_table(table: Table) -> Result<InputFileScanTasks> {
    let snapshot_id = table.metadata().current_snapshot_id().unwrap();

    let scan = table
        .scan()
        .snapshot_id(snapshot_id)
        .with_delete_file_processing_enabled(true)
        .build()?;
    let file_scan_stream = scan.plan_files().await?;

    let mut position_delete_files = HashMap::new();
    let mut data_files = vec![];
    let mut equality_delete_files = HashMap::new();

    #[for_await]
    for task in file_scan_stream {
        let task: FileScanTask = task?;
        match task.data_file_content {
            iceberg::spec::DataContentType::Data => {
                for delete_task in task.deletes.iter() {
                    match &delete_task.data_file_content {
                        iceberg::spec::DataContentType::PositionDeletes => {
                            let mut delete_task = delete_task.clone();
                            delete_task.project_field_ids = vec![];
                            position_delete_files
                                .insert(delete_task.data_file_path.clone(), delete_task);
                        }
                        iceberg::spec::DataContentType::EqualityDeletes => {
                            let mut delete_task = delete_task.clone();
                            delete_task.project_field_ids = delete_task.equality_ids.clone();
                            equality_delete_files
                                .insert(delete_task.data_file_path.clone(), delete_task);
                        }
                        _ => {
                            unreachable!()
                        }
                    }
                }
                data_files.push(task);
            }
            _ => {
                unreachable!()
            }
        }
    }
    Ok(InputFileScanTasks {
        data_files,
        position_delete_files: position_delete_files.into_values().collect(),
        equality_delete_files: equality_delete_files.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use iceberg::Catalog;
    use iceberg::{TableIdent, io::FileIOBuilder};
    use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogConfig};
    use std::sync::Arc;

    use crate::CompactionConfig;
    use crate::compaction::Compaction;

    async fn build_catalog() -> SqlCatalog {
        let sql_lite_uri = "postgresql://xxhx:123456@localhost:5432/demo_iceberg";
        let warehouse_location = "s3a://hummock001/iceberg-data".to_owned();
        let config = SqlCatalogConfig::builder()
            .uri(sql_lite_uri.to_owned())
            .name("demo1".to_owned())
            .warehouse_location(warehouse_location)
            .file_io(
                FileIOBuilder::new("s3a")
                    .with_prop("s3.secret-access-key", "hummockadmin")
                    .with_prop("s3.access-key-id", "hummockadmin")
                    .with_prop("s3.endpoint", "http://127.0.0.1:9301")
                    .with_prop("s3.region", "")
                    .build()
                    .unwrap(),
            )
            .sql_bind_style(SqlBindStyle::DollarNumeric)
            .build();
        SqlCatalog::new(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_compact() {
        let catalog: Arc<dyn Catalog> = Arc::new(build_catalog().await);
        let table_id = TableIdent::from_strs(vec!["demo_db", "test_all_delete"]).unwrap();
        let compaction_config = Arc::new(CompactionConfig {
            batch_parallelism: Some(4),
            target_partitions: Some(4),
            data_file_prefix: None,
        });

        // Using the builder pattern (recommended)
        let compaction = Compaction::builder()
            .with_config(compaction_config.clone())
            .with_catalog(catalog.clone())
            .with_executor_type(crate::executor::ExecutorType::DataFusion)
            .with_table_ident(table_id)
            .build()
            .await
            .unwrap();

        compaction.compact().await.unwrap();
    }
}
