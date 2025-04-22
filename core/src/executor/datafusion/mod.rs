use crate::error::Result;
use ::datafusion::{
    parquet::file::properties::WriterProperties,
    prelude::{SessionConfig, SessionContext},
};
use async_trait::async_trait;
use file_scan_task_table_provider::IcebergFileScanTaskTableProvider;
use futures::{StreamExt, future::try_join_all};
use iceberg::{
    arrow::schema_to_arrow_schema,
    io::FileIO,
    scan::FileScanTask,
    spec::{DataFile, NestedField, PrimitiveType, Schema, Type},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder},
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
    },
};
use sqlx::types::Uuid;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::CompactionError;

use super::{
    CompactionExecutor, InputFileScanTasks, RewriteFilesRequest, RewriteFilesResponse,
    RewriteFilesStat,
};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;
pub mod sql_builder;

const SEQ_NUM: &str = "seq_num";
const FILE_PATH: &str = "file_path";
const POS: &str = "pos";
const DATA_FILE_TABLE: &str = "data_file_table";
const POSITION_DELETE_TABLE: &str = "position_delete_table";
const EQUALITY_DELETE_TABLE: &str = "equality_delete_table";
const DEFAULT_PREFIX: &str = "10";

pub struct DataFusionExecutor {}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn rewrite_files(request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        let RewriteFilesRequest {
            file_io,
            schema,
            input_file_scan_tasks,
            config,
            dir_path,
        } = request;
        let batch_parallelism = config.batch_parallelism.unwrap_or(4);
        let target_partitions = config.target_partitions.unwrap_or(4);
        let data_file_prefix = config
            .data_file_prefix
            .clone()
            .unwrap_or(DEFAULT_PREFIX.to_owned());
        let mut session_config = SessionConfig::new();
        session_config = session_config.with_target_partitions(target_partitions);
        let ctx = SessionContext::new_with_config(session_config);

        let mut stat = RewriteFilesStat::default();
        let rewritten_files_count = input_file_scan_tasks.input_files_count();

        let InputFileScanTasks {
            data_files,
            position_delete_files,
            equality_delete_files,
        } = input_file_scan_tasks;

        let mut data_fusion_task_context = DataFusionTaskContext::builder()?
            .with_schema(schema)
            .with_datafile(data_files)
            .with_position_delete_files(position_delete_files)
            .with_equality_delete_files(equality_delete_files)
            .build()?;

        let need_seq_num = data_fusion_task_context.need_seq_num();
        let need_file_path_and_pos = data_fusion_task_context.need_file_path_and_pos();

        let data_file_schema = data_fusion_task_context.data_file_schema.take().unwrap();
        let input_schema = data_fusion_task_context.input_schema.take().unwrap();

        // register data file table provider
        Self::register_data_table_provider(
            &data_file_schema,
            data_fusion_task_context.data_files.take().unwrap(),
            file_io.clone(),
            &ctx,
            DATA_FILE_TABLE,
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        )?;

        if let Some(position_delete_schema) = data_fusion_task_context.position_delete_schema.take()
        {
            Self::register_delete_table_provider(
                &position_delete_schema,
                data_fusion_task_context
                    .position_delete_files
                    .take()
                    .unwrap(),
                file_io.clone(),
                &ctx,
                POSITION_DELETE_TABLE,
                batch_parallelism,
            )?;
        }

        if let Some(equality_delete_schema) = data_fusion_task_context.equality_delete_schema.take()
        {
            Self::register_delete_table_provider(
                &equality_delete_schema,
                data_fusion_task_context
                    .equality_delete_files
                    .take()
                    .unwrap(),
                file_io.clone(),
                &ctx,
                EQUALITY_DELETE_TABLE,
                batch_parallelism,
            )?;
        }

        let arc_input_schema = Arc::new(input_schema);

        let df = ctx.sql(&data_fusion_task_context.merge_on_read_sql).await?;
        let batchs = df.execute_stream_partitioned().await?;
        let mut futures = Vec::with_capacity(batch_parallelism);
        // build iceberg writer for each partition
        for mut batch in batchs {
            let dir_path = dir_path.clone();
            let schema = arc_input_schema.clone();
            let data_file_prefix = data_file_prefix.clone();
            let file_io = file_io.clone();
            let future: JoinHandle<Result<Vec<iceberg::spec::DataFile>>> =
                tokio::spawn(async move {
                    let mut data_file_writer =
                        Self::build_iceberg_writer(data_file_prefix, dir_path, schema, file_io)
                            .await?;
                    while let Some(b) = batch.as_mut().next().await {
                        data_file_writer.write(b?).await?;
                    }
                    let data_files = data_file_writer.close().await?;
                    Ok(data_files)
                });
            futures.push(future);
        }
        // collect all data files from all partitions
        let output_data_files: Vec<DataFile> = try_join_all(futures)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .into_iter()
            .map(|res| res.map(|v| v.into_iter()))
            .collect::<Result<Vec<_>>>()
            .map(|iters| iters.into_iter().flatten().collect())?;

        stat.added_files_count = output_data_files.len() as u32;
        stat.rewritten_bytes = output_data_files
            .iter()
            .map(|f| f.file_size_in_bytes())
            .sum();
        stat.rewritten_files_count = rewritten_files_count;

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stat,
        })
    }
}

impl DataFusionExecutor {
    #[allow(clippy::too_many_arguments)]
    fn register_data_table_provider(
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        file_io: FileIO,
        ctx: &SessionContext,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Result<()> {
        Self::register_table_provider_impl(
            schema,
            file_scan_tasks,
            file_io,
            ctx,
            table_name,
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        )
    }

    fn register_delete_table_provider(
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        file_io: FileIO,
        ctx: &SessionContext,
        table_name: &str,
        batch_parallelism: usize,
    ) -> Result<()> {
        Self::register_table_provider_impl(
            schema,
            file_scan_tasks,
            file_io,
            ctx,
            table_name,
            false,
            false,
            batch_parallelism,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn register_table_provider_impl(
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        file_io: FileIO,
        ctx: &SessionContext,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Result<()> {
        let schema = schema_to_arrow_schema(schema)?;
        let data_file_table_provider = IcebergFileScanTaskTableProvider::new(
            file_scan_tasks,
            Arc::new(schema),
            file_io,
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        );

        ctx.register_table(table_name, Arc::new(data_file_table_provider))
            .unwrap();
        Ok(())
    }

    async fn build_iceberg_writer(
        data_file_prefix: String,
        dir_path: String,
        schema: Arc<Schema>,
        file_io: FileIO,
    ) -> Result<
        DataFileWriter<ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>>,
    > {
        let location_generator = DefaultLocationGenerator { dir_path };
        let unique_uuid_suffix = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(
            data_file_prefix,
            Some(unique_uuid_suffix.to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            schema,
            file_io,
            location_generator,
            file_name_generator,
        );
        let data_file_writer = DataFileWriterBuilder::new(parquet_writer_builder, None, 0)
            .build()
            .await
            .unwrap();
        Ok(data_file_writer)
    }
}

struct DataFusionTaskContext {
    pub(crate) data_file_schema: Option<Schema>,
    pub(crate) input_schema: Option<Schema>,
    pub(crate) data_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_files: Option<Vec<FileScanTask>>,
    pub(crate) equality_delete_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_schema: Option<Schema>,
    pub(crate) equality_delete_schema: Option<Schema>,
    pub(crate) merge_on_read_sql: String,
}

struct DataFusionTaskContextBuilder {
    schema: Arc<Schema>,
    data_files: Vec<FileScanTask>,
    position_delete_files: Vec<FileScanTask>,
    equality_delete_files: Vec<FileScanTask>,
}

impl DataFusionTaskContextBuilder {
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = schema;
        self
    }

    pub fn with_datafile(mut self, data_files: Vec<FileScanTask>) -> Self {
        self.data_files = data_files;
        self
    }

    pub fn with_position_delete_files(mut self, position_delete_files: Vec<FileScanTask>) -> Self {
        self.position_delete_files = position_delete_files;
        self
    }

    pub fn with_equality_delete_files(mut self, equality_delete_files: Vec<FileScanTask>) -> Self {
        self.equality_delete_files = equality_delete_files;
        self
    }

    // build data fusion task context
    pub fn build(self) -> Result<DataFusionTaskContext> {
        let highest_field_id = self.schema.highest_field_id();
        // Build scheam for position delete file, file_path + pos
        let position_delete_schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::new(
                    1,
                    FILE_PATH,
                    Type::Primitive(PrimitiveType::String),
                    true,
                )),
                Arc::new(NestedField::new(
                    2,
                    POS,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )),
            ])
            .build()?;
        // Build schema for equality delete file, equality_ids + seq_num
        let mut equality_ids: Option<Vec<i32>> = None;
        for i in &self.equality_delete_files {
            if let Some(ids) = equality_ids.as_ref() {
                if ids.eq(&i.equality_ids) {
                    continue;
                } else {
                    return Err(CompactionError::Config("equality_ids not equal".to_owned()));
                }
            } else {
                equality_ids = Some(i.equality_ids.clone());
            }
        }
        let mut equality_delete_vec = vec![];
        if let Some(ids) = equality_ids.as_ref() {
            for i in ids {
                let field = self
                    .schema
                    .field_by_id(*i)
                    .ok_or_else(|| CompactionError::Config("equality_ids not found".to_owned()))?;
                equality_delete_vec.push(field.clone());
            }
        }
        let equality_join_names: Vec<_> =
            equality_delete_vec.iter().map(|i| i.name.clone()).collect();

        if !equality_delete_vec.is_empty() {
            equality_delete_vec.push(Arc::new(NestedField::new(
                highest_field_id + 1,
                SEQ_NUM,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        let equality_delete_schema = Schema::builder().with_fields(equality_delete_vec).build()?;
        let need_file_path_and_pos = !self.position_delete_files.is_empty();
        let need_seq_num = !equality_join_names.is_empty();

        // Build schema for data file, old schema + seq_num + file_path + pos
        let project_names: Vec<_> = self
            .schema
            .as_struct()
            .fields()
            .iter()
            .map(|i| i.name.clone())
            .collect();
        let highest_field_id = self.schema.highest_field_id();
        let mut add_schema_fields = vec![];
        // add sequence number column if needed
        if need_seq_num {
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 1,
                SEQ_NUM,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        // add file path and position column if needed
        if need_file_path_and_pos {
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 2,
                FILE_PATH,
                Type::Primitive(PrimitiveType::String),
                true,
            )));
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 3,
                POS,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        // data file schema is old schema + seq_num + file_path + pos. used for data file table provider
        let data_file_schema = self
            .schema
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(add_schema_fields)
            .build()?;
        // input schema is old schema. used for data file writer
        let input_schema = self.schema.as_ref().clone();

        let sql_builder = sql_builder::SqlBuilder::new(
            &project_names,
            &self.position_delete_files,
            &self.equality_delete_files,
            &equality_join_names,
            need_seq_num,
            need_file_path_and_pos,
        );
        let merge_on_read_sql = sql_builder.build_merge_on_read_sql();

        Ok(DataFusionTaskContext {
            data_file_schema: Some(data_file_schema),
            input_schema: Some(input_schema),
            data_files: Some(self.data_files),
            position_delete_files: Some(self.position_delete_files),
            equality_delete_files: Some(self.equality_delete_files),
            position_delete_schema: if need_file_path_and_pos {
                Some(position_delete_schema)
            } else {
                None
            },
            equality_delete_schema: if need_seq_num {
                Some(equality_delete_schema)
            } else {
                None
            },
            merge_on_read_sql,
        })
    }
}

impl DataFusionTaskContext {
    pub fn builder() -> Result<DataFusionTaskContextBuilder> {
        Ok(DataFusionTaskContextBuilder {
            schema: Arc::new(Schema::builder().build()?),
            data_files: vec![],
            position_delete_files: vec![],
            equality_delete_files: vec![],
        })
    }

    pub fn need_file_path_and_pos(&self) -> bool {
        self.position_delete_files
            .as_ref()
            .is_some_and(|v| !v.is_empty())
    }

    pub fn need_seq_num(&self) -> bool {
        self.equality_delete_files
            .as_ref()
            .is_some_and(|v| !v.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use futures_async_stream::for_await;
    use iceberg::Catalog;
    use iceberg::scan::FileScanTask;
    use iceberg::table::Table;
    use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
    use iceberg::{TableIdent, io::FileIOBuilder, transaction::Transaction};
    use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogConfig};
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::CompactionError;
    use crate::executor::{InputFileScanTasks, RewriteFilesRequest, RewriteFilesResponse};
    use crate::{CompactionConfig, CompactionExecutor, executor::DataFusionExecutor};

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

    async fn get_tasks_from_table(table: Table) -> Result<InputFileScanTasks, CompactionError> {
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

    #[tokio::test]
    async fn test_compact() {
        let catalog = build_catalog().await;
        let table_id = TableIdent::from_strs(vec!["demo_db", "test_all_delete"]).unwrap();
        let table = catalog.load_table(&table_id).await.unwrap();

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
        let all_file_scan_tasks = get_tasks_from_table(table.clone()).await.unwrap();
        let file_io = table.file_io().clone();
        let schema = table.metadata().current_schema();
        let default_location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let request = RewriteFilesRequest {
            file_io: file_io,
            schema: schema.clone(),
            input_file_scan_tasks: all_file_scan_tasks,
            config: Arc::new(CompactionConfig {
                batch_parallelism: Some(4),
                target_partitions: Some(4),
                data_file_prefix: None,
            }),
            dir_path: default_location_generator.dir_path,
        };
        let RewriteFilesResponse {
            data_files: output_data_files,
            stat: _,
        } = DataFusionExecutor::rewrite_files(request).await.unwrap();
        let txn = Transaction::new(&table);
        let mut rewrite_action = txn.rewrite_files(None, vec![]).unwrap();
        rewrite_action
            .add_data_files(output_data_files.clone())
            .unwrap();
        rewrite_action.delete_files(data_file).unwrap();
        rewrite_action.delete_files(delete_file).unwrap();
        let tx = rewrite_action.apply().await.unwrap();
        tx.commit(&catalog).await.unwrap();
    }
}
