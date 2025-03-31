use ::datafusion::{
    parquet::file::properties::WriterProperties,
    prelude::{SessionConfig, SessionContext},
};
use async_trait::async_trait;
use file_scan_task_table_provider::IcebergFileScanTaskTableProvider;
use futures::{StreamExt, future::try_join_all};
use iceberg::{
    arrow::schema_to_arrow_schema,
    scan::FileScanTask,
    spec::{NestedField, PrimitiveType, Schema, Type},
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
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;

const SEQ_NUM: &str = "seq_num";
const FILE_PATH: &str = "file_path";
const POS: &str = "pos";

use super::*;

pub struct DataFusionExecutor {
    // ctx: SessionContext,
}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn compact(
        file_io: FileIO,
        schema: Arc<Schema>,
        input_file_scan_tasks: AllFileScanTasks,
        config: Arc<CompactionConfig>,
        dir_path: String,
    ) -> Result<Vec<DataFile>, CompactionError> {
        let batch_parallelism = config.batch_parallelism.unwrap_or(1);
        let mut config = SessionConfig::new();
        config = config.with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);

        let AllFileScanTasks {
            data_files,
            position_delete_files,
            equality_delete_files,
        } = input_file_scan_tasks;
        let highest_field_id = schema.highest_field_id();
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
        let mut equlity_ids: Option<Vec<i32>> = None;
        for i in &equality_delete_files {
            if let Some(ids) = equlity_ids.as_ref() {
                if ids.eq(&i.equality_ids) {
                    continue;
                } else {
                    return Err(CompactionError::Config(
                        "equality_ids not equal".to_string(),
                    ));
                }
            } else {
                equlity_ids = Some(i.equality_ids.clone());
            }
        }
        let mut equality_delete_vec = vec![];
        if let Some(ids) = equlity_ids.as_ref() {
            for i in ids {
                let field = schema
                    .field_by_id(*i)
                    .ok_or_else(|| CompactionError::Config("equality_ids not found".to_string()))?;
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
        let need_file_path_and_pos = !position_delete_files.is_empty();
        let need_seq_num = !equality_join_names.is_empty();

        // Build schema for data file, old schema + seq_num + file_path + pos
        let project_names: Vec<_> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|i| i.name.clone())
            .collect();
        let highest_field_id = schema.highest_field_id();
        let mut add_schema_fields = vec![];
        if need_seq_num {
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 1,
                SEQ_NUM,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
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
        let data_file_schema = schema
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(add_schema_fields)
            .build()?;

        Self::register_table_provider(
            data_file_schema,
            data_files,
            &file_io,
            &ctx,
            "test_all_delete_data_file",
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        )?;

        let mut merge_on_read_sql = format!(
            "select {} from test_all_delete_data_file",
            project_names.join(",")
        );
        if need_file_path_and_pos {
            Self::register_table_provider(
                position_delete_schema,
                position_delete_files,
                &file_io,
                &ctx,
                "test_all_delete_position_delete",
                false,
                false,
                batch_parallelism,
            )?;
            merge_on_read_sql.push_str(" left anti join test_all_delete_position_delete on test_all_delete_data_file.file_path = test_all_delete_position_delete.file_path and test_all_delete_data_file.pos = test_all_delete_position_delete.pos");
        }
        if need_seq_num {
            Self::register_table_provider(
                equality_delete_schema,
                equality_delete_files,
                &file_io,
                &ctx,
                "test_all_delete_equality_delete",
                false,
                false,
                batch_parallelism,
            )?;
            merge_on_read_sql.push_str(format!(
                " left anti join test_all_delete_equality_delete on {} and test_all_delete_data_file.seq_num < test_all_delete_equality_delete.seq_num",
                equality_join_names.iter().map(|i| format!("test_all_delete_data_file.{} = test_all_delete_equality_delete.{}",i,i)).collect::<Vec<String>>().join(" and "),
            ).as_str());
        }

        let df = ctx.sql(merge_on_read_sql.as_str()).await?;
        let batchs = df.execute_stream_partitioned().await?;
        let mut futures = Vec::with_capacity(batch_parallelism);
        for mut batch in batchs {
            let dir_path = dir_path.clone();
            let schema = schema.clone();
            let file_io = file_io.clone();
            let future: JoinHandle<
                std::result::Result<Vec<iceberg::spec::DataFile>, CompactionError>,
            > = tokio::spawn(async move {
                let mut data_file_writer =
                    Self::build_iceberg_writer(dir_path, schema, file_io).await?;
                while let Some(b) = batch.as_mut().next().await {
                    data_file_writer.write(b?).await?;
                }
                let data_files = data_file_writer.close().await?;
                Ok(data_files)
            });
            futures.push(future);
        }

        let all_data_files = try_join_all(futures)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .into_iter()
            .map(|res| res.map(|v| v.into_iter())) // 转换每个 Result<Vec<T>> 为 Result<迭代器>
            .collect::<Result<Vec<_>, _>>()
            .map(|iters| iters.into_iter().flatten().collect())?;
        Ok(all_data_files)
    }
}

impl DataFusionExecutor {
    #[allow(clippy::too_many_arguments)]
    fn register_table_provider(
        schema: Schema,
        file_scan_tasks: Vec<FileScanTask>,
        file_io: &FileIO,
        ctx: &SessionContext,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Result<(), CompactionError> {
        let schema = schema_to_arrow_schema(&schema)?;
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
        dir_path: String,
        schema: Arc<Schema>,
        file_io: FileIO,
    ) -> Result<
        DataFileWriter<ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>>,
        CompactionError,
    > {
        let location_generator = DefaultLocationGenerator { dir_path };
        let unique_uuid_suffix = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(
            "1".to_string(),
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
    use crate::executor::AllFileScanTasks;
    use crate::{CompactionConfig, CompactionExecutor, executor::DataFusionExecutor};

    async fn build_catalog() -> SqlCatalog {
        let sql_lite_uri = "postgresql://xxhx:123456@localhost:5432/demo_iceberg";
        let warehouse_location = "s3a://hummock001/iceberg-data".to_owned();
        let config = SqlCatalogConfig::builder()
            .uri(sql_lite_uri.to_string())
            .name("demo1".to_string())
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
        let catalog = SqlCatalog::new(config).await.unwrap();
        catalog
    }

    async fn get_tasks_from_table(table: Table) -> Result<AllFileScanTasks, CompactionError> {
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
        Ok(AllFileScanTasks {
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
        let new_data_files = DataFusionExecutor::compact(
            file_io,
            schema.clone(),
            all_file_scan_tasks,
            Arc::new(CompactionConfig {
                batch_parallelism: Some(4),
            }),
            default_location_generator.dir_path,
        )
        .await
        .unwrap();
        let txn = Transaction::new(&table);
        let mut rewrite_action = txn.rewrite_files(None, vec![]).unwrap();
        rewrite_action
            .add_data_files(new_data_files.clone())
            .unwrap();
        rewrite_action.delete_files(data_file).unwrap();
        rewrite_action.delete_files(delete_file).unwrap();
        let tx = rewrite_action.apply().await.unwrap();
        tx.commit(&catalog).await.unwrap();
    }
}
