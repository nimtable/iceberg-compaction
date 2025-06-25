/*
 * Copyright 2025 BergLoom
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

use crate::compaction::{self, get_tasks_from_table, Compaction};
use crate::config::CompactionConfigBuilder;
use crate::error::Result;
use crate::executor::datafusion::datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use crate::test_utils::{
    docker_compose::get_rest_catalog,
    generator::{FileGenerator, FileGeneratorConfig, WriterConfig},
};
use crate::{CompactionConfig, CompactionError};
use futures::future::try_join_all;
use futures::StreamExt;
use iceberg::{
    Catalog, NamespaceIdent, TableCreation,
    spec::{NestedField, PrimitiveType, Schema, Type},
    transaction::Transaction,
};
use tokio::task::JoinHandle;
use std::{collections::HashMap, sync::Arc};

pub mod docker_compose;
pub mod generator;

const TABLE_NAME: &str = "t1";
const NAMESPACE_NAME: &str = "namespace";

pub fn get_test_schema() -> Result<Schema> {
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::new(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
                false,
            )),
            Arc::new(NestedField::new(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
                false,
            )),
        ])
        .build()?;
    Ok(schema)
}

pub async fn build_test_iceberg_table(
    schema: Option<Schema>,
    file_generator_config: Option<FileGeneratorConfig>,
) -> Result<()> {
    let catalog = get_rest_catalog().await;
    let schema = schema.unwrap_or_else(|| get_test_schema().unwrap());
    let table_creation = TableCreation::builder()
        .name(TABLE_NAME.to_string())
        .schema(schema.clone())
        .build();
    let namespace_ident = NamespaceIdent::new(NAMESPACE_NAME.to_owned());
    catalog
        .create_namespace(&namespace_ident, HashMap::default())
        .await?;
    let table = catalog
        .create_table(&namespace_ident, table_creation)
        .await?;
    let writer_config = WriterConfig::new(&table);
    let file_generator_config = file_generator_config.unwrap_or_default();
    let mut file_generator =
        FileGenerator::new(file_generator_config, Arc::new(schema.clone()), writer_config)?;
    let commit_data_files = file_generator.generate().await?;
    let mut data_files = Vec::new();
    let mut position_delete_files = Vec::new();
    let mut equality_delete_files = Vec::new();
    for data_file in commit_data_files {
        match data_file.content_type() {
            iceberg::spec::DataContentType::Data => data_files.push(data_file),
            iceberg::spec::DataContentType::PositionDeletes => {
                position_delete_files.push(data_file)
            }
            iceberg::spec::DataContentType::EqualityDeletes => {
                equality_delete_files.push(data_file)
            }
        }
    }
    println!("data_files: {:?}", data_files.len());
    println!("position_delete_files: {:?}", position_delete_files.len());
    println!("equality_delete_files: {:?}", equality_delete_files.len());
    let txn = Transaction::new(&table);
    let mut fast_append_action = txn.fast_append(Some(rand::random::<i64>()), None, vec![])?;
    fast_append_action
        .add_data_files(data_files)?
        .add_data_files(position_delete_files)?;
    let table = fast_append_action.apply().await?.commit(&catalog).await?;

    let snapshot = table.metadata().current_snapshot().unwrap();
    let txn = Transaction::new(&table);
    let mut fast_append_action = txn.fast_append(Some(snapshot.snapshot_id() + 1), None, vec![])?;
    fast_append_action.add_data_files(equality_delete_files)?;
    let table = fast_append_action.apply().await?.commit(&catalog).await?;

    let config = Arc::new(CompactionConfigBuilder::default()
        .batch_parallelism(16)
        .target_partitions(1)
        .build()
        .unwrap());
    let timer = std::time::Instant::now();
    let file_io = table.file_io().clone();
    let mut input_file_scan_tasks = Some(get_tasks_from_table(table.clone()).await?).unwrap();
    let mut datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(Arc::new(schema))
            .with_input_data_files(input_file_scan_tasks)
            .build()?;
    datafusion_task_ctx.exec_sql = format!("select * from {}", datafusion_task_ctx.data_file_table_name());
    let mut futures = Vec::new();
    let (batches, input_schema) = DatafusionProcessor::new(config.clone(), file_io.clone())
            .execute(datafusion_task_ctx)
            .await?;
        for mut batch in batches {
            let future: JoinHandle<
                std::result::Result<(), CompactionError>,
            > = tokio::spawn(async move {
                // 拉取batch阶段
                while let Some(b) = batch.as_mut().next().await {
                    let batch_data = b?;
                }
                
                Ok(())
            });
            futures.push(future);
        }
        try_join_all(futures).await.unwrap();
    let duration = timer.elapsed();
    println!("duration: {:?}", duration);
    Ok(())
}

mod tests {
    use super::*;

    #[tokio::test]
    async fn test_build_test_iceberg_table() {
        build_test_iceberg_table(None, Some(FileGeneratorConfig::default())).await.unwrap();
    }
}
