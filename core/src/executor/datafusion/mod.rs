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

use crate::{error::Result, executor::iceberg_writer::rolling_iceberg_writer};
use ::datafusion::parquet::file::properties::WriterProperties;
use async_trait::async_trait;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::{StreamExt, future::try_join_all};
use iceberg::{
    io::FileIO,
    spec::{DataFile, PartitionSpec, Schema},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
        function_writer::fanout_partition_writer::FanoutPartitionWriterBuilder,
    },
};
use sqlx::types::Uuid;
use std::sync::Arc;
use tokio::task::JoinHandle;
use std::time::Instant;

use crate::CompactionError;

use super::{CompactionExecutor, RewriteFilesStat};
pub mod datafusion_processor;
use super::{RewriteFilesRequest, RewriteFilesResponse};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;

#[derive(Default)]
pub struct DataFusionExecutor {}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn rewrite_files(&self, request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        let total_start = Instant::now();
        
        let RewriteFilesRequest {
            file_io,
            schema,
            input_file_scan_tasks,
            config,
            dir_path,
            partition_spec,
        } = request;

        let mut stat = RewriteFilesStat::default();
        let rewritten_files_count = input_file_scan_tasks.input_files_count();

        // 读文件阶段
        let read_start = Instant::now();
        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema)
            .with_input_data_files(input_file_scan_tasks)
            .build()?;
        let read_duration = read_start.elapsed();

        // 执行阶段 - 只统计计划构建时间，实际执行在异步任务中
        let execute_start = Instant::now();
        let (batches, input_schema) = DatafusionProcessor::new(config.clone(), file_io.clone())
            .execute(datafusion_task_ctx)
            .await?;
        let execute_plan_duration = execute_start.elapsed();

        // 写出阶段 - 构建写入器
        let write_build_start = Instant::now();
        let arc_input_schema = Arc::new(input_schema);
        let mut futures = Vec::with_capacity(config.batch_parallelism);
        
        // build iceberg writer for each partition
        for mut batch in batches {
            let dir_path = dir_path.clone();
            let schema = arc_input_schema.clone();
            let config = config.clone();
            let file_io = file_io.clone();
            let partition_spec = partition_spec.clone();
            let future: JoinHandle<
                std::result::Result<(Vec<iceberg::spec::DataFile>, std::time::Duration, std::time::Duration, std::time::Duration), CompactionError>,
            > = tokio::spawn(async move {
                let task_start = Instant::now();
                
                let mut data_file_writer = build_iceberg_data_file_writer(
                    config.data_file_prefix.clone(),
                    dir_path,
                    schema,
                    file_io,
                    partition_spec,
                    config.target_file_size,
                    config.write_parquet_properties.clone(),
                )
                .await?;
                
                // 拉取batch阶段
                let mut total_pull_time = std::time::Duration::ZERO;
                let mut total_write_time = std::time::Duration::ZERO;
                
                let mut pull_start = Instant::now();
                while let Some(b) = batch.as_mut().next().await {
                    let batch_data = b?;
                    total_pull_time += pull_start.elapsed();
                    
                    let write_start = Instant::now();
                    data_file_writer.write(batch_data).await?;
                    let write_duration = write_start.elapsed();
                    total_write_time += write_duration;
                    pull_start = Instant::now();
                }
                
                let close_start = Instant::now();
                let data_files = data_file_writer.close().await?;
                let close_duration = close_start.elapsed();
                
                let task_duration = task_start.elapsed();
                
                Ok((data_files, total_pull_time, total_write_time, close_duration))
            });
            futures.push(future);
        }
        
        let write_build_duration = write_build_start.elapsed();
        
        // 等待所有异步任务完成并收集统计信息
        let write_wait_start = Instant::now();
        let results = try_join_all(futures)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?;
        let write_wait_duration = write_wait_start.elapsed();
        
        // 收集所有数据文件和统计信息
        let mut output_data_files = Vec::new();
        let mut total_pull_time = std::time::Duration::ZERO;
        let mut total_write_time = std::time::Duration::ZERO;
        let mut total_close_time = std::time::Duration::ZERO;
        
        for result in results {
            let (data_files, pull_duration, write_duration, close_duration) = result?;
            output_data_files.extend(data_files);
            total_pull_time += pull_duration;
            total_write_time += write_duration;
            total_close_time += close_duration;
        }

        stat.added_files_count = output_data_files.len() as u32;
        stat.rewritten_bytes = output_data_files
            .iter()
            .map(|f| f.file_size_in_bytes())
            .sum();
        stat.rewritten_files_count = rewritten_files_count;

        let total_duration = total_start.elapsed();
        println!("[PERF] 读文件: {:?} ({:.1}%) | 执行计划: {:?} ({:.1}%) | 拉取batch: {:?} ({:.1}%) | 写入batch: {:?} ({:.1}%) | 写入构建: {:?} ({:.1}%) | 写入等待: {:?} ({:.1}%) | 实际关闭: {:?} ({:.1}%) | 总计: {:?}", 
            read_duration, read_duration.as_millis() as f64 / total_duration.as_millis() as f64 * 100.0,
            execute_plan_duration, execute_plan_duration.as_millis() as f64 / total_duration.as_millis() as f64 * 100.0,
            total_pull_time, total_pull_time.as_millis() as f64 / total_duration.as_millis() as f64 * 100.0,
            total_write_time, total_write_time.as_millis() as f64 / total_duration.as_millis() as f64 * 100.0,
            write_build_duration, write_build_duration.as_millis() as f64 / total_duration.as_millis() as f64 * 100.0,
            write_wait_duration, write_wait_duration.as_millis() as f64 / total_duration.as_millis() as f64 * 100.0,
            total_close_time, total_close_time.as_millis() as f64 / total_duration.as_millis() as f64 * 100.0,
            total_duration);

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stat,
        })
    }
}

pub async fn build_iceberg_data_file_writer(
    data_file_prefix: String,
    dir_path: String,
    schema: Arc<Schema>,
    file_io: FileIO,
    partition_spec: Arc<PartitionSpec>,
    target_file_size: u64,
    write_parquet_properties: WriterProperties,
) -> Result<Box<dyn IcebergWriter>> {
    let parquet_writer_builder = build_parquet_writer_builder(
        data_file_prefix,
        dir_path,
        schema.clone(),
        file_io,
        write_parquet_properties,
    )
    .await?;
    let data_file_builder =
        DataFileWriterBuilder::new(parquet_writer_builder, None, partition_spec.spec_id());
    let data_file_size_writer = rolling_iceberg_writer::RollingIcebergWriterBuilder::new(
        data_file_builder,
        target_file_size,
    );
    let iceberg_output_writer = if partition_spec.fields().is_empty() {
        Box::new(data_file_size_writer.build().await?) as Box<dyn IcebergWriter>
    } else {
        Box::new(
            FanoutPartitionWriterBuilder::new(
                data_file_size_writer,
                partition_spec.clone(),
                schema,
            )?
            .build()
            .await?,
        ) as Box<dyn IcebergWriter>
    };
    Ok(iceberg_output_writer)
}

pub async fn build_parquet_writer_builder(
    data_file_prefix: String,
    dir_path: String,
    schema: Arc<Schema>,
    file_io: FileIO,
    write_parquet_properties: WriterProperties,
) -> Result<ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>> {
    let location_generator = DefaultLocationGenerator { dir_path };
    let unique_uuid_suffix = Uuid::now_v7();
    let file_name_generator = DefaultFileNameGenerator::new(
        data_file_prefix,
        Some(unique_uuid_suffix.to_string()),
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        write_parquet_properties,
        schema.clone(),
        file_io,
        location_generator,
        file_name_generator,
    );
    Ok(parquet_writer_builder)
}
