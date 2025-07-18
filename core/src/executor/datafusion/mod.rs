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

use crate::{error::Result, executor::iceberg_writer::rolling_iceberg_writer, CompactionConfig};
use async_trait::async_trait;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::{future::try_join_all, StreamExt};
use iceberg::{
    io::FileIO,
    spec::{DataFile, PartitionSpec, Schema},
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            ParquetWriterBuilder,
        },
        function_writer::fanout_partition_writer::FanoutPartitionWriterBuilder,
        IcebergWriter, IcebergWriterBuilder,
    },
};
use parquet::file::properties::WriterProperties;
use sqlx::types::Uuid;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;

use crate::CompactionError;

use super::{CompactionExecutor, RewriteFilesStat};
pub mod datafusion_processor;
use super::{RewriteFilesRequest, RewriteFilesResponse};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;

#[derive(Debug, Default)]
pub struct DataFusionExecutor {}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn rewrite_files(&self, request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        let RewriteFilesRequest {
            file_io,
            schema,
            input_file_scan_tasks,
            config,
            dir_path,
            partition_spec,
            metrics_recorder,
            runtime_config,
        } = request;

        let mut stats = RewriteFilesStat::default();
        stats.record_input(&input_file_scan_tasks);

        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema.clone())
            .with_input_data_files(input_file_scan_tasks)
            .build()?;
        let (batches, input_schema) =
            DatafusionProcessor::new(config.clone(), runtime_config.clone(), file_io.clone())
                .execute(datafusion_task_ctx)
                .await?;
        let arc_input_schema = Arc::new(input_schema);
        let mut futures = Vec::with_capacity(runtime_config.executor_parallelism);

        // build iceberg writer for each partition
        for mut batch_stream in batches {
            let dir_path = dir_path.clone();
            let schema = arc_input_schema.clone();
            let config = config.clone();
            let file_io = file_io.clone();
            let partition_spec = partition_spec.clone();
            let metrics_recorder = metrics_recorder.clone();

            let future: JoinHandle<
                std::result::Result<Vec<iceberg::spec::DataFile>, CompactionError>,
            > = tokio::spawn(async move {
                let mut data_file_writer = build_iceberg_data_file_writer(
                    config.execution.data_file_prefix.clone(),
                    dir_path,
                    schema,
                    file_io,
                    partition_spec,
                    config.clone(),
                )
                .await?;

                // Process each record batch with metrics
                let mut fetch_batch_start = Instant::now();
                while let Some(batch_result) = batch_stream.as_mut().next().await {
                    if let Some(metrics_recorder) = &metrics_recorder {
                        metrics_recorder.record_datafusion_batch_fetch_duration(
                            fetch_batch_start.elapsed().as_millis() as f64,
                        );
                    }

                    let batch = batch_result?;

                    let record_count = batch.num_rows() as u64;
                    let batch_bytes = batch.get_array_memory_size() as u64;

                    // Write the batch
                    let write_start = Instant::now();
                    data_file_writer.write(batch).await?;
                    if let Some(metrics_recorder) = &metrics_recorder {
                        metrics_recorder.record_datafusion_batch_write_duration(
                            write_start.elapsed().as_millis() as f64,
                        );
                    }

                    // Record detailed batch stats
                    if let Some(metrics_recorder) = &metrics_recorder {
                        metrics_recorder.record_batch_stats(record_count, batch_bytes);
                    }

                    fetch_batch_start = Instant::now(); // Reset for next batch
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

        stats.record_output(&output_data_files);

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stats,
        })
    }
}

pub async fn build_iceberg_data_file_writer(
    data_file_prefix: String,
    dir_path: String,
    schema: Arc<Schema>,
    file_io: FileIO,
    partition_spec: Arc<PartitionSpec>,
    config: Arc<CompactionConfig>,
) -> Result<Box<dyn IcebergWriter>> {
    let parquet_writer_builder = build_parquet_writer_builder(
        data_file_prefix,
        dir_path,
        schema.clone(),
        file_io,
        config.execution.write_parquet_properties.clone(),
    )?;
    let data_file_builder =
        DataFileWriterBuilder::new(parquet_writer_builder, None, partition_spec.spec_id());
    let data_file_size_writer =
        rolling_iceberg_writer::RollingIcebergWriterBuilder::new(data_file_builder)
            .with_target_file_size(config.execution.target_file_size)
            .with_max_concurrent_closes(config.execution.max_concurrent_closes)
            .with_dynamic_size_estimation(config.execution.enable_dynamic_size_estimation)
            .with_size_estimation_smoothing_factor(
                config.execution.size_estimation_smoothing_factor,
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

pub fn build_parquet_writer_builder(
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
