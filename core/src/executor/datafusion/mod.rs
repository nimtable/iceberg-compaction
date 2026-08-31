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

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::StreamExt;
use futures::future::try_join_all;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, PartitionSpec, Schema};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, TaskWriter};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{CompactionExecutor, RewriteFilesStat};
use crate::CompactionError;
use crate::common::CompactionMetricsRecorder;
use crate::config::CompactionExecutionConfig;
use crate::error::Result;
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
            file_group,
            execution_config,
            partition_spec,
            metrics_recorder,
            location_generator,
            sort_order,
            format_version,
        } = request;
        let mut stats = RewriteFilesStat::default();
        stats.record_input(&file_group);
        let sort_order_id = sort_order.clone().map(|sort_order| sort_order.id as i32);

        // Extract parallelism before file_group is moved
        let executor_parallelism = file_group.executor_parallelism;
        let output_parallelism = file_group.output_parallelism;

        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema.clone())
            .with_format_version(format_version)
            .with_input_data_files(file_group)
            .with_sort_order(sort_order.clone())
            .build()?;
        let (batches, input_schema) = DatafusionProcessor::new(
            execution_config.clone(),
            executor_parallelism,
            file_io.clone(),
        )?
        .execute(datafusion_task_ctx, output_parallelism)
        .await?;
        let arc_input_schema = Arc::new(input_schema);
        let mut futures = Vec::with_capacity(executor_parallelism);

        // Consume each output partition concurrently.
        for batch_stream in batches {
            let location_generator = location_generator.clone();
            let schema = arc_input_schema.clone();
            let execution_config = execution_config.clone();
            let file_io = file_io.clone();
            let partition_spec = partition_spec.clone();
            let metrics_recorder = metrics_recorder.clone();

            let future: JoinHandle<
                std::result::Result<Vec<iceberg::spec::DataFile>, CompactionError>,
            > = tokio::spawn(write_batch_stream(
                batch_stream,
                move || {
                    build_iceberg_data_file_writer(
                        execution_config.data_file_prefix.clone(),
                        location_generator,
                        schema,
                        file_io,
                        partition_spec,
                        sort_order_id,
                        execution_config,
                    )
                },
                metrics_recorder,
            ));
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

async fn write_batch_stream<F>(
    mut batch_stream: SendableRecordBatchStream,
    build_writer: F,
    metrics_recorder: Option<CompactionMetricsRecorder>,
) -> Result<Vec<DataFile>>
where
    F: FnOnce() -> Result<Box<dyn IcebergWriter>> + Send + 'static,
{
    let mut data_file_writer = None;
    let mut build_writer = Some(build_writer);

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

        // Repartitioning can leave output streams empty, so build only on first use.
        if data_file_writer.is_none() {
            let build_writer = build_writer
                .take()
                .expect("writer factory must only be called once");
            data_file_writer = Some(build_writer()?);
        }

        let write_start = Instant::now();
        data_file_writer
            .as_mut()
            .expect("writer must be initialized before writing")
            .write(batch)
            .await?;
        if let Some(metrics_recorder) = &metrics_recorder {
            metrics_recorder
                .record_datafusion_batch_write_duration(write_start.elapsed().as_millis() as f64);
            metrics_recorder.record_batch_stats(record_count, batch_bytes);
        }

        fetch_batch_start = Instant::now();
    }

    match data_file_writer.as_mut() {
        Some(data_file_writer) => Ok(data_file_writer.close().await?),
        None => Ok(Vec::new()),
    }
}

pub fn build_iceberg_data_file_writer(
    data_file_prefix: String,
    location_generator: DefaultLocationGenerator,
    schema: Arc<Schema>,
    file_io: FileIO,
    partition_spec: Arc<PartitionSpec>,
    sort_order_id: Option<i32>,
    execution_config: Arc<CompactionExecutionConfig>,
) -> Result<Box<dyn IcebergWriter>> {
    let target_file_size =
        usize::try_from(execution_config.target_file_size_bytes).map_err(|_| {
            CompactionError::Config(format!(
                "target_file_size_bytes {} exceeds platform usize",
                execution_config.target_file_size_bytes
            ))
        })?;

    let data_file_builder = {
        let parquet_writer_builder = ParquetWriterBuilder::new(
            execution_config.write_parquet_properties.clone(),
            schema.clone(),
        );

        let unique_uuid_suffix = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(
            data_file_prefix,
            Some(unique_uuid_suffix.to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            target_file_size,
            file_io,
            location_generator,
            file_name_generator,
        )
        .with_max_concurrent_closes(execution_config.max_concurrent_closes);

        DataFileWriterBuilder::new(rolling_writer_builder).sort_order_id(sort_order_id)
    };

    let partition_splitter = if partition_spec.is_unpartitioned() {
        None
    } else {
        Some(RecordBatchPartitionSplitter::try_new_with_computed_values(
            schema.clone(),
            partition_spec.clone(),
        )?)
    };

    let iceberg_task_writer = TaskWriter::new_with_partition_splitter(
        data_file_builder,
        true,
        schema,
        partition_spec,
        partition_splitter,
    );

    Ok(Box::new(iceberg_task_writer))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::Schema as ArrowSchema;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    use super::*;

    struct CountingWriter {
        write_count: Arc<AtomicUsize>,
        close_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl IcebergWriter for CountingWriter {
        async fn write(&mut self, _batch: RecordBatch) -> iceberg::Result<()> {
            self.write_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn close(&mut self) -> iceberg::Result<Vec<DataFile>> {
            self.close_count.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
    }

    fn batch_stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .unwrap_or_else(|| Arc::new(ArrowSchema::empty()));
        let batches = batches
            .into_iter()
            .map(Ok::<_, datafusion::error::DataFusionError>);

        Box::pin(RecordBatchStreamAdapter::new(schema, stream::iter(batches)))
    }

    #[tokio::test]
    async fn empty_stream_does_not_build_writer() {
        let build_count = Arc::new(AtomicUsize::new(0));
        let build_count_clone = build_count.clone();

        let data_files = write_batch_stream(
            batch_stream(Vec::new()),
            move || -> Result<Box<dyn IcebergWriter>> {
                build_count_clone.fetch_add(1, Ordering::Relaxed);
                unreachable!("empty stream must not build a writer")
            },
            None,
        )
        .await
        .unwrap();

        assert!(data_files.is_empty());
        assert_eq!(build_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn non_empty_stream_builds_one_writer() {
        let build_count = Arc::new(AtomicUsize::new(0));
        let write_count = Arc::new(AtomicUsize::new(0));
        let close_count = Arc::new(AtomicUsize::new(0));

        let build_count_clone = build_count.clone();
        let write_count_clone = write_count.clone();
        let close_count_clone = close_count.clone();
        let schema = Arc::new(ArrowSchema::empty());
        let batches = vec![
            RecordBatch::new_empty(schema.clone()),
            RecordBatch::new_empty(schema),
        ];

        let data_files = write_batch_stream(
            batch_stream(batches),
            move || -> Result<Box<dyn IcebergWriter>> {
                build_count_clone.fetch_add(1, Ordering::Relaxed);
                Ok(Box::new(CountingWriter {
                    write_count: write_count_clone,
                    close_count: close_count_clone,
                }))
            },
            None,
        )
        .await
        .unwrap();

        assert!(data_files.is_empty());
        assert_eq!(build_count.load(Ordering::Relaxed), 1);
        assert_eq!(write_count.load(Ordering::Relaxed), 2);
        assert_eq!(close_count.load(Ordering::Relaxed), 1);
    }
}
