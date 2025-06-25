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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::task::JoinHandle;

use crate::CompactionError;

use super::{CompactionExecutor, RewriteFilesStat};
pub mod datafusion_processor;
use super::{RewriteFilesRequest, RewriteFilesResponse};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;

/// 写入阶段的性能诊断工具
#[derive(Debug)]
pub struct WriterDiagnostics {
    // 写入指标
    total_partitions: AtomicUsize,
    active_writers: AtomicUsize,
    total_batches_written: AtomicU64,
    total_bytes_written: AtomicU64,

    // 时间分布
    writer_creation_time: AtomicU64,
    wait_exec_time: AtomicU64,
    batch_write_time: AtomicU64,
    writer_close_time: AtomicU64,

    start_time: Instant,
}

impl WriterDiagnostics {
    pub fn new(total_partitions: usize) -> Arc<Self> {
        Arc::new(Self {
            total_partitions: AtomicUsize::new(total_partitions),
            active_writers: AtomicUsize::new(0),
            total_batches_written: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            writer_creation_time: AtomicU64::new(0),
            wait_exec_time: AtomicU64::new(0),
            batch_write_time: AtomicU64::new(0),
            writer_close_time: AtomicU64::new(0),
            start_time: Instant::now(),
        })
    }

    pub fn diagnose_writer_performance(&self, output_files_count: usize) {
        let total_time = self.start_time.elapsed().as_nanos() as u64;
        let creation_time = self.writer_creation_time.load(Ordering::Relaxed);
        let write_time = self.batch_write_time.load(Ordering::Relaxed);
        let close_time = self.writer_close_time.load(Ordering::Relaxed);
        let wait_exec_time = self.wait_exec_time.load(Ordering::Relaxed);

        let total_partitions = self.total_partitions.load(Ordering::Relaxed);
        let total_batches = self.total_batches_written.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes_written.load(Ordering::Relaxed);

        let creation_percent = (creation_time as f64 / total_time as f64) * 100.0;
        let write_percent = (write_time as f64 / total_time as f64) * 100.0;
        let close_percent = (close_time as f64 / total_time as f64) * 100.0;

        let throughput_mbps =
            (total_bytes as f64 / 1024.0 / 1024.0) / (total_time as f64 / 1_000_000_000.0);

        // 瓶颈分析
        let writer_analysis = if creation_percent > 30.0 {
            "BOTTLENECK: Writer creation too slow"
        } else if close_percent > 40.0 {
            "BOTTLENECK: Writer close/finalization too slow"
        } else if write_percent < 50.0 {
            "WARNING: Low write time percentage - possible I/O issues"
        } else if throughput_mbps < 50.0 {
            "BOTTLENECK: Low write throughput"
        } else {
            "Writer performance optimal"
        };

        tracing::info!(
            "🔍 WRITER DIAGNOSIS - Timing: Creation {:.1}%, Write {:.1}%, Close {:.1}% | Partitions: {}, Batches: {}, Files: {}, Throughput: {:.1}MB/s | Analysis: {}, WaitExec {:.1}ms, BatchWrite {:.1}ms, WriterClose {:.1}ms",
            creation_percent,
            write_percent,
            close_percent,
            total_partitions,
            total_batches,
            output_files_count,
            throughput_mbps,
            writer_analysis,
            wait_exec_time as f64 / 1_000_000.0,
            write_time as f64 / 1_000_000.0,
            close_time as f64 / 1_000_000.0
        );
    }
}

#[derive(Default)]
pub struct DataFusionExecutor {}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn rewrite_files(&self, request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        let overall_start = Instant::now();

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

        tracing::info!(
            "🚀 Starting rewrite_files - Input files: {}, Target partitions: {}, Batch parallelism: {}",
            rewritten_files_count,
            config.target_partitions,
            config.batch_parallelism
        );

        // 1. DataFusion 执行阶段
        let datafusion_start = Instant::now();
        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema)
            .with_input_data_files(input_file_scan_tasks)
            .build()?;
        let (batches, input_schema) = DatafusionProcessor::new(config.clone(), file_io.clone())
            .execute(datafusion_task_ctx)
            .await?;
        let datafusion_time = datafusion_start.elapsed();

        // 2. 写入阶段诊断初始化
        let writer_diagnostics = WriterDiagnostics::new(config.batch_parallelism);
        let arc_input_schema = Arc::new(input_schema);
        let mut futures = Vec::with_capacity(config.batch_parallelism);

        tracing::info!(
            "📊 DataFusion execution completed in {:?}, starting {} writer tasks",
            datafusion_time,
            config.batch_parallelism
        );

        // 3. 启动写入任务并行监控
        let writer_diag_clone = writer_diagnostics.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let active = writer_diag_clone.active_writers.load(Ordering::Relaxed);
                let batches = writer_diag_clone
                    .total_batches_written
                    .load(Ordering::Relaxed);
                let bytes_mb = writer_diag_clone
                    .total_bytes_written
                    .load(Ordering::Relaxed) as f64
                    / 1024.0
                    / 1024.0;

                tracing::info!(
                    "📝 Writer Progress: {} active writers, {} batches written, {:.1}MB written",
                    active,
                    batches,
                    bytes_mb
                );

                if active == 0 {
                    break; // 所有写入任务完成
                }
            }
        });

        // build iceberg writer for each partition
        for (partition_id, mut batch) in batches.into_iter().enumerate() {
            let dir_path = dir_path.clone();
            let schema = arc_input_schema.clone();
            let config = config.clone();
            let file_io = file_io.clone();
            let partition_spec = partition_spec.clone();
            let diagnostics = writer_diagnostics.clone();

            let future: JoinHandle<
                std::result::Result<Vec<iceberg::spec::DataFile>, CompactionError>,
            > = tokio::spawn(async move {
                let partition_start = Instant::now();
                diagnostics.active_writers.fetch_add(1, Ordering::Relaxed);

                // 1. Writer 创建阶段
                let writer_create_start = Instant::now();
                let mut data_file_writer = Self::build_iceberg_writer(
                    config.data_file_prefix.clone(),
                    dir_path,
                    schema,
                    file_io,
                    partition_spec,
                    config.target_file_size,
                    config.write_parquet_properties.clone(),
                )
                .await?;
                let writer_create_time = writer_create_start.elapsed();
                diagnostics
                    .writer_creation_time
                    .fetch_add(writer_create_time.as_nanos() as u64, Ordering::Relaxed);

                // 2. 批次写入阶段
                let mut batch_count = 0;
                let mut total_rows = 0;
                let write_start = Instant::now();

                let mut wait_exec_time = Instant::now();
                while let Some(b) = batch.as_mut().next().await {
                    let batch_write_start = Instant::now();
                    let record_batch = b?;
                    let batch_rows = record_batch.num_rows();
                    diagnostics.wait_exec_time.fetch_add(
                        wait_exec_time.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );

                    data_file_writer.write(record_batch).await?;

                    batch_count += 1;
                    total_rows += batch_rows;

                    let batch_write_time = batch_write_start.elapsed();
                    diagnostics
                        .batch_write_time
                        .fetch_add(batch_write_time.as_nanos() as u64, Ordering::Relaxed);
                    diagnostics
                        .total_batches_written
                        .fetch_add(1, Ordering::Relaxed);

                    tracing::debug!(
                        "📝 Partition {} wrote batch {}: {} rows in {:?}",
                        partition_id,
                        batch_count,
                        batch_rows,
                        batch_write_time
                    );
                    wait_exec_time = Instant::now();
                }
                let total_write_time = write_start.elapsed();

                // 3. Writer 关闭阶段
                let close_start = Instant::now();
                let data_files = data_file_writer.close().await?;
                let close_time = close_start.elapsed();
                diagnostics
                    .writer_close_time
                    .fetch_add(close_time.as_nanos() as u64, Ordering::Relaxed);

                let partition_total_time = partition_start.elapsed();
                let total_bytes: u64 = data_files.iter().map(|f| f.file_size_in_bytes()).sum();
                diagnostics
                    .total_bytes_written
                    .fetch_add(total_bytes, Ordering::Relaxed);

                // 分区性能分析
                let throughput_mbps =
                    (total_bytes as f64 / 1024.0 / 1024.0) / partition_total_time.as_secs_f64();
                let create_percent =
                    (writer_create_time.as_secs_f64() / partition_total_time.as_secs_f64()) * 100.0;
                let write_percent =
                    (total_write_time.as_secs_f64() / partition_total_time.as_secs_f64()) * 100.0;
                let close_percent =
                    (close_time.as_secs_f64() / partition_total_time.as_secs_f64()) * 100.0;

                tracing::info!(
                    "📝 Partition {} complete: {} batches, {} rows, {} files, {:.1}MB, {:.1}MB/s | Create: {:.1}%, Write: {:.1}%, Close: {:.1}%",
                    partition_id,
                    batch_count,
                    total_rows,
                    data_files.len(),
                    total_bytes as f64 / 1024.0 / 1024.0,
                    throughput_mbps,
                    create_percent,
                    write_percent,
                    close_percent
                );

                diagnostics.active_writers.fetch_sub(1, Ordering::Relaxed);
                Ok(data_files)
            });
            futures.push(future);
        }

        // 4. 收集所有写入结果
        let collect_start = Instant::now();
        let output_data_files: Vec<DataFile> = try_join_all(futures)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .into_iter()
            .map(|res| res.map(|v| v.into_iter()))
            .collect::<Result<Vec<_>>>()
            .map(|iters| iters.into_iter().flatten().collect())?;
        let collect_time = collect_start.elapsed();

        // 5. 统计信息
        stat.added_files_count = output_data_files.len() as u32;
        stat.rewritten_bytes = output_data_files
            .iter()
            .map(|f| f.file_size_in_bytes())
            .sum();
        stat.rewritten_files_count = rewritten_files_count;

        let overall_time = overall_start.elapsed();

        // 6. 最终诊断
        let datafusion_percent =
            (datafusion_time.as_secs_f64() / overall_time.as_secs_f64()) * 100.0;
        let collect_percent = (collect_time.as_secs_f64() / overall_time.as_secs_f64()) * 100.0;
        let writer_percent = 100.0 - datafusion_percent - collect_percent;

        writer_diagnostics.diagnose_writer_performance(output_data_files.len());

        tracing::info!(
            "🔍 REWRITE_FILES DIAGNOSIS - Total: {:?} | Phases: DataFusion {:.1}%, Writers {:.1}%, Collect {:.1}% | Files: {} -> {} | Bytes: {:.1}MB | Throughput: {:.1}MB/s",
            overall_time,
            datafusion_percent,
            writer_percent,
            collect_percent,
            rewritten_files_count,
            output_data_files.len(),
            stat.rewritten_bytes as f64 / 1024.0 / 1024.0,
            (stat.rewritten_bytes as f64 / 1024.0 / 1024.0) / overall_time.as_secs_f64()
        );

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stat,
        })
    }
}

impl DataFusionExecutor {
    async fn build_iceberg_writer(
        data_file_prefix: String,
        dir_path: String,
        schema: Arc<Schema>,
        file_io: FileIO,
        partition_spec: Arc<PartitionSpec>,
        target_file_size: u64,
        write_parquet_properties: WriterProperties,
    ) -> Result<Box<dyn IcebergWriter>> {
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
}
