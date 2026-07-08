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
use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion_processor::{
    DataFusionTaskContext, DatafusionProcessor, SYS_HIDDEN_FILE_PATH, SYS_HIDDEN_POS,
};
use futures::StreamExt;
use futures::future::try_join_all;
use hashbrown::HashSet;
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

use super::{CompactionExecutor, RewriteFilesStat, RowProvenance};
use crate::CompactionError;
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

        // Whether to emit per-row provenance mapping for surviving output rows.
        let need_row_provenance = execution_config.need_row_provenance;

        // Extract parallelism before file_group is moved
        let executor_parallelism = file_group.executor_parallelism;
        let output_parallelism = file_group.output_parallelism;

        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema.clone())
            .with_format_version(format_version)
            .with_input_data_files(file_group)
            .with_sort_order(sort_order.clone())
            .with_need_row_provenance(need_row_provenance)
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

        // build iceberg writer for each partition
        for mut batch_stream in batches {
            let location_generator = location_generator.clone();
            let schema = arc_input_schema.clone();
            let execution_config = execution_config.clone();
            let file_io = file_io.clone();
            let partition_spec = partition_spec.clone();
            let metrics_recorder = metrics_recorder.clone();

            let future: JoinHandle<
                std::result::Result<
                    (Vec<iceberg::spec::DataFile>, Vec<RowProvenance>),
                    CompactionError,
                >,
            > = tokio::spawn(async move {
                let mut data_file_writer = build_iceberg_data_file_writer(
                    execution_config.data_file_prefix.clone(),
                    location_generator,
                    schema,
                    file_io,
                    partition_spec,
                    sort_order_id,
                    execution_config,
                )?;

                // Accumulated per-row provenance for this partition (only used when enabled).
                let mut partition_provenance: Vec<RowProvenance> = Vec::new();

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
                    if need_row_provenance {
                        // Split the hidden provenance columns off the batch so the written
                        // data file only contains the real columns, then capture the output
                        // positions for each surviving row.
                        write_batch_with_provenance(
                            data_file_writer.as_mut(),
                            batch,
                            &mut partition_provenance,
                        )
                        .await?;
                    } else {
                        // Default path: unchanged behavior, no provenance accumulation.
                        data_file_writer.write(batch).await?;
                    }
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
                Ok((data_files, partition_provenance))
            });
            futures.push(future);
        }

        // collect all data files (and provenance) from all partitions
        let per_partition_results: Vec<(Vec<DataFile>, Vec<RowProvenance>)> = try_join_all(futures)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let mut output_data_files: Vec<DataFile> = Vec::new();
        let mut row_provenance: Vec<RowProvenance> = Vec::new();
        for (data_files, provenance) in per_partition_results {
            output_data_files.extend(data_files);
            row_provenance.extend(provenance);
        }

        stats.record_output(&output_data_files);

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stats,
            row_provenance,
        })
    }
}

/// Writes `batch` while capturing per-row provenance.
///
/// The incoming `batch` carries the two hidden provenance columns
/// (`SYS_HIDDEN_FILE_PATH` / `SYS_HIDDEN_POS`) appended after the real columns. This
/// function reads them for the input side, projects them OFF so the written data file
/// contains only the real columns, then calls `write_with_position` to obtain the
/// output `(file, pos)` for each surviving row and appends the zipped mapping to
/// `provenance`.
async fn write_batch_with_provenance(
    writer: &mut dyn IcebergWriter,
    batch: RecordBatch,
    provenance: &mut Vec<RowProvenance>,
) -> Result<()> {
    let schema = batch.schema();
    let file_path_idx = schema.index_of(SYS_HIDDEN_FILE_PATH).map_err(|e| {
        CompactionError::Execution(format!(
            "provenance column '{SYS_HIDDEN_FILE_PATH}' missing from batch: {e}"
        ))
    })?;
    let pos_idx = schema.index_of(SYS_HIDDEN_POS).map_err(|e| {
        CompactionError::Execution(format!(
            "provenance column '{SYS_HIDDEN_POS}' missing from batch: {e}"
        ))
    })?;

    let num_rows = batch.num_rows();

    // Read the hidden columns for the input provenance before projecting them away.
    let file_path_col = batch
        .column(file_path_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            CompactionError::Execution(format!(
                "provenance column '{SYS_HIDDEN_FILE_PATH}' is not a StringArray"
            ))
        })?;
    let pos_col = batch
        .column(pos_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            CompactionError::Execution(format!(
                "provenance column '{SYS_HIDDEN_POS}' is not an Int64Array"
            ))
        })?;

    // Drop the hidden columns so the written parquet contains only the real columns.
    let real_indices: Vec<usize> = (0..schema.fields().len())
        .filter(|i| *i != file_path_idx && *i != pos_idx)
        .collect();
    let real_batch = batch.project(&real_indices).map_err(|e| {
        CompactionError::Execution(format!(
            "failed to project provenance columns off batch: {e}"
        ))
    })?;

    // Output side: per-row (output_file, output_pos) in the same input-row order.
    let positions = writer.write_with_position(real_batch).await?;
    if positions.len() != num_rows {
        return Err(CompactionError::Execution(format!(
            "write_with_position returned {} positions for {} input rows",
            positions.len(),
            num_rows
        )));
    }

    let mut cached_file_paths: HashSet<Arc<str>> = HashSet::new();
    provenance.reserve(num_rows);
    for (i, pos_input) in positions.into_iter().enumerate() {
        let input_file = file_path_col.value(i);
        let input_file_arc = cached_file_paths
            .get_or_insert_with(input_file, |s| Arc::from(s))
            .clone();
        provenance.push(RowProvenance {
            input_file: input_file_arc,
            input_pos: pos_col.value(i) as u64,
            output_file: pos_input.path.clone(),
            output_pos: pos_input.pos as u64,
        });
    }

    Ok(())
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
