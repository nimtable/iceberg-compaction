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

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use async_trait::async_trait;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::StreamExt;
use futures::future::try_join_all;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, DataFileFormat, PartitionKey, PartitionSpec, Schema};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, TaskWriter};
use tokio_util::task::AbortOnDropHandle;
use uuid::Uuid;

use super::{CompactionExecutor, RewriteFilesStat};
use crate::CompactionError;
use crate::config::{CompactionExecutionConfig, DataFileNaming};
use crate::error::Result;
pub mod datafusion_processor;
use super::{RewriteFilesRequest, RewriteFilesResponse};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;
pub mod iceberg_partition_expr;

#[derive(Default)]
pub struct DataFusionExecutor {
    /// Runtime (bounded `FairSpillPool` + `DiskManager`) built once and shared
    /// across every `rewrite_files` call on this executor. igloo runs all the
    /// concurrent plans of one invocation through a single `Compaction`, which
    /// holds a single `DataFusionExecutor`, so caching the runtime here makes
    /// `max_memory_bytes` a *pod-wide* ceiling instead of a per-plan one:
    /// two concurrent unsorted plans would otherwise hold two independent
    /// `max_memory_bytes` pools and blow the pod's memory limit (F6 OOM).
    ///
    /// `OnceLock` so every call after the first successful build is a lock-free
    /// read. Lazily initialized from the first request whose `execution_config`
    /// sets a budget; all plans in an invocation share the same config, so the
    /// first config governs the shared pool for that invocation. `None`
    /// (unbudgeted) requests keep the previous per-call, unbounded behavior.
    ///
    /// Only the success case is cached here: `build_spilling_runtime_env` is
    /// fallible (e.g. it can fail to create `spill_dir`), and `OnceLock`'s
    /// initializer must be infallible, so an `Err` is never stored — it's
    /// returned to that caller and retried on the next call, same as before
    /// this was split out of the `build_lock`-guarded slow path below.
    shared_runtime: OnceLock<Arc<RuntimeEnv>>,
    /// Serializes concurrent first-time builds so only one `RuntimeEnv` is ever
    /// constructed even when multiple plans race on the very first call —
    /// otherwise two racing builds could each pass the `shared_runtime.get()`
    /// check, transiently defeating the single-pool guarantee this cache exists
    /// for. Held only across the synchronous build, never across an `.await`.
    build_lock: Mutex<()>,
}

impl std::fmt::Debug for DataFusionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionExecutor").finish_non_exhaustive()
    }
}

impl DataFusionExecutor {
    /// Returns the shared bounded runtime for this executor, building it once on
    /// first use, or `None` when no memory budget is configured (unbounded,
    /// per-call behavior preserved).
    ///
    /// Sync; the fast path (already built) never locks. The slow path (first
    /// build) briefly holds `build_lock`, dropped before the caller `.await`s,
    /// so it never crosses an await point.
    fn shared_runtime_env(
        &self,
        execution_config: &CompactionExecutionConfig,
    ) -> Result<Option<Arc<RuntimeEnv>>> {
        let Some(max_memory_bytes) = execution_config.max_memory_bytes.filter(|n| *n > 0) else {
            return Ok(None);
        };

        if let Some(runtime_env) = self.shared_runtime.get() {
            return Ok(Some(runtime_env.clone()));
        }

        let _guard = self.build_lock.lock().map_err(|e| {
            CompactionError::Unexpected(format!("shared runtime build lock poisoned: {e}"))
        })?;
        // Someone else may have finished building while we waited for the lock.
        if let Some(runtime_env) = self.shared_runtime.get() {
            return Ok(Some(runtime_env.clone()));
        }
        let runtime_env = datafusion_processor::build_spilling_runtime_env(
            max_memory_bytes,
            execution_config.spill_dir.as_deref(),
            execution_config.max_disk_spill_bytes,
        )?;
        // Can only fail if another thread set it between our check and here,
        // which `build_lock` rules out.
        let _ = self.shared_runtime.set(runtime_env.clone());
        Ok(Some(runtime_env))
    }
}

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
            .with_partition_spec(partition_spec.clone())
            .build()?;
        let shared_runtime = self.shared_runtime_env(&execution_config)?;
        let (batches, input_schema) = DatafusionProcessor::new(
            execution_config.clone(),
            executor_parallelism,
            file_io.clone(),
            shared_runtime,
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

            // Abort on drop: if one partition fails (try_join_all drops the rest) or this
            // call is cancelled, the other writers stop early, leaving fewer orphan files.
            let future: AbortOnDropHandle<
                std::result::Result<Vec<iceberg::spec::DataFile>, CompactionError>,
            > = AbortOnDropHandle::new(tokio::spawn(async move {
                let mut data_file_writer = build_iceberg_data_file_writer(
                    execution_config.data_file_prefix.clone(),
                    location_generator,
                    schema,
                    file_io,
                    partition_spec,
                    sort_order_id,
                    execution_config,
                )?;

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

                Ok(data_file_writer.close().await?)
            }));
            futures.push(future);
        }

        // collect all data files from all partitions
        let output_data_files: Vec<DataFile> =
            try_join_all(futures.into_iter().map(|handle| async move {
                handle
                    .await
                    .map_err(|e| CompactionError::Execution(e.to_string()))?
            }))
            .await?
            .into_iter()
            .flatten()
            .collect();

        stats.record_output(&output_data_files);

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stats,
        })
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

    match execution_config.data_file_naming {
        DataFileNaming::Default => {
            let file_name_generator = DefaultFileNameGenerator::new(
                data_file_prefix,
                Some(Uuid::now_v7().to_string()),
                DataFileFormat::Parquet,
            );
            build_task_writer(
                location_generator,
                file_name_generator,
                target_file_size,
                schema,
                file_io,
                partition_spec,
                sort_order_id,
                &execution_config,
            )
        }
        DataFileNaming::FlatUuid => build_task_writer(
            FlatLocationGenerator(location_generator),
            UuidFileNameGenerator,
            target_file_size,
            schema,
            file_io,
            partition_spec,
            sort_order_id,
            &execution_config,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_task_writer<L: LocationGenerator, F: FileNameGenerator>(
    location_generator: L,
    file_name_generator: F,
    target_file_size: usize,
    schema: Arc<Schema>,
    file_io: FileIO,
    partition_spec: Arc<PartitionSpec>,
    sort_order_id: Option<i32>,
    execution_config: &CompactionExecutionConfig,
) -> Result<Box<dyn IcebergWriter>> {
    let parquet_writer_builder = ParquetWriterBuilder::new(
        execution_config.write_parquet_properties.clone(),
        schema.clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new(
        parquet_writer_builder,
        target_file_size,
        file_io,
        location_generator,
        file_name_generator,
    )
    .with_max_concurrent_closes(execution_config.max_concurrent_closes);
    let data_file_builder =
        DataFileWriterBuilder::new(rolling_writer_builder).sort_order_id(sort_order_id);

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

/// Places every file directly under the data location, ignoring the partition key.
#[derive(Clone, Debug)]
struct FlatLocationGenerator(DefaultLocationGenerator);

impl LocationGenerator for FlatLocationGenerator {
    fn generate_location(&self, _partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        self.0.generate_location(None, file_name)
    }
}

/// Names each file `{uuid}.parquet` with a fresh UUID v7.
#[derive(Clone, Debug)]
struct UuidFileNameGenerator;

impl FileNameGenerator for UuidFileNameGenerator {
    fn generate_file_name(&self) -> String {
        format!("{}.{}", Uuid::now_v7(), DataFileFormat::Parquet)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use iceberg::arrow::schema_to_arrow_schema;
    use iceberg::spec::{NestedField, PrimitiveType, Transform, Type};
    use tempfile::TempDir;

    use super::*;
    use crate::config::CompactionExecutionConfigBuilder;

    #[tokio::test]
    async fn test_flat_uuid_naming_ignores_partition_path() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "part", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .add_partition_field("part", "part", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        let temp_dir = TempDir::new().unwrap();
        let data_location = format!("{}/data", temp_dir.path().to_str().unwrap());
        let execution_config = Arc::new(
            CompactionExecutionConfigBuilder::default()
                .data_file_naming(DataFileNaming::FlatUuid)
                .build()
                .unwrap(),
        );

        let mut writer = build_iceberg_data_file_writer(
            "ignored-prefix".to_owned(),
            DefaultLocationGenerator::with_data_location(data_location.clone()),
            schema.clone(),
            FileIO::new_with_fs(),
            partition_spec,
            None,
            execution_config,
        )
        .unwrap();
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::new(schema_to_arrow_schema(&schema).unwrap()),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![10, 20, 10])),
            ],
        )
        .unwrap();
        writer.write(batch).await.unwrap();
        let data_files = writer.close().await.unwrap();

        // One file per partition value, all directly under the data location.
        assert_eq!(data_files.len(), 2);
        let mut uuids = HashSet::new();
        for data_file in &data_files {
            let file_name = data_file
                .file_path()
                .strip_prefix(&format!("{data_location}/"))
                .unwrap();
            let uuid = Uuid::parse_str(file_name.strip_suffix(".parquet").unwrap()).unwrap();
            assert_eq!(uuid.get_version_num(), 7);
            assert_eq!(
                data_file.file_path(),
                format!("{data_location}/{}.parquet", uuid.hyphenated())
            );
            uuids.insert(uuid);
        }
        assert_eq!(uuids.len(), 2);
    }
}
