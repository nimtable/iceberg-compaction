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

use std::any::Any;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::sync::Arc;
use std::vec;

use async_stream::try_stream;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef as ArrowSchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::scan::FileScanTask;
use iceberg::spec::DataContentType;
use iceberg_datafusion::physical_plan::expr_to_predicate::convert_filters_to_predicate;
use iceberg_datafusion::to_datafusion_error;

use super::datafusion_processor::SYS_HIDDEN_SEQ_NUM;

struct RecordBatchBuffer {
    buffer: Vec<RecordBatch>,
    current_rows: usize,

    max_record_batch_rows: usize,

    /// Bytes of the decoded batches currently retained in `buffer`, mirrored as
    /// a live reservation against the `DataFusion` memory pool.
    ///
    /// Charging the scan's retained decode output to the pool gives the
    /// compaction memory budget (`max_memory_bytes` -> `FairSpillPool`) teeth on
    /// the unsorted-table path, which has no `SortExec` to reserve from the pool
    /// and therefore otherwise never surfaces its decode footprint. Under a
    /// bounded pool an over-budget batch fails fast with `ResourcesExhausted`
    /// (a clean, retriable error) instead of the process being OOM-killed; with
    /// the default unbounded pool `try_grow` always succeeds, so behavior is
    /// unchanged.
    ///
    /// The reservation only ever reflects bytes we still hold: it is released as
    /// batches are flushed or yielded downstream (and fully on drop), so it does
    /// not over-reserve against a co-resident `SortExec` on the sorted path.
    buffered_bytes: usize,
    reservation: MemoryReservation,
}

impl RecordBatchBuffer {
    fn new(max_record_batch_rows: usize, memory_pool: &Arc<dyn MemoryPool>) -> Self {
        Self {
            buffer: vec![],
            current_rows: 0,
            max_record_batch_rows,
            buffered_bytes: 0,
            reservation: MemoryConsumer::new("IcebergFileTaskScan[decode]").register(memory_pool),
        }
    }

    fn add(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>, DataFusionError> {
        // Charge the freshly decoded batch against the pool before we retain or
        // forward it. When the bounded budget is exceeded by the aggregate
        // in-flight decode output across concurrently-executing scan partitions
        // this fails fast with `ResourcesExhausted` instead of OOM-killing.
        let batch_bytes = batch.get_array_memory_size();
        self.reservation.try_grow(batch_bytes)?;

        // Case 1: New batch itself is large enough and buffer is empty or too small to be significant
        if batch.num_rows() >= self.max_record_batch_rows && self.buffer.is_empty() {
            // Buffer was empty, yield current large batch directly. It leaves our
            // control on yield, so release its charge immediately.
            self.reservation.shrink(batch_bytes);
            return Ok(Some(batch));
        }

        // Case 2: Buffer will overflow with the new batch
        if !self.buffer.is_empty()
            && (self.current_rows + batch.num_rows() > self.max_record_batch_rows)
        {
            let combined = self.finish_internal()?; // Drain and combine buffer (releases the retained charge)
            self.current_rows = batch.num_rows();
            self.buffered_bytes = batch_bytes; // retain the new batch; its charge is already grown
            self.buffer.push(batch); // Add current batch to now-empty buffer
            return Ok(combined); // Return the combined batch from buffer
        }

        // Case 3: Buffer has space
        self.current_rows += batch.num_rows();
        self.buffered_bytes += batch_bytes; // charge already grown above
        self.buffer.push(batch);
        Ok(None)
    }

    // Helper to drain and combine buffer, used by add and finish
    fn finish_internal(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let schema_to_use = self.buffer[0].schema();
        let batches_to_combine: Vec<_> = self.buffer.drain(..).collect();
        let combined = concat_batches(&schema_to_use, &batches_to_combine)?;
        self.current_rows = 0;
        // The retained input batches are dropped here and the combined batch
        // leaves our control on yield, so release the charge we held for them.
        self.reservation.shrink(self.buffered_bytes);
        self.buffered_bytes = 0;
        Ok(Some(combined))
    }

    fn finish(mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        self.finish_internal()
    }
}

/// An execution plan for scanning iceberg file scan tasks
#[derive(Debug)]
pub(crate) struct IcebergFileTaskScan {
    file_scan_tasks_group: Vec<Vec<FileScanTask>>,
    plan_properties: Arc<PlanProperties>,
    projection: Option<Vec<String>>,
    predicates: Option<Predicate>,
    file_io: FileIO,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    max_record_batch_rows: usize,
    prefetch_enabled: bool,
}

impl IcebergFileTaskScan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file_scan_tasks: Vec<FileScanTask>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        file_io: &FileIO,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        executor_parallelism: usize,
        max_record_batch_rows: usize,
        prefetch_enabled: bool,
    ) -> Result<Self, DataFusionError> {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let projection = get_column_names(schema.clone(), projection);
        let file_scan_tasks_projection = if let Some(projection) = &projection {
            file_scan_tasks
                .into_iter()
                .map(|mut task| {
                    let project_field_ids = projection
                        .iter()
                        .filter_map(|name| task.schema().field_id_by_name(name))
                        .collect::<Vec<_>>();
                    let new_schema = iceberg::spec::Schema::builder()
                        .with_fields(
                            projection
                                .iter()
                                .filter_map(|name| task.schema().field_by_name(name).cloned()),
                        )
                        .build()
                        .map_err(to_datafusion_error)?;
                    task.schema = Arc::new(new_schema);
                    task.project_field_ids = project_field_ids;
                    Ok(task)
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?
        } else {
            file_scan_tasks
        };
        let file_scan_tasks_group = split_n_vecs(file_scan_tasks_projection, executor_parallelism);
        let plan_properties =
            Self::compute_properties(output_schema.clone(), file_scan_tasks_group.len());
        let predicates = convert_filters_to_predicate(filters);

        Ok(Self {
            file_scan_tasks_group,
            plan_properties,
            projection,
            predicates,
            file_io: file_io.clone(),
            need_seq_num,
            need_file_path_and_pos,
            max_record_batch_rows,
            prefetch_enabled,
        })
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef, partitioning_size: usize) -> Arc<PlanProperties> {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partitioning_size),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

/// Uniformly distribute scan tasks to compute nodes.
/// It's deterministic so that it can best utilize the data locality.
///
/// # Arguments
/// * `file_scan_tasks`: The file scan tasks to be split.
/// * `split_num`: The number of splits to be created.
///
/// This algorithm is based on a min-heap. It will push all groups into the heap, and then pop the smallest group and add the file scan task to it.
/// Ensure that the total length of each group is as balanced as possible.
/// The time complexity is O(n log k), where n is the number of file scan tasks and k is the number of splits.
/// The space complexity is O(k), where k is the number of splits.
/// The algorithm is stable, so the order of the file scan tasks will be preserved.
fn split_n_vecs(file_scan_tasks: Vec<FileScanTask>, split_num: usize) -> Vec<Vec<FileScanTask>> {
    use std::cmp::{Ordering, Reverse};

    #[derive(Default)]
    struct FileScanTaskGroup {
        idx: usize,
        tasks: Vec<FileScanTask>,
        total_length: u64,
    }

    impl Ord for FileScanTaskGroup {
        fn cmp(&self, other: &Self) -> Ordering {
            // when total_length is the same, we will sort by index
            if self.total_length == other.total_length {
                self.idx.cmp(&other.idx)
            } else {
                self.total_length.cmp(&other.total_length)
            }
        }
    }

    impl PartialOrd for FileScanTaskGroup {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Eq for FileScanTaskGroup {}

    impl PartialEq for FileScanTaskGroup {
        fn eq(&self, other: &Self) -> bool {
            self.total_length == other.total_length
        }
    }

    let mut heap = BinaryHeap::new();
    // push all groups into heap
    for idx in 0..split_num {
        heap.push(Reverse(FileScanTaskGroup {
            idx,
            tasks: vec![],
            total_length: 0,
        }));
    }

    for file_task in file_scan_tasks {
        let mut group = heap.peek_mut().unwrap();
        group.0.total_length += file_task.length;
        group.0.tasks.push(file_task);
    }

    // convert heap into vec and extract tasks
    heap.into_vec()
        .into_iter()
        .map(|reverse_group| reverse_group.0.tasks)
        .collect()
}

impl ExecutionPlan for IcebergFileTaskScan {
    fn name(&self) -> &str {
        "IcebergFileTaskScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let fut = get_batch_stream(
            self.file_io.clone(),
            self.file_scan_tasks_group[partition].clone(),
            self.need_seq_num,
            self.need_file_path_and_pos,
            self.max_record_batch_rows,
            self.prefetch_enabled,
            context.memory_pool().clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Gets a stream of record batches from a list of file scan tasks.
///
/// If `is_prefetch_enabled` is true, it will download the entire file into memory before
/// reading into a batch stream. Instead of making N HTTP requests for N column chunks,
/// we make 1 HTTP request per file.
///
/// For now, only one file is processed at a time. Subsequent calls to the stream will
/// eventually trigger more I/O reads.
#[allow(clippy::unused_async)]
async fn get_batch_stream(
    file_io: FileIO,
    file_scan_tasks: Vec<FileScanTask>,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    max_record_batch_rows: usize,
    is_prefetch_enabled: bool,
    memory_pool: Arc<dyn MemoryPool>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let optional_prefetch: Option<Prefetcher> = if is_prefetch_enabled {
        Some(Prefetcher::new()?)
    } else {
        None
    };

    let stream = try_stream! {
        let mut record_batch_buffer = RecordBatchBuffer::new(max_record_batch_rows, &memory_pool);

        // Account the bytes prefetch holds in memory against the DataFusion memory
        // pool. When compaction runs under a bounded budget (`max_memory_bytes` ->
        // `FairSpillPool`), prefetching more concurrent full files than the budget
        // allows fails cleanly with `ResourcesExhausted` instead of buffering every
        // downloaded file in memory and OOM-killing the process. Unsorted-table
        // compaction has no `SortExec` to reserve from the pool, so without this the
        // pool never sees the scan's largest allocation. With the default unbounded
        // pool `try_grow` always succeeds, so behavior is unchanged.
        let mut prefetch_reservation =
            MemoryConsumer::new("IcebergFileTaskScan[prefetch]").register(&memory_pool);

        for task in file_scan_tasks {
            let mut file_context = FileTaskContext::from_task(&task);

            let (file_io, task) = match &optional_prefetch {
                Some(prefetcher) => {
                    // We prefetch the file into memory, and return a MemoryFileIO to give to
                    // the ArrowReaderBuilder instead of the original file IO.
                    let in_memory_task = prefetcher.prefetch_file_into_memory(&task, &mut file_context, &file_io, &mut prefetch_reservation).await?;
                    (prefetcher.memory_file_io(), in_memory_task)
                },
                None => (file_io.clone(), task)
            };

            let task_stream = futures::stream::iter(vec![Ok(task)]).boxed();
            let arrow_reader_builder = ArrowReaderBuilder::new(file_io).with_batch_size(max_record_batch_rows);
            let mut batch_stream = arrow_reader_builder.build()
                .read(task_stream)
                .map_err(to_datafusion_error)?;

            let mut index_start = 0;
            while let Some(batch) = batch_stream.next().await {
                let mut batch = batch.map_err(to_datafusion_error)?;
                let batch = match file_context.data_file_content {
                    DataContentType::Data => {
                        // add sequence number if needed
                        if need_seq_num {
                            batch = add_seq_num_into_batch(batch, file_context.sequence_number)?;
                        }
                        // add file path and position if needed (for prefetching: use original path, not memory path)
                        if need_file_path_and_pos {
                            batch = add_file_path_pos_into_batch(batch, &file_context.original_path, index_start)?;
                            index_start += batch.num_rows() as i64;
                        }
                        batch
                    }
                    DataContentType::PositionDeletes => {
                        batch
                    },
                    DataContentType::EqualityDeletes => {
                        add_seq_num_into_batch(batch, file_context.sequence_number)?
                    },
                };
                if let Some(batch) = record_batch_buffer.add(batch)? {
                    yield batch;
                }
            }

            // 3. Clean up any memory file to avoid accmulating too much memory; ignore errors as cleanup is best-effort
            if let Some(prefetcher) = &optional_prefetch {
                let _ = prefetcher.clear_file(&file_context, &mut prefetch_reservation).await;
            }
        }

        if let Some(batch) = record_batch_buffer.finish()? {
            yield batch;
        }
    };

    Ok(Box::pin(stream))
}

/// Holds metadata about a file scan task that needs to be preserved
/// when using memory-backed file reading for prefetch optimization.
struct FileTaskContext {
    original_path: String,
    data_file_content: DataContentType,
    sequence_number: i64,
    memory_path: Option<String>,
    /// Bytes reserved from the `DataFusion` memory pool for the prefetched
    /// in-memory copy of this file; released when the file is cleared.
    reserved_bytes: usize,
}

impl FileTaskContext {
    fn from_task(task: &FileScanTask) -> Self {
        Self {
            original_path: task.data_file_path.clone(),
            data_file_content: task.data_file_content,
            sequence_number: task.sequence_number,
            memory_path: None,
            reserved_bytes: 0,
        }
    }

    fn set_memory_path(&mut self, memory_path: String) {
        self.memory_path = Some(memory_path);
    }
}

struct Prefetcher {
    memory_file_io: FileIO,
}

/// Internal type for prefetching file scan tasks into memory. This allows us to
/// only setup the memory file IO when prefetch is enabled.
impl Prefetcher {
    fn new() -> DFResult<Self> {
        Ok(Self {
            memory_file_io: FileIOBuilder::new("memory")
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        })
    }

    pub fn memory_file_io(&self) -> FileIO {
        self.memory_file_io.clone()
    }

    pub async fn prefetch_file_into_memory(
        &self,
        task: &FileScanTask,
        file_context: &mut FileTaskContext,
        file_io: &FileIO,
        reservation: &mut MemoryReservation,
    ) -> DFResult<FileScanTask> {
        // Reserve the file's *declared* size against the pool BEFORE downloading
        // it. The download buffers the whole file in memory, so reserving only
        // afterward (the previous behavior) let several concurrent object-store
        // reads materialize their buffers and blow past a bounded budget during
        // `read().await`, before any of them returned `ResourcesExhausted`.
        // Reserving up-front means an over-budget prefetch is rejected before the
        // read is ever started. A failure here leaves the reservation untouched,
        // so we can propagate it directly.
        let declared_bytes = usize::try_from(task.file_size_in_bytes).unwrap_or(usize::MAX);
        reservation.try_grow(declared_bytes)?;
        file_context.reserved_bytes = declared_bytes;

        // Everything past the up-front reservation can fail after we already hold
        // pool memory, so release `file_context.reserved_bytes` (the declared
        // amount, or the reconciled actual once known) on any error to keep the
        // pool clean if this prefetch is abandoned.
        match self
            .download_and_stage(task, file_context, file_io, reservation)
            .await
        {
            Ok(memory_task) => Ok(memory_task),
            Err(e) => {
                reservation.shrink(file_context.reserved_bytes);
                file_context.reserved_bytes = 0;
                Err(e)
            }
        }
    }

    /// Download the file, reconcile the up-front reservation to the file's actual
    /// length, and stage it in the in-memory `FileIO`. Assumes the caller has
    /// already reserved `file_context.reserved_bytes` and will release it on error.
    async fn download_and_stage(
        &self,
        task: &FileScanTask,
        file_context: &mut FileTaskContext,
        file_io: &FileIO,
        reservation: &mut MemoryReservation,
    ) -> DFResult<FileScanTask> {
        // 1. Download file from source storage (single HTTP request)
        let file_bytes = file_io
            .new_input(&file_context.original_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .read()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Reconcile the up-front (declared) reservation with the actual byte
        // length. Iceberg metadata is normally exact; if the file is smaller,
        // `try_resize` releases the slack, and if it is larger it grows the
        // difference and can still fail cleanly with `ResourcesExhausted`.
        let actual_bytes = file_bytes.len();
        reservation.try_resize(actual_bytes)?;
        file_context.reserved_bytes = actual_bytes;

        // 2. Write to memory storage
        let memory_path = generate_memory_path(&file_context.original_path);
        self.memory_file_io
            .new_output(&memory_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .write(file_bytes)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // 3. Create modified task with memory path
        let mut memory_task = task.clone();
        memory_task.data_file_path = memory_path.clone();
        file_context.set_memory_path(memory_path);
        Ok(memory_task)
    }

    pub async fn clear_file(
        &self,
        file_context: &FileTaskContext,
        reservation: &mut MemoryReservation,
    ) -> DFResult<()> {
        // swallow errors: this is best effort cleanup
        if let Some(memory_path) = &file_context.memory_path {
            let _ = self.memory_file_io.delete(memory_path).await;
        }
        // Release the pool reservation for this file's in-memory copy.
        reservation.shrink(file_context.reserved_bytes);
        Ok(())
    }
}

/// Generate a memory path from an original storage path. This does not impact the final
/// compaction output. It simply allows the `ArrowReaderBuilder` to read file data from memory.
/// Ex: `s3://bucket/path/to/file.parquet` -> `memory:/bucket/path/to/file.parquet`
fn generate_memory_path(original_path: &str) -> String {
    format!("memory:/{}", original_path.replace("://", "/"))
}

/// Adds a sequence number column to a record batch
fn add_seq_num_into_batch(batch: RecordBatch, seq_num: i64) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let seq_num_field = Arc::new(Field::new(
        SYS_HIDDEN_SEQ_NUM,
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    let mut new_fields = schema.fields().to_vec();
    new_fields.push(seq_num_field);
    let new_schema = Arc::new(Schema::new(new_fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![seq_num; batch.num_rows()])));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Adds a file path and position column to a record batch
fn add_file_path_pos_into_batch(
    batch: RecordBatch,
    file_path: &str,
    index_start: i64,
) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let file_path_field = Arc::new(Field::new(
        "file_path",
        datafusion::arrow::datatypes::DataType::Utf8,
        false,
    ));
    let pos_field = Arc::new(Field::new(
        "pos",
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    let mut new_fields = schema.fields().to_vec();
    new_fields.push(file_path_field);
    new_fields.push(pos_field);
    let new_schema = Arc::new(Schema::new(new_fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![
        file_path;
        batch.num_rows()
    ])));
    columns.push(Arc::new(Int64Array::from_iter(
        (index_start..(index_start + batch.num_rows() as i64)).collect::<Vec<i64>>(),
    )));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

impl DisplayAs for IcebergFileTaskScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "IcebergTableScan projection:[{}] predicate:[{}]",
            self.projection
                .clone()
                .map_or(String::new(), |v| v.join(",")),
            self.predicates
                .clone()
                .map_or(String::from(""), |p| format!("{}", p))
        )
    }
}

pub fn get_column_names(
    schema: ArrowSchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<String>> {
    projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType as ArrowDataType, SchemaBuilder};
    use iceberg::scan::FileScanTask;
    use iceberg::spec::{DataContentType, Schema};

    use super::*;

    fn create_file_scan_task(length: u64, file_id: u64) -> FileScanTask {
        FileScanTask {
            length,
            start: 0,
            record_count: Some(0),
            data_file_path: format!("test_{}.parquet", file_id),
            referenced_data_file: None,
            data_file_content: DataContentType::Data,
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: None,
            file_size_in_bytes: length,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    #[test]
    fn test_split_n_vecs_basic() {
        let file_scan_tasks = (1..=12)
            .map(|i| create_file_scan_task(i + 100, i))
            .collect::<Vec<_>>();

        let groups = split_n_vecs(file_scan_tasks, 3);

        assert_eq!(groups.len(), 3);

        let group_lengths: Vec<u64> = groups
            .iter()
            .map(|group| group.iter().map(|task| task.length).sum())
            .collect();

        let max_length = *group_lengths.iter().max().unwrap();
        let min_length = *group_lengths.iter().min().unwrap();
        assert!(max_length - min_length <= 10, "Groups should be balanced");

        let total_tasks: usize = groups.iter().map(|group| group.len()).sum();
        assert_eq!(total_tasks, 12);
    }

    #[test]
    fn test_split_n_vecs_empty() {
        let file_scan_tasks = Vec::new();
        let groups = split_n_vecs(file_scan_tasks, 3);
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(|group| group.is_empty()));
    }

    #[test]
    fn test_split_n_vecs_single_task() {
        let file_scan_tasks = vec![create_file_scan_task(100, 1)];
        let groups = split_n_vecs(file_scan_tasks, 3);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups.iter().filter(|group| !group.is_empty()).count(), 1);
    }

    #[test]
    fn test_split_n_vecs_uneven_distribution() {
        let file_scan_tasks = vec![
            create_file_scan_task(1000, 1),
            create_file_scan_task(100, 2),
            create_file_scan_task(100, 3),
            create_file_scan_task(100, 4),
            create_file_scan_task(100, 5),
        ];

        let groups = split_n_vecs(file_scan_tasks, 2);
        assert_eq!(groups.len(), 2);

        let group_with_large_task = groups
            .iter()
            .find(|group| group.iter().any(|task| task.length == 1000))
            .unwrap();
        assert_eq!(group_with_large_task.len(), 1);
    }

    #[test]
    fn test_split_n_vecs_same_files_distribution() {
        let file_scan_tasks = vec![
            create_file_scan_task(100, 1),
            create_file_scan_task(100, 2),
            create_file_scan_task(100, 3),
            create_file_scan_task(100, 4),
            create_file_scan_task(100, 5),
            create_file_scan_task(100, 6),
            create_file_scan_task(100, 7),
            create_file_scan_task(100, 8),
        ];

        let groups = split_n_vecs(file_scan_tasks.clone(), 4)
            .iter()
            .map(|g| {
                g.iter()
                    .map(|task| task.data_file_path.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for _ in 0..10000 {
            let groups_2 = split_n_vecs(file_scan_tasks.clone(), 4)
                .iter()
                .map(|g| {
                    g.iter()
                        .map(|task| task.data_file_path.clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            assert_eq!(groups, groups_2);
        }
    }

    use datafusion::arrow::array::Int32Array;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};

    // Helper: build a RecordBatchBuffer backed by an unbounded pool for tests
    // that only exercise the coalescing logic (not the memory accounting).
    fn test_record_batch_buffer(max_record_batch_rows: usize) -> RecordBatchBuffer {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        RecordBatchBuffer::new(max_record_batch_rows, &memory_pool)
    }

    // Helper function to create a RecordBatch with a single Int32 column and specified number of rows
    fn create_test_batch(num_rows: usize, schema_opt: Option<ArrowSchemaRef>) -> RecordBatch {
        // Renamed schema to schema_opt for clarity
        let schema = schema_opt.unwrap_or_else(|| {
            let mut builder = SchemaBuilder::new();
            builder.push(Field::new(
                "a",
                ArrowDataType::Int32, // Use ArrowDataType explicitly
                false,
            ));
            Arc::new(builder.finish()) // Use builder.finish() to get the Schema
        });
        let arr = Arc::new(Int32Array::from_iter_values(0..num_rows as i32));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    // ... existing tests for split_n_vecs ...

    #[test]
    fn test_record_batch_buffer_fails_fast_when_pool_exhausted() {
        // A bounded pool far too small to hold a decoded batch must make `add`
        // fail fast with `ResourcesExhausted` rather than buffer without bound.
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16));
        let mut buffer = RecordBatchBuffer::new(1_000, &memory_pool);

        // ~400 bytes of Int32 data, well over the 16-byte budget.
        let err = buffer.add(create_test_batch(100, None)).unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "expected ResourcesExhausted, got: {err}"
        );
        // Nothing retained (and nothing charged) after the failed grow.
        assert_eq!(buffer.buffered_bytes, 0);
        assert!(buffer.buffer.is_empty());
        assert_eq!(memory_pool.reserved(), 0);
    }

    #[test]
    fn test_record_batch_buffer_releases_reservation() {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let mut buffer = RecordBatchBuffer::new(100, &memory_pool);

        // Case 1: a batch that fills the buffer is yielded directly; its charge
        // must be released immediately.
        assert!(buffer.add(create_test_batch(100, None)).unwrap().is_some());
        assert_eq!(
            memory_pool.reserved(),
            0,
            "direct-yield must release its charge"
        );
        assert_eq!(buffer.buffered_bytes, 0);

        // Retained (buffered) batches keep their bytes charged to the pool.
        assert!(buffer.add(create_test_batch(30, None)).unwrap().is_none());
        assert!(buffer.add(create_test_batch(40, None)).unwrap().is_none());
        assert!(
            memory_pool.reserved() > 0,
            "retained batches must be charged"
        );
        assert_eq!(memory_pool.reserved(), buffer.buffered_bytes);

        // finish() flushes the buffer and releases everything.
        assert!(buffer.finish().unwrap().is_some());
        assert_eq!(memory_pool.reserved(), 0, "finish must release all charge");
    }

    #[test]
    fn test_record_batch_buffer_empty_buffer_large_batch() {
        let max_rows = 100;
        let mut buffer = test_record_batch_buffer(max_rows);
        let large_batch = create_test_batch(max_rows, None); // Exactly max_rows

        // Case 1: New batch is large and buffer is empty. Yield new batch directly.
        let result = buffer.add(large_batch.clone()).unwrap();
        assert!(result.is_some(), "Should yield the large batch");
        assert_eq!(result.unwrap().num_rows(), max_rows);
        assert_eq!(buffer.current_rows, 0, "Buffer current_rows should be 0");
        assert!(buffer.buffer.is_empty(), "Buffer should be empty");

        let large_batch_over = create_test_batch(max_rows + 10, None); // Over max_rows
        let result_over = buffer.add(large_batch_over.clone()).unwrap();
        assert!(result_over.is_some(), "Should yield the large batch");
        assert_eq!(result_over.unwrap().num_rows(), max_rows + 10);
        assert_eq!(buffer.current_rows, 0);
        assert!(buffer.buffer.is_empty());
    }

    #[test]
    fn test_record_batch_buffer_overflow_and_yield() {
        let max_rows = 100;
        let mut buffer = test_record_batch_buffer(max_rows);

        // Add some initial batches that don't fill the buffer
        let batch1 = create_test_batch(30, None);
        let batch1_rows = batch1.num_rows();
        assert!(buffer.add(batch1).unwrap().is_none());
        assert_eq!(buffer.current_rows, batch1_rows);
        assert_eq!(buffer.buffer.len(), 1);

        let batch2 = create_test_batch(40, None);
        let batch2_rows = batch2.num_rows();
        assert!(buffer.add(batch2).unwrap().is_none());
        assert_eq!(buffer.current_rows, batch1_rows + batch2_rows); // 30 + 40 = 70
        assert_eq!(buffer.buffer.len(), 2);

        // Add a batch that will cause an overflow
        let batch3 = create_test_batch(50, None); // 70 + 50 = 120 > 100
        let batch3_rows = batch3.num_rows();
        let result = buffer.add(batch3.clone()).unwrap();

        // Case 2: Buffer is not empty and adding new batch would overflow.
        // Yield combined content of current buffer, then add new batch to now-empty buffer.
        assert!(
            result.is_some(),
            "Should yield the combined batch from buffer"
        );
        assert_eq!(
            result.unwrap().num_rows(),
            batch1_rows + batch2_rows, // 70 rows
            "Yielded batch should have rows from batch1 and batch2"
        );
        assert_eq!(
            buffer.current_rows, batch3_rows,
            "Buffer current_rows should be batch3's rows"
        );
        assert_eq!(
            buffer.buffer.len(),
            1,
            "Buffer should now contain only batch3"
        );
        assert_eq!(buffer.buffer[0].num_rows(), batch3_rows);
    }

    #[test]
    fn test_record_batch_buffer_add_to_buffer_no_yield() {
        let max_rows = 100;
        let mut buffer = test_record_batch_buffer(max_rows);

        let batch1 = create_test_batch(30, None);
        let batch1_rows = batch1.num_rows();
        // Case 3: Buffer has space
        assert!(buffer.add(batch1).unwrap().is_none());
        assert_eq!(buffer.current_rows, batch1_rows);
        assert_eq!(buffer.buffer.len(), 1);

        let batch2 = create_test_batch(40, None);
        let batch2_rows = batch2.num_rows();
        assert!(buffer.add(batch2).unwrap().is_none());
        assert_eq!(buffer.current_rows, batch1_rows + batch2_rows);
        assert_eq!(buffer.buffer.len(), 2);
    }

    #[test]
    fn test_record_batch_buffer_finish_with_remaining() {
        let max_rows = 100;
        let mut buffer = test_record_batch_buffer(max_rows);

        let batch1 = create_test_batch(30, None);
        let batch1_rows = batch1.num_rows();
        buffer.add(batch1).unwrap();

        let batch2 = create_test_batch(40, None);
        let batch2_rows = batch2.num_rows();
        buffer.add(batch2).unwrap();

        let result = buffer.finish().unwrap();
        assert!(result.is_some(), "Finish should yield remaining batches");
        assert_eq!(result.unwrap().num_rows(), batch1_rows + batch2_rows);
    }

    #[test]
    fn test_record_batch_buffer_finish_empty() {
        let max_rows = 100;
        let buffer = test_record_batch_buffer(max_rows);
        let result = buffer.finish().unwrap();
        assert!(result.is_none(), "Finish on empty buffer should yield None");
    }

    #[test]
    fn test_record_batch_buffer_add_multiple_then_overflow() {
        let max_rows = 100;
        let mut buffer = test_record_batch_buffer(max_rows);

        // Add batches that sum up to less than max_rows
        buffer.add(create_test_batch(20, None)).unwrap(); // current_rows = 20
        buffer.add(create_test_batch(30, None)).unwrap(); // current_rows = 50
        buffer.add(create_test_batch(40, None)).unwrap(); // current_rows = 90
        assert_eq!(buffer.current_rows, 90);
        assert_eq!(buffer.buffer.len(), 3);

        // Add a batch that causes overflow
        let overflow_batch = create_test_batch(25, None); // 90 + 25 = 115 > 100
        let overflow_batch_rows = overflow_batch.num_rows();
        let yielded_batch = buffer.add(overflow_batch.clone()).unwrap();

        assert!(yielded_batch.is_some());
        assert_eq!(
            yielded_batch.unwrap().num_rows(),
            90,
            "Should yield the 90 rows from buffer"
        );
        assert_eq!(
            buffer.current_rows, overflow_batch_rows,
            "Buffer should have rows of the new batch"
        );
        assert_eq!(buffer.buffer.len(), 1);
        assert_eq!(buffer.buffer[0].num_rows(), overflow_batch_rows);

        // Finish the buffer
        let final_batch = buffer.finish().unwrap();
        assert!(final_batch.is_some());
        assert_eq!(final_batch.unwrap().num_rows(), overflow_batch_rows);
    }

    #[test]
    fn test_record_batch_buffer_add_batch_exactly_fills_then_overflows() {
        let max_rows = 100;
        let mut buffer = test_record_batch_buffer(max_rows);

        buffer.add(create_test_batch(50, None)).unwrap(); // current_rows = 50
        // This batch makes current_rows exactly max_rows
        let exact_fill_batch = create_test_batch(50, None);
        assert!(buffer.add(exact_fill_batch).unwrap().is_none()); // 50 + 50 = 100. No yield yet.
        assert_eq!(buffer.current_rows, 100);
        assert_eq!(buffer.buffer.len(), 2);

        // Next batch will cause overflow
        let overflow_batch = create_test_batch(10, None);
        let overflow_batch_rows = overflow_batch.num_rows();
        let yielded_batch = buffer.add(overflow_batch.clone()).unwrap();

        assert!(yielded_batch.is_some());
        assert_eq!(
            yielded_batch.unwrap().num_rows(),
            100,
            "Should yield the 100 rows"
        );
        assert_eq!(buffer.current_rows, overflow_batch_rows);
        assert_eq!(buffer.buffer.len(), 1);
        assert_eq!(buffer.buffer[0].num_rows(), overflow_batch_rows);
    }

    #[test]
    fn test_record_batch_buffer_add_to_empty_buffer_small_batch() {
        let max_rows = 100;
        let mut buffer = test_record_batch_buffer(max_rows);
        let small_batch = create_test_batch(10, None);
        let small_batch_rows = small_batch.num_rows();

        // Case 3: Buffer was empty and new batch is not "large".
        let result = buffer.add(small_batch).unwrap();
        assert!(result.is_none(), "Should not yield the small batch");
        assert_eq!(buffer.current_rows, small_batch_rows);
        assert_eq!(buffer.buffer.len(), 1);
    }

    #[tokio::test]
    async fn test_prefetch_reservation_enforces_memory_budget() {
        use datafusion::execution::memory_pool::{FairSpillPool, UnboundedMemoryPool};

        // A source file living in an in-memory FileIO that the prefetcher reads from.
        let source_io = FileIOBuilder::new("memory").build().unwrap();
        let source_path = "memory:/src/prefetch_budget_test.bin";
        let file_len = 1024 * 1024; // 1 MiB
        source_io
            .new_output(source_path)
            .unwrap()
            .write(vec![7u8; file_len].into())
            .await
            .unwrap();

        let make_task = || {
            let mut task = create_file_scan_task(file_len as u64, 1);
            task.data_file_path = source_path.to_owned();
            task
        };

        // Bounded pool smaller than the file: prefetch must fail cleanly with
        // ResourcesExhausted instead of buffering the whole file in memory.
        {
            let prefetcher = Prefetcher::new().unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(4 * 1024));
            let mut reservation = MemoryConsumer::new("test[prefetch]").register(&pool);

            let task = make_task();
            let mut ctx = FileTaskContext::from_task(&task);
            let err = prefetcher
                .prefetch_file_into_memory(&task, &mut ctx, &source_io, &mut reservation)
                .await
                .expect_err("prefetch should fail when the budget is smaller than the file");
            assert!(
                matches!(err, DataFusionError::ResourcesExhausted(_)),
                "expected ResourcesExhausted, got: {err:?}"
            );
            // A failed grow must leave nothing reserved.
            assert_eq!(reservation.size(), 0);
            assert_eq!(ctx.reserved_bytes, 0);
        }

        // Unbounded pool (the default): prefetch succeeds, the reservation tracks
        // the file's bytes, and clearing the file releases them.
        {
            let prefetcher = Prefetcher::new().unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let mut reservation = MemoryConsumer::new("test[prefetch]").register(&pool);

            let task = make_task();
            let mut ctx = FileTaskContext::from_task(&task);
            prefetcher
                .prefetch_file_into_memory(&task, &mut ctx, &source_io, &mut reservation)
                .await
                .expect("prefetch should succeed under the default unbounded pool");
            assert_eq!(ctx.reserved_bytes, file_len);
            assert_eq!(reservation.size(), file_len);

            prefetcher.clear_file(&ctx, &mut reservation).await.unwrap();
            assert_eq!(reservation.size(), 0);
        }
    }

    #[tokio::test]
    async fn test_prefetch_reserves_before_reading_file() {
        use datafusion::execution::memory_pool::FairSpillPool;

        // The prefetcher's source FileIO is EMPTY: it contains no file at the
        // task's path. If the reservation were taken only after the download (the
        // old ordering), the read would run first and fail with an IO/not-found
        // error. Because we reserve the declared size up-front, an over-budget
        // prefetch is rejected with `ResourcesExhausted` before the read is ever
        // attempted -- so seeing that error (rather than an IO error against the
        // missing file) proves the read never started.
        let prefetcher = Prefetcher::new().unwrap();
        let empty_source_io = FileIOBuilder::new("memory").build().unwrap();

        let declared_len = 1024 * 1024; // 1 MiB, larger than the pool below
        let mut task = create_file_scan_task(declared_len, 1);
        task.data_file_path = "memory:/src/does_not_exist.bin".to_owned();

        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(4 * 1024));
        let mut reservation = MemoryConsumer::new("test[prefetch]").register(&pool);

        let mut ctx = FileTaskContext::from_task(&task);
        let err = prefetcher
            .prefetch_file_into_memory(&task, &mut ctx, &empty_source_io, &mut reservation)
            .await
            .expect_err("prefetch must fail before reading when the budget is too small");

        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "expected ResourcesExhausted from the up-front reservation (proving the \
             read never started), got: {err:?}"
        );
        // The failed up-front grow must leave nothing reserved or recorded.
        assert_eq!(reservation.size(), 0);
        assert_eq!(ctx.reserved_bytes, 0);
    }
}
