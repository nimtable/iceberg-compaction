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

use std::any::Any;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::vec;

use async_stream::try_stream;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef as ArrowSchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg_datafusion::physical_plan::expr_to_predicate::convert_filters_to_predicate;
use iceberg_datafusion::to_datafusion_error;
use tokio; // 添加 tokio 导入

use super::datafusion_processor::SYS_HIDDEN_SEQ_NUM;

/// Calculate optimal file concurrency for each partition based on configuration
///
/// This function implements safety measures to prevent S3 connection timeout issues:
/// 1. Limits maximum concurrent connections per partition
/// 2. Ensures fair distribution across partitions
/// 3. Prevents overwhelming S3 service with too many simultaneous connections
///
/// # Arguments
/// * `total_file_scan_concurrency` - Total concurrent file reads across all partitions
/// * `batch_parallelism` - Number of partitions (parallel batches)
/// * `file_count_in_partition` - Number of files in this specific partition
///
/// # Returns
/// The optimal concurrency for this partition, ensuring:
/// 1. Fair distribution of concurrency across partitions
/// 2. Not exceeding the number of files in this partition
/// 3. Conservative limits to prevent S3 connection timeouts
fn calculate_partition_file_concurrency(
    total_file_scan_concurrency: usize,
    batch_parallelism: usize,
    file_count_in_partition: usize,
) -> usize {
    if file_count_in_partition == 0 {
        return 0;
    }

    // Calculate base concurrency per partition
    let base_concurrency = total_file_scan_concurrency / batch_parallelism.max(1);

    // 🔥 关键修改：大幅提高并发限制
    const MAX_CONCURRENT_FILES_PER_PARTITION: usize = 32; // 从8提高到32
    const MIN_CONCURRENT_FILES_PER_PARTITION: usize = 2; // 确保最小并发度

    // Ensure proper concurrency range
    let safe_concurrency = base_concurrency
        .clamp(
            MIN_CONCURRENT_FILES_PER_PARTITION,
            MAX_CONCURRENT_FILES_PER_PARTITION,
        )
        .min(file_count_in_partition);

    // 详细诊断日志
    tracing::info!(
        "🔧 Partition concurrency calculation - Total scan concurrency: {}, Partitions: {}, Files in partition: {}, Base per partition: {}, Final concurrency: {}",
        total_file_scan_concurrency, batch_parallelism, file_count_in_partition, base_concurrency, safe_concurrency
    );

    // 警告：如果并发度太低，可能影响性能
    if safe_concurrency < 8 {
        tracing::warn!(
            "⚠️  Low file concurrency detected ({}) - consider increasing file_scan_concurrency from {} to {}",
            safe_concurrency, total_file_scan_concurrency, batch_parallelism * 16
        );
    }

    safe_concurrency
}

struct RecordBatchBuffer {
    buffer: Vec<RecordBatch>,
    current_rows: usize,

    max_record_batch_rows: usize,
}

impl RecordBatchBuffer {
    fn new(max_record_batch_rows: usize) -> Self {
        Self {
            buffer: vec![],
            current_rows: 0,
            max_record_batch_rows,
        }
    }

    fn add(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>, DataFusionError> {
        // Case 1: New batch itself is large enough and buffer is empty or too small to be significant
        if batch.num_rows() >= self.max_record_batch_rows && self.buffer.is_empty() {
            // Buffer was empty, yield current large batch directly
            return Ok(Some(batch));
        }

        // Case 2: Buffer will overflow with the new batch
        if !self.buffer.is_empty()
            && (self.current_rows + batch.num_rows() > self.max_record_batch_rows)
        {
            let combined = self.finish_internal()?; // Drain and combine buffer
            self.current_rows = batch.num_rows();
            self.buffer.push(batch); // Add current batch to now-empty buffer
            return Ok(combined); // Return the combined batch from buffer
        }

        // Case 3: Buffer has space
        self.current_rows += batch.num_rows();
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
    plan_properties: PlanProperties,
    projection: Option<Vec<String>>,
    predicates: Option<Predicate>,
    file_io: FileIO,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    max_record_batch_rows: usize,
    file_scan_concurrency: usize,
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
        batch_parallelism: usize,
        max_record_batch_rows: usize,
        file_scan_concurrency: usize,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let file_scan_tasks_group = split_n_vecs(file_scan_tasks, batch_parallelism);
        let plan_properties =
            Self::compute_properties(output_schema.clone(), file_scan_tasks_group.len());
        let projection = get_column_names(schema.clone(), projection);
        let predicates = convert_filters_to_predicate(filters);

        Self {
            file_scan_tasks_group,
            plan_properties,
            projection,
            predicates,
            file_io: file_io.clone(),
            need_seq_num,
            need_file_path_and_pos,
            max_record_batch_rows,
            file_scan_concurrency,
        }
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef, partitioning_size: usize) -> PlanProperties {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partitioning_size),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
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

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let file_scan_tasks = &self.file_scan_tasks_group[partition];
        let partition_concurrency = calculate_partition_file_concurrency(
            self.file_scan_concurrency,
            self.file_scan_tasks_group.len(), // batch_parallelism
            file_scan_tasks.len(),
        );

        let fut = get_batch_stream(
            self.file_io.clone(),
            file_scan_tasks.clone(),
            self.need_seq_num,
            self.need_file_path_and_pos,
            self.max_record_batch_rows,
            partition_concurrency,
        );
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Gets a stream of record batches from a list of file scan tasks
///
/// TODO: Add better S3 connection management to prevent timeout issues:
/// - Implement connection pooling with proper lifecycle management
/// - Add retry logic at the individual batch level (not stream level)
/// - Consider implementing backpressure to prevent too many idle connections
/// - Add monitoring for connection timeouts and appropriate logging
async fn get_batch_stream(
    file_io: FileIO,
    file_scan_tasks: Vec<FileScanTask>,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    max_record_batch_rows: usize,
    partition_file_concurrency: usize,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let total_files = file_scan_tasks.len();
    let diagnostics = BottleneckDiagnostics::new();
    let active_file_counter = Arc::new(AtomicUsize::new(0));

    tracing::info!(
        "🚀 Starting file scan - Files: {}, Concurrency: {}, Target batch size: {}",
        total_files,
        partition_file_concurrency,
        max_record_batch_rows
    );

    // 启动实时监控
    let diag_clone = diagnostics.clone();
    let counter_clone = active_file_counter.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let current_files = counter_clone.load(Ordering::Relaxed);

            // 更新最大并发文件数
            loop {
                let current_max = diag_clone.max_concurrent_files.load(Ordering::Relaxed);
                if current_files <= current_max {
                    break;
                }
                if diag_clone
                    .max_concurrent_files
                    .compare_exchange_weak(
                        current_max,
                        current_files,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    break;
                }
            }

            tracing::info!("📊 Real-time: {} files reading concurrently", current_files);

            // 🚨 实时警告
            if current_files < partition_file_concurrency / 2 {
                tracing::warn!(
                    "⚠️  File concurrency low: {}/{}",
                    current_files,
                    partition_file_concurrency
                );
            }
        }
    });

    let stream = try_stream! {
        let mut record_batch_buffer = RecordBatchBuffer::new(max_record_batch_rows);
        let mut batch_count = 0;
        let start_time = Instant::now();

        // Create concurrent streams for all file tasks with diagnostics
        let file_streams = futures::stream::iter(file_scan_tasks.into_iter().enumerate().map(|(file_idx, task)| {
            let file_io = file_io.clone();
            let file_path = task.data_file_path.clone();
            let data_file_content = task.data_file_content;
            let sequence_number = task.sequence_number;
            let file_size = task.length;
            let diagnostics = diagnostics.clone();
            let active_counter = active_file_counter.clone();

            async move {
                // 📈 开始文件读取
                active_counter.fetch_add(1, Ordering::Relaxed);
                let file_start = Instant::now();

                tracing::debug!("📂 Starting file {}: {:.2}MB", file_idx, file_size as f64 / 1024.0 / 1024.0);

                // 1. 文件打开阶段 - 纯I/O
                let open_start = Instant::now();
                let task_stream = futures::stream::iter(vec![Ok(task)]).boxed();
                let arrow_reader_builder = ArrowReaderBuilder::new(file_io).with_batch_size(max_record_batch_rows);
                let batch_stream = arrow_reader_builder.build()
                    .read(task_stream)
                    .await
                    .map_err(to_datafusion_error)?;
                let open_time = open_start.elapsed();

                // 2. 数据读取和处理阶段
                let read_start = Instant::now();
                let mut processed_batches = Vec::new();
                let mut total_rows = 0;
                let mut index_start = 0i64;

                // Transform the stream to add metadata
                let transformed_stream = batch_stream.map(move |batch_result| {
                    let batch = batch_result.map_err(to_datafusion_error)?;
                    let current_index = index_start;
                    index_start += batch.num_rows() as i64;

                    let processed_batch = match data_file_content {
                        iceberg::spec::DataContentType::Data => {
                            let mut batch = batch;
                            // add sequence number if needed
                            if need_seq_num {
                                batch = add_seq_num_into_batch(batch, sequence_number)?;
                            }
                            // add file path and position if needed
                            if need_file_path_and_pos {
                                batch = add_file_path_pos_into_batch(batch, &file_path, current_index)?;
                            }
                            batch
                        }
                        iceberg::spec::DataContentType::PositionDeletes => {
                            batch
                        },
                        iceberg::spec::DataContentType::EqualityDeletes => {
                            add_seq_num_into_batch(batch, sequence_number)?
                        },
                    };
                    Ok::<RecordBatch, DataFusionError>(processed_batch)
                });

                // 收集所有批次
                pin_mut!(transformed_stream);
                while let Some(batch) = transformed_stream.try_next().await? {
                    total_rows += batch.num_rows();
                    processed_batches.push(batch);
                }

                let read_time = read_start.elapsed();
                let total_file_time = file_start.elapsed();
                active_counter.fetch_sub(1, Ordering::Relaxed);

                // 📊 更新诊断数据
                diagnostics.file_open_time.fetch_add(open_time.as_nanos() as u64, Ordering::Relaxed);
                diagnostics.total_io_wait_time.fetch_add((open_time + read_time).as_nanos() as u64, Ordering::Relaxed);
                diagnostics.time_in_file_read.fetch_add(total_file_time.as_nanos() as u64, Ordering::Relaxed);

                // 🔍 单文件分析
                let file_size_mb = file_size as f64 / 1024.0 / 1024.0;
                let throughput = file_size_mb / total_file_time.as_secs_f64();

                let open_percent = (open_time.as_secs_f64() / total_file_time.as_secs_f64()) * 100.0;
                let read_percent = (read_time.as_secs_f64() / total_file_time.as_secs_f64()) * 100.0;

                tracing::debug!(
                    "📁 File {}: {:.1}MB, {:.1}MB/s, {} rows, {} batches | Open: {:.1}%, Read: {:.1}%",
                    file_idx, file_size_mb, throughput, total_rows, processed_batches.len(),
                    open_percent, read_percent
                );

                // 🚨 单文件瓶颈检测
                if throughput < 10.0 {
                    tracing::warn!("🐌 Very slow file {}: {:.1}MB/s", file_idx, throughput);
                }
                if open_percent > 50.0 {
                    tracing::warn!("🐌 Slow file open {}: {:.1}% of time", file_idx, open_percent);
                }

                Ok::<Vec<RecordBatch>, DataFusionError>(processed_batches)
            }
        }))
        .buffer_unordered(partition_file_concurrency); // Use calculated concurrency for this partition

        // Process batches as they come from concurrent file streams
        pin_mut!(file_streams);
        while let Some(batches_result) = file_streams.try_next().await? {
            for batch in batches_result {
                let buffer_start = Instant::now();
                if let Some(buffered_batch) = record_batch_buffer.add(batch)? {
                    batch_count += 1;
                    let buffer_time = buffer_start.elapsed();
                    diagnostics.time_in_buffer_ops.fetch_add(buffer_time.as_nanos() as u64, Ordering::Relaxed);

                    tracing::debug!("📦 Produced batch {}: {} rows", batch_count, buffered_batch.num_rows());
                    yield buffered_batch;
                }
            }
        }

        if let Some(final_batch) = record_batch_buffer.finish()? {
            batch_count += 1;
            tracing::debug!("📦 Final batch {}: {} rows", batch_count, final_batch.num_rows());
            yield final_batch;
        }

        let total_time = start_time.elapsed();
        tracing::info!(
            "📊 Partition complete - {} files, {} batches produced in {:?}",
            total_files, batch_count, total_time
        );

        // 🎯 最终诊断
        diagnostics.diagnose_bottleneck();
    };
    Ok(Box::pin(stream))
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
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))
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
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))
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

/// 瓶颈诊断工具，用于精确识别性能瓶颈
#[derive(Debug)]
pub struct BottleneckDiagnostics {
    // 并行度指标
    max_concurrent_files: AtomicUsize,
    max_concurrent_partitions: AtomicUsize,

    // I/O指标
    total_io_wait_time: AtomicU64,
    total_cpu_time: AtomicU64,
    file_open_time: AtomicU64,

    // 时间分布
    time_in_file_read: AtomicU64,
    time_in_datafusion: AtomicU64,
    time_in_buffer_ops: AtomicU64,

    start_time: Instant,
}

impl BottleneckDiagnostics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            max_concurrent_files: AtomicUsize::new(0),
            max_concurrent_partitions: AtomicUsize::new(0),
            total_io_wait_time: AtomicU64::new(0),
            total_cpu_time: AtomicU64::new(0),
            file_open_time: AtomicU64::new(0),
            time_in_file_read: AtomicU64::new(0),
            time_in_datafusion: AtomicU64::new(0),
            time_in_buffer_ops: AtomicU64::new(0),
            start_time: Instant::now(),
        })
    }

    pub fn diagnose_bottleneck(&self) {
        let total_time = self.start_time.elapsed().as_nanos() as u64;
        let io_time = self.total_io_wait_time.load(Ordering::Relaxed);
        let cpu_time = self.total_cpu_time.load(Ordering::Relaxed);
        let file_read_time = self.time_in_file_read.load(Ordering::Relaxed);
        let datafusion_time = self.time_in_datafusion.load(Ordering::Relaxed);

        let max_files = self.max_concurrent_files.load(Ordering::Relaxed);
        let max_partitions = self.max_concurrent_partitions.load(Ordering::Relaxed);

        // 时间分布分析
        let io_percent = (io_time as f64 / total_time as f64) * 100.0;
        let cpu_percent = (cpu_time as f64 / total_time as f64) * 100.0;
        let file_read_percent = (file_read_time as f64 / total_time as f64) * 100.0;
        let datafusion_percent = (datafusion_time as f64 / total_time as f64) * 100.0;

        // 🎯 关键诊断逻辑
        let bottleneck_analysis = if io_percent > 60.0 {
            "PRIMARY BOTTLENECK: I/O BOUND - Storage/Network too slow"
        } else if max_files < 16 {
            "PRIMARY BOTTLENECK: LOW FILE CONCURRENCY - increase file_scan_concurrency"
        } else if max_partitions < 8 {
            "PRIMARY BOTTLENECK: LOW DATAFUSION PARALLELISM - check target_partitions"
        } else if datafusion_percent < 30.0 {
            "PRIMARY BOTTLENECK: DATAFUSION UNDERUTILIZED - query optimization needed"
        } else {
            "No obvious bottleneck - performance balanced"
        };

        // 🔍 单条日志包含所有诊断信息
        tracing::info!(
            "🔍 BOTTLENECK DIAGNOSIS - Time: I/O {:.1}%, CPU {:.1}%, FileRead {:.1}%, DataFusion {:.1}% | Parallelism: MaxFiles {}, MaxPartitions {} | Result: {}",
            io_percent, cpu_percent, file_read_percent, datafusion_percent,
            max_files, max_partitions, bottleneck_analysis
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, SchemaBuilder};
    use iceberg::scan::FileScanTask;
    use iceberg::spec::{DataContentType, Schema};
    use std::sync::Arc;

    fn create_file_scan_task(length: u64, file_id: u64) -> FileScanTask {
        FileScanTask {
            length,
            start: 0,
            record_count: Some(0),
            data_file_path: format!("test_{}.parquet", file_id),
            data_file_content: DataContentType::Data,
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: vec![],
            file_size_in_bytes: 0,
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

    #[test]
    fn test_record_batch_buffer_empty_buffer_large_batch() {
        let max_rows = 100;
        let mut buffer = RecordBatchBuffer::new(max_rows);
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
        let mut buffer = RecordBatchBuffer::new(max_rows);

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
        let mut buffer = RecordBatchBuffer::new(max_rows);

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
        let mut buffer = RecordBatchBuffer::new(max_rows);

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
        let buffer = RecordBatchBuffer::new(max_rows);
        let result = buffer.finish().unwrap();
        assert!(result.is_none(), "Finish on empty buffer should yield None");
    }

    #[test]
    fn test_record_batch_buffer_add_multiple_then_overflow() {
        let max_rows = 100;
        let mut buffer = RecordBatchBuffer::new(max_rows);

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
        let mut buffer = RecordBatchBuffer::new(max_rows);

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
        let mut buffer = RecordBatchBuffer::new(max_rows);
        let small_batch = create_test_batch(10, None);
        let small_batch_rows = small_batch.num_rows();

        // Case 3: Buffer was empty and new batch is not "large".
        let result = buffer.add(small_batch).unwrap();
        assert!(result.is_none(), "Should not yield the small batch");
        assert_eq!(buffer.current_rows, small_batch_rows);
        assert_eq!(buffer.buffer.len(), 1);
    }

    #[test]
    fn test_calculate_partition_file_concurrency() {
        // Test with no files
        assert_eq!(calculate_partition_file_concurrency(8, 4, 0), 0);

        // Test normal distribution: 8 total concurrency, 4 partitions = 2 per partition
        assert_eq!(calculate_partition_file_concurrency(8, 4, 10), 2);
        assert_eq!(calculate_partition_file_concurrency(8, 4, 2), 2);
        assert_eq!(calculate_partition_file_concurrency(8, 4, 1), 2); // At least MIN_CONCURRENT_FILES_PER_PARTITION (2)

        // Test safety limits: should be capped at MAX_CONCURRENT_FILES_PER_PARTITION (32)
        assert_eq!(calculate_partition_file_concurrency(64, 4, 20), 16); // 64/4=16, within limit
        assert_eq!(calculate_partition_file_concurrency(200, 4, 50), 32); // 200/4=50, but capped at 32
        assert_eq!(calculate_partition_file_concurrency(100, 1, 50), 32); // 100/1=100, but capped at 32

        // Test edge cases
        assert_eq!(calculate_partition_file_concurrency(1, 4, 10), 2); // At least MIN_CONCURRENT_FILES_PER_PARTITION (2)
        assert_eq!(calculate_partition_file_concurrency(16, 2, 5), 5); // Limited by file count
        assert_eq!(calculate_partition_file_concurrency(16, 1, 10), 10); // Single partition, limited by file count

        // Test uneven distribution
        assert_eq!(calculate_partition_file_concurrency(7, 4, 10), 2); // 7/4 = 1, but at least 2
        assert_eq!(calculate_partition_file_concurrency(9, 4, 10), 2); // 9/4 = 2
    }
}
