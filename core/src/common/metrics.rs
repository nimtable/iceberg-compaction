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

use mixtrics::metrics::{BoxedCounterVec, BoxedHistogramVec, BoxedRegistry, Buckets};
use std::{borrow::Cow, sync::Arc};

use crate::executor::RewriteFilesStat;

pub struct Metrics {
    // commit metrics
    pub compaction_commit_counter: BoxedCounterVec,
    pub compaction_duration: BoxedHistogramVec,
    pub compaction_commit_duration: BoxedHistogramVec,
    pub compaction_commit_failed_counter: BoxedCounterVec,
    pub compaction_executor_error_counter: BoxedCounterVec,

    // New hierarchical metrics for plan-level analysis
    pub compaction_plan_execution_duration: BoxedHistogramVec, // Individual plan execution time
    pub compaction_plan_file_count: BoxedHistogramVec,         // Number of files processed per plan
    pub compaction_plan_size_bytes: BoxedHistogramVec,         // Bytes processed per plan

    // input/output metrics
    pub compaction_input_files_count: BoxedCounterVec,
    pub compaction_output_files_count: BoxedCounterVec,
    pub compaction_input_bytes_total: BoxedCounterVec,
    pub compaction_output_bytes_total: BoxedCounterVec,

    // DataFusion processing metrics
    pub compaction_datafusion_records_processed_total: BoxedCounterVec,
    pub compaction_datafusion_batch_fetch_duration: BoxedHistogramVec,
    pub compaction_datafusion_batch_write_duration: BoxedHistogramVec,
    pub compaction_datafusion_bytes_processed_total: BoxedCounterVec,

    // DataFusion distribution metrics
    pub compaction_datafusion_batch_row_count_dist: BoxedHistogramVec,
    pub compaction_datafusion_batch_bytes_dist: BoxedHistogramVec,
}

impl Metrics {
    pub fn new(registry: BoxedRegistry) -> Self {
        // Bucket constants for large-scale compaction support
        // Designed to handle: ~1 hour duration, ~1TB data size, ~4096 files
        const COMPACTION_DURATION_BUCKET_START_MS: f64 = 1000.0; // 1s
        const COMPACTION_DURATION_BUCKET_FACTOR: f64 = 4.0; // x4 per bucket
        const COMPACTION_DURATION_BUCKET_COUNT: usize = 8; // 8 buckets: 1s ~ 16384s (~4.5 hours)

        const PLAN_EXEC_DURATION_BUCKET_START_MS: f64 = 1000.0; // 1s
        const PLAN_EXEC_DURATION_BUCKET_FACTOR: f64 = 4.0; // x4 per bucket
        const PLAN_EXEC_DURATION_BUCKET_COUNT: usize = 8; // 8 buckets: 1s ~ 16384s (~4.5 hours)

        const PLAN_FILE_COUNT_BUCKET_START: f64 = 1.0; // 1 file
        const PLAN_FILE_COUNT_BUCKET_FACTOR: f64 = 2.0; // x2 per bucket
        const PLAN_FILE_COUNT_BUCKET_COUNT: usize = 13; // 13 buckets: 1 ~ 4096 files

        const PLAN_SIZE_BUCKET_START_BYTES: f64 = 1024.0 * 1024.0; // 1MB
        const PLAN_SIZE_BUCKET_FACTOR: f64 = 4.0; // x4 per bucket
        const PLAN_SIZE_BUCKET_COUNT: usize = 12; // 12 buckets: 1MB ~ 16TB

        let compaction_commit_counter = registry.register_counter_vec(
            "iceberg_compaction_commit_counter".into(),
            "iceberg-compaction compaction total commit counts".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_duration = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_duration".into(),
            "iceberg-compaction compaction duration in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                COMPACTION_DURATION_BUCKET_START_MS,
                COMPACTION_DURATION_BUCKET_FACTOR,
                COMPACTION_DURATION_BUCKET_COUNT,
            ),
        );

        // 10ms 100ms 1s 10s 100s
        let compaction_commit_duration = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_commit_duration".into(),
            "iceberg-compaction compaction commit duration in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                10.0, 10.0, 5, // Start at 10ms, multiply each bucket by 10, up to 5 buckets
            ),
        );

        let compaction_commit_failed_counter = registry.register_counter_vec(
            "iceberg_compaction_commit_failed_counter".into(),
            "iceberg-compaction compaction commit failed counts".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_executor_error_counter = registry.register_counter_vec(
            "iceberg_compaction_executor_error_counter".into(),
            "iceberg-compaction compaction executor error counts".into(),
            &["catalog_name", "table_ident"],
        );

        // === New plan-level metrics ===
        let compaction_plan_execution_duration = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_plan_execution_duration".into(),
            "Duration for executing individual compaction plans in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                PLAN_EXEC_DURATION_BUCKET_START_MS,
                PLAN_EXEC_DURATION_BUCKET_FACTOR,
                PLAN_EXEC_DURATION_BUCKET_COUNT,
            ),
        );

        let compaction_plan_file_count = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_plan_file_count".into(),
            "Number of files processed by individual compaction plans".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                PLAN_FILE_COUNT_BUCKET_START,
                PLAN_FILE_COUNT_BUCKET_FACTOR,
                PLAN_FILE_COUNT_BUCKET_COUNT,
            ),
        );

        let compaction_plan_size_bytes = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_plan_size_bytes".into(),
            "Bytes processed by individual compaction plans".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                PLAN_SIZE_BUCKET_START_BYTES,
                PLAN_SIZE_BUCKET_FACTOR,
                PLAN_SIZE_BUCKET_COUNT,
            ),
        );

        // === Input/Output metrics registration ===
        let compaction_input_files_count = registry.register_counter_vec(
            "iceberg_compaction_input_files_count".into(),
            "Number of input files being compacted".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_output_files_count = registry.register_counter_vec(
            "iceberg_compaction_output_files_count".into(),
            "Number of output files from compaction".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_input_bytes_total = registry.register_counter_vec(
            "iceberg_compaction_input_bytes_total".into(),
            "Total number of bytes in input files for compaction".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_output_bytes_total = registry.register_counter_vec(
            "iceberg_compaction_output_bytes_total".into(),
            "Total number of bytes in output files from compaction".into(),
            &["catalog_name", "table_ident"],
        );

        // === DataFusion processing metrics ===
        let compaction_datafusion_records_processed_total = registry.register_counter_vec(
            "iceberg_compaction_datafusion_records_processed_total".into(),
            "Total number of records processed by DataFusion during compaction".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_datafusion_batch_fetch_duration = registry
            .register_histogram_vec_with_buckets(
                "iceberg_compaction_datafusion_batch_fetch_duration".into(),
                "Duration of fetching individual record batches in DataFusion (milliseconds)"
                    .into(),
                &["catalog_name", "table_ident"],
                Buckets::exponential(
                    1.0, 10.0, 6, // 1ms, 10ms, 100ms, 1s, 10s, 100s
                ),
            );

        let compaction_datafusion_batch_write_duration = registry
            .register_histogram_vec_with_buckets(
                "iceberg_compaction_datafusion_batch_write_duration".into(),
                "Duration of writing individual record batches in DataFusion (milliseconds)".into(),
                &["catalog_name", "table_ident"],
                Buckets::exponential(
                    1.0, 10.0, 6, // 1ms, 10ms, 100ms, 1s, 10s, 100s
                ),
            );

        let compaction_datafusion_bytes_processed_total = registry.register_counter_vec(
            "iceberg_compaction_datafusion_bytes_processed_total".into(),
            "Total number of bytes processed by DataFusion during compaction".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_datafusion_batch_row_count_dist = registry
            .register_histogram_vec_with_buckets(
                "iceberg_compaction_datafusion_batch_row_count_dist".into(),
                "Distribution of row counts in record batches processed by DataFusion".into(),
                &["catalog_name", "table_ident"],
                Buckets::exponential(100.0, 2.0, 10), // 100, 200, 400, ..., 51200 rows
            );

        let compaction_datafusion_batch_bytes_dist = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_datafusion_batch_bytes_dist".into(),
            "Distribution of byte sizes of record batches processed by DataFusion".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(1024.0 * 64.0, 2.0, 12), // 64KB, 128KB, 256KB, ..., 128MB
        );

        Self {
            compaction_commit_counter,
            compaction_duration,
            compaction_commit_duration,
            compaction_commit_failed_counter,
            compaction_executor_error_counter,

            // New plan-level metrics
            compaction_plan_execution_duration,
            compaction_plan_file_count,
            compaction_plan_size_bytes,

            compaction_input_files_count,
            compaction_output_files_count,
            compaction_input_bytes_total,
            compaction_output_bytes_total,

            // datafusion metrics
            compaction_datafusion_records_processed_total,
            compaction_datafusion_batch_fetch_duration,
            compaction_datafusion_batch_write_duration,
            compaction_datafusion_bytes_processed_total,
            compaction_datafusion_batch_row_count_dist,
            compaction_datafusion_batch_bytes_dist,
        }
    }
}

/// Helper for recording compaction metrics
/// Focuses on business-level metrics that can be accurately measured
#[derive(Clone)]
pub struct CompactionMetricsRecorder {
    metrics: Arc<Metrics>,
    catalog_name: Cow<'static, str>,
    table_ident: Cow<'static, str>,
}

impl CompactionMetricsRecorder {
    pub fn new(
        metrics: Arc<Metrics>,
        catalog_name: impl Into<Cow<'static, str>>,
        table_ident: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            metrics,
            catalog_name: catalog_name.into(),
            table_ident: table_ident.into(),
        }
    }

    /// Helper to create label vector for metrics
    fn label_vec(&self) -> [std::borrow::Cow<'static, str>; 2] {
        [self.catalog_name.clone(), self.table_ident.clone()]
    }

    /// Record compaction duration (milliseconds)
    pub fn record_compaction_duration(&self, duration_ms: f64) {
        if duration_ms == 0.0 || !duration_ms.is_finite() {
            return; // Avoid recording zero duration
        }

        let label_vec = self.label_vec();
        self.metrics
            .compaction_duration
            .histogram(&label_vec)
            .record(duration_ms);
    }

    /// Record commit duration (milliseconds)
    pub fn record_commit_duration(&self, duration_ms: f64) {
        if duration_ms == 0.0 || !duration_ms.is_finite() {
            return; // Avoid recording zero duration
        }

        let label_vec = self.label_vec();
        self.metrics
            .compaction_commit_duration
            .histogram(&label_vec)
            .record(duration_ms);
    }

    /// Record individual plan execution duration (milliseconds)
    pub fn record_plan_execution_duration(&self, duration_ms: f64) {
        if duration_ms == 0.0 || !duration_ms.is_finite() {
            return; // Avoid recording zero duration
        }

        let label_vec = self.label_vec();
        self.metrics
            .compaction_plan_execution_duration
            .histogram(&label_vec)
            .record(duration_ms);
    }

    /// Record the number of files processed by a plan
    pub fn record_plan_file_count(&self, file_count: usize) {
        if file_count == 0 {
            return; // Avoid recording zero file count
        }

        let label_vec = self.label_vec();

        self.metrics
            .compaction_plan_file_count
            .histogram(&label_vec)
            .record(file_count as f64);
    }

    /// Record the bytes processed by a plan
    pub fn record_plan_size_bytes(&self, size_bytes: u64) {
        if size_bytes == 0 {
            return; // Avoid recording zero size
        }

        let label_vec = self.label_vec();

        self.metrics
            .compaction_plan_size_bytes
            .histogram(&label_vec)
            .record(size_bytes as f64);
    }

    /// Record successful compaction commit
    pub fn record_commit_success(&self) {
        let label_vec = self.label_vec();

        self.metrics
            .compaction_commit_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record compaction commit failure
    pub fn record_commit_failure(&self) {
        let label_vec = self.label_vec();

        self.metrics
            .compaction_commit_failed_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record executor error
    pub fn record_executor_error(&self) {
        let label_vec = self.label_vec();

        self.metrics
            .compaction_executor_error_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record complete compaction metrics
    /// This is a convenience method that records all basic compaction metrics
    pub fn record_compaction_complete(&self, stats: &RewriteFilesStat) {
        if stats.input_files_count == 0 && stats.output_files_count == 0 {
            return; // No files processed, skip metrics
        }

        let label_vec = self.label_vec();

        if stats.input_files_count > 0 {
            self.metrics
                .compaction_input_files_count
                .counter(&label_vec)
                .increase(stats.input_files_count as u64);
        }

        if stats.input_total_bytes > 0 {
            self.metrics
                .compaction_input_bytes_total
                .counter(&label_vec)
                .increase(stats.input_total_bytes);
        }

        // output
        if stats.output_files_count > 0 {
            self.metrics
                .compaction_output_files_count
                .counter(&label_vec)
                .increase(stats.output_files_count as u64);
        }

        if stats.output_total_bytes > 0 {
            self.metrics
                .compaction_output_bytes_total
                .counter(&label_vec)
                .increase(stats.output_total_bytes);
        }
    }

    pub fn record_datafusion_batch_fetch_duration(&self, fetch_duration_ms: f64) {
        if fetch_duration_ms <= 0.0 || !fetch_duration_ms.is_finite() {
            return; // Avoid recording zero, negative, or invalid durations
        }

        let label_vec = self.label_vec();

        self.metrics
            .compaction_datafusion_batch_fetch_duration
            .histogram(&label_vec)
            .record(fetch_duration_ms); // Already in milliseconds
    }

    pub fn record_datafusion_batch_write_duration(&self, write_duration_ms: f64) {
        if write_duration_ms <= 0.0 || !write_duration_ms.is_finite() {
            return; // Avoid recording zero, negative, or invalid durations
        }

        let label_vec = self.label_vec();

        self.metrics
            .compaction_datafusion_batch_write_duration
            .histogram(&label_vec)
            .record(write_duration_ms); // Already in milliseconds
    }

    pub fn record_batch_stats(&self, record_count: u64, batch_bytes: u64) {
        if record_count == 0 && batch_bytes == 0 {
            return; // No records or bytes, skip metrics
        }

        let label_vec = self.label_vec();

        if record_count > 0 {
            self.metrics
                .compaction_datafusion_records_processed_total
                .counter(&label_vec)
                .increase(record_count);

            self.metrics
                .compaction_datafusion_batch_row_count_dist
                .histogram(&label_vec)
                .record(record_count as f64);
        }

        if batch_bytes > 0 {
            self.metrics
                .compaction_datafusion_bytes_processed_total
                .counter(&label_vec)
                .increase(batch_bytes);

            self.metrics
                .compaction_datafusion_batch_bytes_dist
                .histogram(&label_vec)
                .record(batch_bytes as f64);
        }
    }
}
