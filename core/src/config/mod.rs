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

use crate::common::available_parallelism;
use derive_builder::Builder;
use parquet::{basic::Compression, file::properties::WriterProperties};

pub const DEFAULT_PREFIX: &str = "iceberg-compact";
pub const DEFAULT_EXECUTOR_PARALLELISM: usize = 1;
pub const DEFAULT_OUTPUT_PARALLELISM: usize = 1;
pub const DEFAULT_TARGET_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
pub const DEFAULT_VALIDATE_COMPACTION: bool = false;
pub const DEFAULT_MAX_RECORD_BATCH_ROWS: usize = 1024;
pub const DEFAULT_MAX_CONCURRENT_CLOSES: usize = 4;
pub const DEFAULT_NORMALIZED_COLUMN_IDENTIFIERS: bool = true;
pub const DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION: bool = false;
pub const DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR: f64 = 0.3;
pub const DEFAULT_SMALL_FILE_THRESHOLD: u64 = 32 * 1024 * 1024; // 32 MB
pub const DEFAULT_MAX_TASK_TOTAL_SIZE: u64 = 50 * 1024 * 1024 * 1024; // 50 GB
pub const DEFAULT_MIN_SIZE_PER_PARTITION: u64 = 512 * 1024 * 1024; // 512 MB per partition
pub const DEFAULT_MAX_FILE_COUNT_PER_PARTITION: usize = 32; // 32 files per partition
pub const DEFAULT_MIN_FILE_COUNT: usize = 0; // default unlimited

// Helper function for the default WriterProperties
fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(
            concat!("iceberg-compaction version ", env!("CARGO_PKG_VERSION")).to_owned(),
        )
        .build()
}

#[derive(Builder, Debug, Default, Clone)]
pub struct RuntimeConfig {
    /// The number of parallel tasks to execute. used for scan and join.
    #[builder(default = "DEFAULT_EXECUTOR_PARALLELISM")]
    pub executor_parallelism: usize,
    /// The number of parallel tasks to output. Only used for repartitioning.
    #[builder(default = "DEFAULT_OUTPUT_PARALLELISM")]
    pub output_parallelism: usize,
}

/// Common configuration for compaction
/// Contains parameters that are shared across different compaction phases
#[derive(Builder, Debug, Clone, Default)]
pub struct CompactionBaseConfig {
    /// Target size in bytes for each compacted file (default: 1GB)
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size: u64,
}

/// Configuration for compaction planning phase
/// Contains parameters that affect file selection and parallelism calculation
#[derive(Builder, Debug, Clone)]
pub struct CompactionPlanningConfig {
    /// Base configuration shared across all compaction phases
    #[builder(default)]
    pub base: CompactionBaseConfig,

    /// Threshold for small file compaction (default: 32MB)
    #[builder(default = "DEFAULT_SMALL_FILE_THRESHOLD")]
    pub small_file_threshold: u64,

    /// Maximum total size for a single compaction task (default: 50GB)
    #[builder(default = "DEFAULT_MAX_TASK_TOTAL_SIZE")]
    pub max_task_total_size: u64,

    /// Expected minimum size in bytes for each partition when partitioning by size
    #[builder(default = "DEFAULT_MIN_SIZE_PER_PARTITION")]
    pub min_size_per_partition: u64,

    /// Expected maximum file count per partition when partitioning by file count
    #[builder(default = "DEFAULT_MAX_FILE_COUNT_PER_PARTITION")]
    pub max_file_count_per_partition: usize,

    /// Maximum parallelism for the compaction process
    #[builder(default = "available_parallelism().get()")]
    pub max_parallelism: usize,

    /// Whether to enable heuristic output parallelism (default: true)
    #[builder(default = "true")]
    pub enable_heuristic_output_parallelism: bool,

    /// Minimum number of files required to trigger compaction (default: 2)
    #[builder(default = "DEFAULT_MIN_FILE_COUNT")]
    pub min_file_count: usize,
}

impl Default for CompactionPlanningConfig {
    fn default() -> Self {
        CompactionPlanningConfigBuilder::default().build().unwrap()
    }
}

/// Configuration for compaction execution phase
/// Contains parameters that affect the actual compaction execution
#[derive(Builder, Debug, Clone)]
pub struct CompactionExecutionConfig {
    /// Base configuration shared across all compaction phases
    #[builder(default)]
    pub base: CompactionBaseConfig,

    #[builder(default = "DEFAULT_PREFIX.to_owned()")]
    pub data_file_prefix: String,

    /// Whether to enable validation after compaction completes
    #[builder(default = "DEFAULT_VALIDATE_COMPACTION")]
    pub enable_validate_compaction: bool,

    /// Maximum number of rows in each record batch
    #[builder(default = "DEFAULT_MAX_RECORD_BATCH_ROWS")]
    pub max_record_batch_rows: usize,

    /// Maximum number of concurrent file close operations
    #[builder(default = "DEFAULT_MAX_CONCURRENT_CLOSES")]
    pub max_concurrent_closes: usize,

    /// Parquet writer properties for output files
    #[builder(default = "default_writer_properties()")]
    pub write_parquet_properties: WriterProperties,

    /// The executor engine will normalize un-quoted column identifiers to lowercase (default: true)
    #[builder(default = "DEFAULT_NORMALIZED_COLUMN_IDENTIFIERS")]
    pub enable_normalized_column_identifiers: bool,

    /// Whether to enable dynamic file size estimation for better rolling prediction (default: false)
    #[builder(default = "DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION")]
    pub enable_dynamic_size_estimation: bool,

    /// Smoothing factor for dynamic size estimation updates (default: 0.3)
    #[builder(default = "DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR")]
    pub size_estimation_smoothing_factor: f64,
}

impl Default for CompactionExecutionConfig {
    fn default() -> Self {
        CompactionExecutionConfigBuilder::default().build().unwrap()
    }
}

/// Main configuration that combines planning and execution configs
/// This provides clear separation between planning and execution phases
#[derive(Builder, Debug, Clone)]
#[builder(pattern = "owned")]
pub struct CompactionConfig {
    #[builder(default)]
    pub planning: CompactionPlanningConfig,
    #[builder(default)]
    pub execution: CompactionExecutionConfig,
}

impl CompactionConfig {
    pub fn new(planning: CompactionPlanningConfig, execution: CompactionExecutionConfig) -> Self {
        Self {
            planning,
            execution,
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfigBuilder::default().build().unwrap()
    }
}
