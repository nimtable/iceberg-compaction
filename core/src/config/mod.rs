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

use derive_builder::Builder;
use parquet::{basic::Compression, file::properties::WriterProperties};

pub const DEFAULT_PREFIX: &str = "iceberg-compact";
pub const DEFAULT_EXECUTOR_PARALLELISM: usize = 4;
pub const DEFAULT_OUTPUT_PARALLELISM: usize = 4;
pub const DEFAULT_TARGET_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
pub const DEFAULT_VALIDATE_COMPACTION: bool = false;
pub const DEFAULT_MAX_RECORD_BATCH_ROWS: usize = 1024;
pub const DEFAULT_MAX_CONCURRENT_CLOSES: usize = 4;
pub const DEFAULT_NORMALIZED_COLUMN_IDENTIFIERS: bool = true;
pub const DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION: bool = false;
pub const DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR: f64 = 0.3;
pub const DEFAULT_SMALL_FILE_THRESHOLD: u64 = 32 * 1024 * 1024; // 32 MB
pub const DEFAULT_MAX_TASK_TOTAL_SIZE: u64 = 50 * 1024 * 1024 * 1024; // 50 GB

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
pub struct CompactionConfig {
    /// The number of parallel tasks to execute. used for scan and join.
    #[builder(default = "DEFAULT_EXECUTOR_PARALLELISM")]
    pub executor_parallelism: usize,
    /// The number of parallel tasks to output. Only used for repartitioning.
    #[builder(default = "DEFAULT_OUTPUT_PARALLELISM")]
    pub output_parallelism: usize,
    #[builder(default = "DEFAULT_PREFIX.to_owned()")]
    pub data_file_prefix: String,
    /// Target size in bytes for each compacted file (default: 1GB)
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size: u64,
    /// Whether to enable validation after compaction completes
    #[builder(default = "DEFAULT_VALIDATE_COMPACTION")]
    pub enable_validate_compaction: bool,
    /// Maximum number of rows in each record batch
    #[builder(default = "DEFAULT_MAX_RECORD_BATCH_ROWS")]
    pub max_record_batch_rows: usize,
    /// Maximum number of concurrent file close operations
    #[builder(default = "DEFAULT_MAX_CONCURRENT_CLOSES")]
    pub max_concurrent_closes: usize,
    /// Threshold for small file compaction (default: 32MB)
    #[builder(default = "DEFAULT_SMALL_FILE_THRESHOLD")]
    pub small_file_threshold: u64,
    /// Maximum total size for a single compaction task (default: 10GB)
    #[builder(default = "DEFAULT_MAX_TASK_TOTAL_SIZE")]
    pub max_task_total_size: u64,
    /// Parquet writer properties for output files
    #[builder(default = "default_writer_properties()")]
    pub write_parquet_properties: WriterProperties,
    /// The executor engine will normalize un-quoted column identifiers to lowercase (default: true).
    ///
    /// Some Iceberg engines allow creating tables with case sensitive column names;
    /// to compact those tables, set this value to false.
    #[builder(default = "DEFAULT_NORMALIZED_COLUMN_IDENTIFIERS")]
    pub enable_normalized_column_identifiers: bool,
    /// Whether to enable dynamic file size estimation for better rolling prediction (default: false)
    /// This feature learns the compression ratio from actual write operations to improve size estimation.
    #[builder(default = "DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION")]
    pub enable_dynamic_size_estimation: bool,
    /// Smoothing factor for dynamic size estimation updates (default: 0.3)
    /// Lower values make the estimation more stable but slower to adapt to new data patterns.
    /// Higher values make the estimation more responsive but potentially more volatile.
    #[builder(default = "DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR")]
    pub size_estimation_smoothing_factor: f64,
}
