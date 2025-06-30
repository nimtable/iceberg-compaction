/*
 * Copyright 2025 iceberg-compact
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
pub const DEFAULT_BATCH_PARALLELISM: usize = 4;
pub const DEFAULT_TARGET_PARTITIONS: usize = 4;
pub const DEFAULT_TARGET_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
pub const DEFAULT_VALIDATE_COMPACTION: bool = false;
pub const DEFAULT_MAX_RECORD_BATCH_ROWS: usize = 1024;
pub const DEFAULT_MAX_CONCURRENT_CLOSES: usize = 4;

// Helper function for the default WriterProperties
fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(concat!("iceberg-compact version ", env!("CARGO_PKG_VERSION")).to_owned())
        .build()
}

#[derive(Builder, Debug, Default, Clone)]
pub struct CompactionConfig {
    /// Number of batches to process in parallel during compaction
    #[builder(default = "DEFAULT_BATCH_PARALLELISM")]
    pub batch_parallelism: usize,
    /// Target number of partitions for the compacted data
    #[builder(default = "DEFAULT_TARGET_PARTITIONS")]
    pub target_partitions: usize,
    /// Prefix for generated data file names
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
    /// Parquet writer properties for output files
    #[builder(default = "default_writer_properties()")]
    pub write_parquet_properties: WriterProperties,
}
