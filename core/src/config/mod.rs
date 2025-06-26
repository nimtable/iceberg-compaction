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
use serde::Deserialize;

const DEFAULT_PREFIX: &str = "iceberg-compaction";
const DEFAULT_EXECUTOR_PARALLELISM: usize = 4;
const DEFAULT_OUTPUT_PARALLELISM: usize = 4;
const DEFAULT_TARGET_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
const DEFAULT_VALIDATE_COMPACTION: bool = false;
const DEFAULT_MAX_RECORD_BATCH_ROWS: usize = 1024;
const DEFAULT_FILE_SCAN_CONCURRENCY: usize = 8;
// Conservative limit to avoid S3 connection timeout issues
const MAX_RECOMMENDED_FILE_SCAN_CONCURRENCY: usize = 32;

// Helper function for the default WriterProperties
fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(
            concat!("iceberg-compaction version ", env!("CARGO_PKG_VERSION")).to_owned(),
        )
        .build()
}

#[derive(Builder, Debug, Deserialize, Default, Clone)]
pub struct CompactionConfig {
    /// The number of parallel tasks to execute. used for scan and join.
    #[builder(default = "DEFAULT_EXECUTOR_PARALLELISM")]
    pub executor_parallelism: usize,
    /// The number of parallel tasks to output. Only used for repartitioning.
    #[builder(default = "DEFAULT_OUTPUT_PARALLELISM")]
    pub output_parallelism: usize,
    #[builder(default = "DEFAULT_PREFIX.to_owned()")]
    pub data_file_prefix: String,
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size: u64,
    #[builder(default = "DEFAULT_VALIDATE_COMPACTION")]
    pub enable_validate_compaction: bool,
    #[builder(default = "DEFAULT_MAX_RECORD_BATCH_ROWS")]
    pub max_record_batch_rows: usize,
    #[builder(default = "DEFAULT_FILE_SCAN_CONCURRENCY")]
    pub file_scan_concurrency: usize,

    #[serde(skip)]
    // FIXME: this is a workaround for serde not supporting default values for WriterProperties
    #[builder(default = "default_writer_properties()")]
    pub write_parquet_properties: WriterProperties,
}

impl CompactionConfig {
    /// Validates the configuration and provides warnings for potential issues
    pub fn validate(&self) -> Vec<String> {
        let mut warnings = Vec::new();

        // Check for S3 connection timeout risks
        if self.file_scan_concurrency > MAX_RECOMMENDED_FILE_SCAN_CONCURRENCY {
            warnings.push(format!(
                "file_scan_concurrency ({}) is higher than recommended maximum ({}). \
                 This may cause S3 connection timeout issues. Consider reducing it.",
                self.file_scan_concurrency, MAX_RECOMMENDED_FILE_SCAN_CONCURRENCY
            ));
        }

        // Check for reasonable executor_parallelism vs file_scan_concurrency ratio
        let files_per_partition = self.file_scan_concurrency / self.executor_parallelism.max(1);
        if files_per_partition > 8 {
            warnings.push(format!(
                "Current configuration may create {} concurrent files per partition. \
                 Consider increasing executor_parallelism or reducing file_scan_concurrency \
                 to prevent S3 connection timeout issues.",
                files_per_partition
            ));
        }

        warnings
    }
}
