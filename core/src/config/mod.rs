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
const DEFAULT_BATCH_PARALLELISM: usize = 4;
const DEFAULT_TARGET_PARTITIONS: usize = 4;
const DEFAULT_TARGET_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
const DEFAULT_VALIDATE_COMPACTION: bool = false;
const DEFAULT_MAX_RECORD_BATCH_ROWS: usize = 1024;

// Helper function for the default WriterProperties
fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(concat!("iceberg-compaction version ", env!("CARGO_PKG_VERSION")).to_owned())
        .build()
}

#[derive(Builder, Debug, Deserialize, Default, Clone)]
pub struct CompactionConfig {
    #[builder(default = "DEFAULT_BATCH_PARALLELISM")]
    pub batch_parallelism: usize,
    #[builder(default = "DEFAULT_TARGET_PARTITIONS")]
    pub target_partitions: usize,
    #[builder(default = "DEFAULT_PREFIX.to_owned()")]
    pub data_file_prefix: String,
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size: u64,
    #[builder(default = "DEFAULT_VALIDATE_COMPACTION")]
    pub enable_validate_compaction: bool,
    #[builder(default = "DEFAULT_MAX_RECORD_BATCH_ROWS")]
    pub max_record_batch_rows: usize,

    #[serde(skip)]
    // FIXME: this is a workaround for serde not supporting default values for WriterProperties
    #[builder(default = "default_writer_properties()")]
    pub write_parquet_properties: WriterProperties,
}
