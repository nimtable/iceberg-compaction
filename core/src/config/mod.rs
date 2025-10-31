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

//! Compaction configuration types and constants.

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
pub const DEFAULT_MIN_SIZE_PER_PARTITION: u64 = 512 * 1024 * 1024; // 512 MB per partition
pub const DEFAULT_MAX_FILE_COUNT_PER_PARTITION: usize = 32; // 32 files per partition
pub const DEFAULT_MIN_FILE_COUNT: usize = 0; // default unlimited
pub const DEFAULT_MAX_CONCURRENT_COMPACTION_PLANS: usize = 4; // default max concurrent compaction plans
pub const DEFAULT_MIN_DELETE_FILE_COUNT_THRESHOLD: usize = 128; // default minimum delete file count for compaction

// Strategy configuration defaults
pub const DEFAULT_TARGET_GROUP_SIZE: u64 = 100 * 1024 * 1024 * 1024; // 100GB - BinPack target size
pub const DEFAULT_MIN_GROUP_SIZE: u64 = 512 * 1024 * 1024; // 512MB - minimum group size filter
pub const DEFAULT_MIN_GROUP_FILE_COUNT: usize = 2; // Minimum files per group filter

/// Configuration for `BinPack` file grouping strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinPackConfig {
    pub target_group_size_bytes: u64,
    pub min_group_size_bytes: Option<u64>,
    pub min_group_file_count: Option<usize>,
}

impl BinPackConfig {
    /// Creates a new `BinPack` config with the given target group size.
    pub fn new(target_group_size_bytes: u64) -> Self {
        Self {
            target_group_size_bytes,
            min_group_size_bytes: None,
            min_group_file_count: None,
        }
    }

    /// Creates a new `BinPack` config with target size and optional filters.
    pub fn with_filters(
        target_group_size_bytes: u64,
        min_group_size_bytes: Option<u64>,
        min_group_file_count: Option<usize>,
    ) -> Self {
        Self {
            target_group_size_bytes,
            min_group_size_bytes,
            min_group_file_count,
        }
    }
}

impl Default for BinPackConfig {
    fn default() -> Self {
        Self::new(DEFAULT_TARGET_GROUP_SIZE)
    }
}

/// File grouping strategy: Noop (no grouping) or `BinPack` (size-based grouping).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum GroupingStrategy {
    #[default]
    Noop,
    BinPack(BinPackConfig),
}

/// Configuration for small files compaction strategy.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct SmallFilesConfig {
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size_bytes: u64,

    #[builder(default = "DEFAULT_MIN_SIZE_PER_PARTITION")]
    pub min_size_per_partition: u64,

    #[builder(default = "DEFAULT_MAX_FILE_COUNT_PER_PARTITION")]
    pub max_file_count_per_partition: usize,

    #[builder(default = "available_parallelism().get()")]
    pub max_parallelism: usize,

    #[builder(default = "true")]
    pub enable_heuristic_output_parallelism: bool,

    #[builder(default = "DEFAULT_SMALL_FILE_THRESHOLD")]
    pub small_file_threshold_bytes: u64,

    #[builder(default = "DEFAULT_MIN_FILE_COUNT")]
    pub min_file_count: usize,

    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,
}

impl Default for SmallFilesConfig {
    fn default() -> Self {
        SmallFilesConfigBuilder::default().build().unwrap()
    }
}

/// Configuration for full compaction strategy.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct FullCompactionConfig {
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size_bytes: u64,

    #[builder(default = "DEFAULT_MIN_SIZE_PER_PARTITION")]
    pub min_size_per_partition: u64,

    #[builder(default = "DEFAULT_MAX_FILE_COUNT_PER_PARTITION")]
    pub max_file_count_per_partition: usize,

    #[builder(default = "available_parallelism().get()")]
    pub max_parallelism: usize,

    #[builder(default = "true")]
    pub enable_heuristic_output_parallelism: bool,

    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,
}

impl Default for FullCompactionConfig {
    fn default() -> Self {
        FullCompactionConfigBuilder::default().build().unwrap()
    }
}

/// Configuration for files-with-deletes compaction strategy.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct FilesWithDeletesConfig {
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size_bytes: u64,

    #[builder(default = "DEFAULT_MIN_SIZE_PER_PARTITION")]
    pub min_size_per_partition: u64,

    #[builder(default = "DEFAULT_MAX_FILE_COUNT_PER_PARTITION")]
    pub max_file_count_per_partition: usize,

    #[builder(default = "available_parallelism().get()")]
    pub max_parallelism: usize,

    #[builder(default = "true")]
    pub enable_heuristic_output_parallelism: bool,

    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,

    #[builder(default = "DEFAULT_MIN_DELETE_FILE_COUNT_THRESHOLD")]
    pub min_delete_file_count_threshold: usize,
}

impl Default for FilesWithDeletesConfig {
    fn default() -> Self {
        FilesWithDeletesConfigBuilder::default().build().unwrap()
    }
}

/// Helper for default `WriterProperties` (SNAPPY compression).
fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(
            concat!("iceberg-compaction version ", env!("CARGO_PKG_VERSION")).to_owned(),
        )
        .build()
}

/// Planning configuration variants for different compaction strategies.
#[derive(Debug, Clone)]
pub enum CompactionPlanningConfig {
    SmallFiles(SmallFilesConfig),
    Full(FullCompactionConfig),
    FilesWithDeletes(FilesWithDeletesConfig),
}

impl CompactionPlanningConfig {
    /// Returns target file size in bytes for the strategy.
    pub fn target_file_size_bytes(&self) -> u64 {
        match self {
            Self::SmallFiles(c) => c.target_file_size_bytes,
            Self::Full(c) => c.target_file_size_bytes,
            Self::FilesWithDeletes(c) => c.target_file_size_bytes,
        }
    }

    /// Returns minimum size per partition for the strategy.
    pub fn min_size_per_partition(&self) -> u64 {
        match self {
            Self::SmallFiles(c) => c.min_size_per_partition,
            Self::Full(c) => c.min_size_per_partition,
            Self::FilesWithDeletes(c) => c.min_size_per_partition,
        }
    }

    /// Returns maximum file count per partition for the strategy.
    pub fn max_file_count_per_partition(&self) -> usize {
        match self {
            Self::SmallFiles(c) => c.max_file_count_per_partition,
            Self::Full(c) => c.max_file_count_per_partition,
            Self::FilesWithDeletes(c) => c.max_file_count_per_partition,
        }
    }

    /// Returns maximum parallelism for the strategy.
    pub fn max_parallelism(&self) -> usize {
        match self {
            Self::SmallFiles(c) => c.max_parallelism,
            Self::Full(c) => c.max_parallelism,
            Self::FilesWithDeletes(c) => c.max_parallelism,
        }
    }

    /// Returns whether heuristic output parallelism is enabled.
    pub fn enable_heuristic_output_parallelism(&self) -> bool {
        match self {
            Self::SmallFiles(c) => c.enable_heuristic_output_parallelism,
            Self::Full(c) => c.enable_heuristic_output_parallelism,
            Self::FilesWithDeletes(c) => c.enable_heuristic_output_parallelism,
        }
    }
}

impl Default for CompactionPlanningConfig {
    fn default() -> Self {
        Self::Full(FullCompactionConfig::default())
    }
}

/// Execution configuration for compaction operations.
#[derive(Builder, Debug, Clone)]
pub struct CompactionExecutionConfig {
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size_bytes: u64,

    #[builder(default = "DEFAULT_PREFIX.to_owned()")]
    pub data_file_prefix: String,

    #[builder(default = "DEFAULT_VALIDATE_COMPACTION")]
    pub enable_validate_compaction: bool,

    #[builder(default = "DEFAULT_MAX_RECORD_BATCH_ROWS")]
    pub max_record_batch_rows: usize,

    #[builder(default = "DEFAULT_MAX_CONCURRENT_CLOSES")]
    pub max_concurrent_closes: usize,

    #[builder(default = "default_writer_properties()")]
    pub write_parquet_properties: WriterProperties,

    #[builder(default = "DEFAULT_NORMALIZED_COLUMN_IDENTIFIERS")]
    pub enable_normalized_column_identifiers: bool,

    #[builder(default = "DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION")]
    pub enable_dynamic_size_estimation: bool,

    #[builder(default = "DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR")]
    pub size_estimation_smoothing_factor: f64,

    /// Maximum concurrent compaction plans in `compact()` method.
    ///
    /// **Note**: Only applies to managed workflow (`compact()`). Plan-driven workflow
    /// (`plan_compaction()` → `rewrite_plan()` → `commit_rewrite_results()`) manages
    /// concurrency externally.
    ///
    /// Theoretical max parallelism = `max_parallelism` × `max_concurrent_compaction_plans`.
    /// Actual parallelism is typically lower due to per-plan heuristics.
    #[builder(default = "DEFAULT_MAX_CONCURRENT_COMPACTION_PLANS")]
    pub max_concurrent_compaction_plans: usize,
}

impl Default for CompactionExecutionConfig {
    fn default() -> Self {
        CompactionExecutionConfigBuilder::default().build().unwrap()
    }
}

/// Combined planning and execution configuration for compaction.
#[derive(Builder, Debug, Clone)]
#[builder(pattern = "owned")]
pub struct CompactionConfig {
    #[builder(default)]
    pub planning: CompactionPlanningConfig,
    #[builder(default)]
    pub execution: CompactionExecutionConfig,
}

impl CompactionConfig {
    /// Creates a new config with planning and execution configurations.
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
