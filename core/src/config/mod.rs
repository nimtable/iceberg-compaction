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
pub const DEFAULT_MAX_CONCURRENT_COMPACTION_PLANS: usize = 4; // default max concurrent compaction plans

// Strategy configuration defaults
pub const DEFAULT_TARGET_GROUP_SIZE: u64 = 100 * 1024 * 1024 * 1024; // 100GB - target size for BinPack algorithm
pub const DEFAULT_MIN_GROUP_SIZE: u64 = 512 * 1024 * 1024; // 512MB - minimum group size filter
pub const DEFAULT_MIN_GROUP_FILE_COUNT: usize = 2; // Minimum files per group filter

/// Configuration for bin-packing grouping strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinPackConfig {
    /// Target size for each group in bytes.
    pub target_group_size: u64,

    /// Minimum group size in bytes. Groups smaller than this will be filtered out.
    pub min_group_size: Option<u64>,

    /// Minimum file count per group. Groups with fewer files will be filtered out.
    pub min_group_file_count: Option<usize>,
}

impl BinPackConfig {
    /// Creates a new bin-pack configuration with only target size.
    pub fn new(target_group_size: u64) -> Self {
        Self {
            target_group_size,
            min_group_size: None,
            min_group_file_count: None,
        }
    }

    /// Creates a new bin-pack configuration with filters.
    pub fn with_filters(
        target_group_size: u64,
        min_group_size: Option<u64>,
        min_group_file_count: Option<usize>,
    ) -> Self {
        Self {
            target_group_size,
            min_group_size,
            min_group_file_count,
        }
    }
}

impl Default for BinPackConfig {
    fn default() -> Self {
        Self::new(DEFAULT_TARGET_GROUP_SIZE)
    }
}

/// File grouping strategy for compaction.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum GroupingStrategy {
    /// All files are merged into a single compaction task.
    #[default]
    Noop,
    /// Files are grouped by size using a bin-packing algorithm.
    BinPack(BinPackConfig),
}

// ============================================
// Compaction Planning Configuration
// ============================================

/// Configuration for small files compaction.
///
/// Compacts data files smaller than `small_file_threshold`.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct SmallFilesConfig {
    /// Target output file size in bytes.
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size: u64,

    /// Minimum partition size for parallelism calculation.
    #[builder(default = "DEFAULT_MIN_SIZE_PER_PARTITION")]
    pub min_size_per_partition: u64,

    /// Maximum file count per partition for parallelism calculation.
    #[builder(default = "DEFAULT_MAX_FILE_COUNT_PER_PARTITION")]
    pub max_file_count_per_partition: usize,

    /// Maximum parallelism.
    #[builder(default = "available_parallelism().get()")]
    pub max_parallelism: usize,

    /// Whether to enable heuristic output parallelism.
    #[builder(default = "true")]
    pub enable_heuristic_output_parallelism: bool,

    /// Small file threshold in bytes. Only files smaller than this will be compacted.
    #[builder(default = "DEFAULT_SMALL_FILE_THRESHOLD")]
    pub small_file_threshold: u64,

    /// Minimum number of files required to trigger compaction. 0 means no limit.
    #[builder(default = "DEFAULT_MIN_FILE_COUNT")]
    pub min_file_count: usize,

    /// File grouping strategy.
    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,
}

impl Default for SmallFilesConfig {
    fn default() -> Self {
        SmallFilesConfigBuilder::default().build().unwrap()
    }
}

/// Configuration for full compaction.
///
/// Selects all data files (no filtering).
/// Use `grouping_strategy` to control task splitting.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct FullCompactionConfig {
    /// Target output file size in bytes.
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size: u64,

    /// Minimum partition size for parallelism calculation.
    #[builder(default = "DEFAULT_MIN_SIZE_PER_PARTITION")]
    pub min_size_per_partition: u64,

    /// Maximum file count per partition for parallelism calculation.
    #[builder(default = "DEFAULT_MAX_FILE_COUNT_PER_PARTITION")]
    pub max_file_count_per_partition: usize,

    /// Maximum parallelism.
    #[builder(default = "available_parallelism().get()")]
    pub max_parallelism: usize,

    /// Whether to enable heuristic output parallelism.
    #[builder(default = "true")]
    pub enable_heuristic_output_parallelism: bool,

    /// File grouping strategy.
    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,
}

impl Default for FullCompactionConfig {
    fn default() -> Self {
        FullCompactionConfigBuilder::default().build().unwrap()
    }
}

// Helper function for the default WriterProperties
fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(
            concat!("iceberg-compaction version ", env!("CARGO_PKG_VERSION")).to_owned(),
        )
        .build()
}

/// Common configuration shared across compaction phases.
///
/// Used by `CompactionExecutionConfig` for target file size.
#[derive(Builder, Debug, Clone)]
pub struct CompactionBaseConfig {
    /// Target file size in bytes.
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size: u64,
}

impl Default for CompactionBaseConfig {
    fn default() -> Self {
        Self {
            target_file_size: DEFAULT_TARGET_FILE_SIZE,
        }
    }
}

/// Compaction planning configuration.
///
/// Represents different compaction types with their specific configurations.
#[derive(Debug, Clone)]
pub enum CompactionPlanningConfig {
    /// Merge small data files.
    MergeSmallDataFiles(SmallFilesConfig),
    /// Full compaction.
    Full(FullCompactionConfig),
}

impl CompactionPlanningConfig {
    /// Creates a small files compaction configuration.
    pub fn from_small_files(config: SmallFilesConfig) -> Self {
        Self::MergeSmallDataFiles(config)
    }

    /// Creates a full compaction configuration.
    pub fn from_full_compaction(config: FullCompactionConfig) -> Self {
        Self::Full(config)
    }

    /// Returns `true` if this is small files compaction.
    pub fn is_merge_small_data_files(&self) -> bool {
        matches!(self, Self::MergeSmallDataFiles(_))
    }

    /// Returns `true` if this is full compaction.
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full(_))
    }

    /// Returns the display name for the compaction type.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::MergeSmallDataFiles(_) => "MergeSmallDataFiles",
            Self::Full(_) => "Full",
        }
    }

    /// Returns the target file size.
    pub fn target_file_size(&self) -> u64 {
        match self {
            Self::MergeSmallDataFiles(c) => c.target_file_size,
            Self::Full(c) => c.target_file_size,
        }
    }

    /// Returns the minimum size per partition.
    pub fn min_size_per_partition(&self) -> u64 {
        match self {
            Self::MergeSmallDataFiles(c) => c.min_size_per_partition,
            Self::Full(c) => c.min_size_per_partition,
        }
    }

    /// Returns the maximum file count per partition.
    pub fn max_file_count_per_partition(&self) -> usize {
        match self {
            Self::MergeSmallDataFiles(c) => c.max_file_count_per_partition,
            Self::Full(c) => c.max_file_count_per_partition,
        }
    }

    /// Returns the maximum parallelism.
    pub fn max_parallelism(&self) -> usize {
        match self {
            Self::MergeSmallDataFiles(c) => c.max_parallelism,
            Self::Full(c) => c.max_parallelism,
        }
    }

    /// Returns whether heuristic output parallelism is enabled.
    pub fn enable_heuristic_output_parallelism(&self) -> bool {
        match self {
            Self::MergeSmallDataFiles(c) => c.enable_heuristic_output_parallelism,
            Self::Full(c) => c.enable_heuristic_output_parallelism,
        }
    }
}

impl Default for CompactionPlanningConfig {
    fn default() -> Self {
        Self::from_small_files(SmallFilesConfig::default())
    }
}

/// Compaction execution configuration.
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

    /// Normalize un-quoted column identifiers to lowercase
    #[builder(default = "DEFAULT_NORMALIZED_COLUMN_IDENTIFIERS")]
    pub enable_normalized_column_identifiers: bool,

    /// Whether to enable dynamic file size estimation
    #[builder(default = "DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION")]
    pub enable_dynamic_size_estimation: bool,

    /// Smoothing factor for dynamic size estimation updates
    #[builder(default = "DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR")]
    pub size_estimation_smoothing_factor: f64,

    /// Maximum number of compaction plans to execute concurrently
    ///
    /// **Note**: This parameter only applies when using the `compact()` method (all-in-one workflow).
    /// For plan-driven workflows (`plan_compaction()` → `rewrite_plan()` → `commit_rewrite_results()`),
    /// users manage concurrency themselves.
    ///
    /// This controls how many compaction plans (`FileGroups`) can execute simultaneously.
    /// Combined with `max_parallelism` (in `CompactionPlanningConfig`), this determines the
    /// theoretical maximum total system parallelism:
    /// ```text
    /// max_parallelism × max_concurrent_compaction_plans
    /// ```
    ///
    /// For example, with `max_parallelism = 16` and `max_concurrent_compaction_plans = 4`,
    /// the theoretical maximum is 64 concurrent tasks across all plans.
    ///
    /// Note: Actual parallelism is typically lower because individual plans calculate
    /// their own parallelism based on data characteristics.
    #[builder(default = "DEFAULT_MAX_CONCURRENT_COMPACTION_PLANS")]
    pub max_concurrent_compaction_plans: usize,
}

impl Default for CompactionExecutionConfig {
    fn default() -> Self {
        CompactionExecutionConfigBuilder::default().build().unwrap()
    }
}

/// Main configuration that combines planning and execution configs
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
