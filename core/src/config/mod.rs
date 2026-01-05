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

use derive_builder::Builder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::common::available_parallelism;

pub const DEFAULT_PREFIX: &str = "iceberg-compact";
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
pub const DEFAULT_MAX_CONCURRENT_COMPACTION_PLANS: usize = 4; // default max concurrent compaction plans
pub const DEFAULT_MIN_DELETE_FILE_COUNT_THRESHOLD: usize = 128; // default minimum delete file count for compaction

// Strategy configuration defaults
pub const DEFAULT_TARGET_GROUP_SIZE: u64 = 100 * 1024 * 1024 * 1024; // 100GB - BinPack target size

// Iceberg file size ratio constants (from SizeBasedFileRewritePlanner)
/// Default ratio for calculating minimum file size: minFileSize = targetFileSize * 0.75
pub const MIN_FILE_SIZE_DEFAULT_RATIO: f64 = 0.75;
/// Default ratio for calculating maximum file size: maxFileSize = targetFileSize * 1.80
pub const MAX_FILE_SIZE_DEFAULT_RATIO: f64 = 1.80;
/// Overhead added to split size for bin-packing (5MB, same as Iceberg)
pub const SPLIT_OVERHEAD: u64 = 5 * 1024 * 1024;

/// Configuration for bin-packing grouping strategy.
///
/// This struct wraps bin-packing parameters to allow future extensibility
/// without breaking API compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinPackConfig {
    /// Target size for each group (in bytes).
    ///
    /// The bin-packing algorithm will try to create groups close to this size.
    pub target_group_size_bytes: u64,
}

impl BinPackConfig {
    /// Creates a new bin-pack configuration with the given target group size.
    pub fn new(target_group_size_bytes: u64) -> Self {
        Self {
            target_group_size_bytes,
        }
    }
}

impl Default for BinPackConfig {
    fn default() -> Self {
        Self::new(DEFAULT_TARGET_GROUP_SIZE)
    }
}

/// File grouping strategy: how to partition files into groups.
///
/// This determines the grouping algorithm only. Group filtering is handled
/// separately by [`GroupFilters`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupingStrategy {
    /// Put all files into a single group.
    Single,
    /// Group files using bin-packing algorithm to target a specific group size.
    BinPack(BinPackConfig),
}

/// Group-level filters applied after grouping.
///
/// These filters remove groups that don't meet certain criteria. They are
/// orthogonal to the grouping strategy and can be used with any strategy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Builder)]
#[builder(setter(into, strip_option), default)]
pub struct GroupFilters {
    /// Minimum total size (in bytes) for a group to be included.
    pub min_group_size_bytes: Option<u64>,
    /// Minimum number of files for a group to be included.
    pub min_group_file_count: Option<usize>,
}

impl Default for GroupingStrategy {
    fn default() -> Self {
        Self::Single
    }
}

/// Configuration for small files compaction strategy.
///
/// This strategy targets small files for compaction. It supports both grouping
/// strategies and group-level filtering to control which groups get compacted.
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

    /// How to group files before compaction.
    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,

    /// Optional filters to apply after grouping.
    ///
    /// Groups that don't meet these criteria will be excluded from compaction.
    /// This allows fine-grained control over which file groups get compacted.
    #[builder(default, setter(strip_option))]
    pub group_filters: Option<GroupFilters>,
}

impl Default for SmallFilesConfig {
    fn default() -> Self {
        SmallFilesConfigBuilder::default()
            .build()
            .expect("SmallFilesConfig default should always build")
    }
}

/// Configuration for full compaction strategy.
///
/// This strategy performs full compaction of all files in a partition.
/// Group filters are NOT supported because "full" compaction means processing
/// ALL files without filtering.
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

    /// How to group files before compaction.
    ///
    /// Note: Group filters are not supported for full compaction.
    /// All groups will be compacted regardless of size or file count.
    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,
}

impl Default for FullCompactionConfig {
    fn default() -> Self {
        FullCompactionConfigBuilder::default()
            .build()
            .expect("FullCompactionConfig default should always build")
    }
}

/// Configuration for files-with-deletes compaction strategy.
///
/// This strategy targets data files that have associated delete files.
/// It supports group filtering to control which groups get compacted.
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

    /// How to group files before compaction.
    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,

    /// Minimum number of delete files required to trigger compaction.
    #[builder(default = "DEFAULT_MIN_DELETE_FILE_COUNT_THRESHOLD")]
    pub min_delete_file_count_threshold: usize,

    /// Optional filters to apply after grouping.
    ///
    /// Groups that don't meet these criteria will be excluded from compaction.
    #[builder(default, setter(strip_option))]
    pub group_filters: Option<GroupFilters>,
}

impl Default for FilesWithDeletesConfig {
    fn default() -> Self {
        FilesWithDeletesConfigBuilder::default()
            .build()
            .expect("FilesWithDeletesConfig default should always build")
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
        CompactionExecutionConfigBuilder::default()
            .build()
            .expect("CompactionExecutionConfig default should always build")
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
        CompactionConfigBuilder::default()
            .build()
            .expect("CompactionConfig default should always build")
    }
}
