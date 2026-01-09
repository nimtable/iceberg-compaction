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

use crate::SnapshotStats;
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

// Auto compaction defaults
pub const DEFAULT_MIN_SMALL_FILES_COUNT: usize = 5;
pub const DEFAULT_MIN_FILES_WITH_DELETES_COUNT: usize = 1;

// Strategy configuration defaults
pub const DEFAULT_TARGET_GROUP_SIZE: u64 = 100 * 1024 * 1024 * 1024; // 100GB - BinPack target size

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

/// Thresholds for automatic strategy selection.
#[derive(Debug, Clone)]
pub struct AutoThresholds {
    /// Minimum small file count to trigger `SmallFiles` strategy.
    pub min_small_files_count: usize,
    /// Minimum delete file count to trigger `FilesWithDeletes` strategy.
    pub min_files_with_deletes_count: usize,
    /// Minimum impact ratio (fraction of total files). None = disabled.
    pub min_impact_ratio: Option<f64>,
}

impl Default for AutoThresholds {
    fn default() -> Self {
        Self {
            min_small_files_count: DEFAULT_MIN_SMALL_FILES_COUNT,
            min_files_with_deletes_count: DEFAULT_MIN_FILES_WITH_DELETES_COUNT,
            min_impact_ratio: None,
        }
    }
}

// TODO: Consider supporting custom strategy order in the future.

/// Automatic strategy selection based on snapshot statistics.
///
/// Priority: `FilesWithDeletes` → `SmallFiles` → `Full` (if enabled).
#[derive(Builder, Debug, Clone)]
#[builder(setter(into, strip_option))]
pub struct AutoCompactionConfig {
    /// Strategy selection thresholds.
    #[builder(default)]
    pub thresholds: AutoThresholds,

    /// Fallback to Full when no specialized strategy matches.
    #[builder(default = "true")]
    pub enable_full_fallback: bool,

    /// Common planning parameters applied to all selected strategies
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

    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,

    #[builder(default, setter(strip_option))]
    pub group_filters: Option<GroupFilters>,

    #[builder(default = "DEFAULT_MIN_DELETE_FILE_COUNT_THRESHOLD")]
    pub min_delete_file_count_threshold: usize,

    #[builder(default)]
    pub execution: CompactionExecutionConfig,
}

impl AutoCompactionConfig {
    /// Selects strategy based on snapshot statistics.
    pub fn resolve(&self, stats: &SnapshotStats) -> Option<CompactionPlanningConfig> {
        if stats.total_data_files <= 1 {
            return None;
        }

        let total = stats.total_data_files as f64;

        if stats.files_with_deletes_count >= self.thresholds.min_files_with_deletes_count
            && self
                .thresholds
                .min_impact_ratio
                .is_none_or(|min| stats.files_with_deletes_count as f64 / total >= min)
        {
            return Some(CompactionPlanningConfig::FilesWithDeletes(
                FilesWithDeletesConfig {
                    target_file_size_bytes: self.target_file_size_bytes,
                    min_size_per_partition: self.min_size_per_partition,
                    max_file_count_per_partition: self.max_file_count_per_partition,
                    max_parallelism: self.max_parallelism,
                    enable_heuristic_output_parallelism: self.enable_heuristic_output_parallelism,
                    grouping_strategy: self.grouping_strategy.clone(),
                    min_delete_file_count_threshold: self.min_delete_file_count_threshold,
                    group_filters: self.group_filters.clone(),
                },
            ));
        }

        if stats.small_files_count >= self.thresholds.min_small_files_count
            && self
                .thresholds
                .min_impact_ratio
                .is_none_or(|min| stats.small_files_count as f64 / total >= min)
        {
            return Some(CompactionPlanningConfig::SmallFiles(SmallFilesConfig {
                target_file_size_bytes: self.target_file_size_bytes,
                min_size_per_partition: self.min_size_per_partition,
                max_file_count_per_partition: self.max_file_count_per_partition,
                max_parallelism: self.max_parallelism,
                enable_heuristic_output_parallelism: self.enable_heuristic_output_parallelism,
                small_file_threshold_bytes: self.small_file_threshold_bytes,
                grouping_strategy: self.grouping_strategy.clone(),
                group_filters: self.group_filters.clone(),
            }));
        }

        if self.enable_full_fallback {
            return Some(CompactionPlanningConfig::Full(FullCompactionConfig {
                target_file_size_bytes: self.target_file_size_bytes,
                min_size_per_partition: self.min_size_per_partition,
                max_file_count_per_partition: self.max_file_count_per_partition,
                max_parallelism: self.max_parallelism,
                enable_heuristic_output_parallelism: self.enable_heuristic_output_parallelism,
                grouping_strategy: self.grouping_strategy.clone(),
            }));
        }

        None
    }
}

impl Default for AutoCompactionConfig {
    fn default() -> Self {
        AutoCompactionConfigBuilder::default()
            .build()
            .expect("AutoCompactionConfig default should always build")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_selection::analyzer::SnapshotStats;

    fn create_test_stats(
        total_data_files: usize,
        small_files: usize,
        files_with_deletes: usize,
    ) -> SnapshotStats {
        SnapshotStats {
            total_data_files,
            small_files_count: small_files,
            files_with_deletes_count: files_with_deletes,
        }
    }

    #[test]
    fn test_resolve_strategy_priority() {
        let config = AutoCompactionConfigBuilder::default()
            .thresholds(AutoThresholds {
                min_files_with_deletes_count: 3,
                min_small_files_count: 5,
                min_impact_ratio: None,
            })
            .build()
            .unwrap();

        // Priority 1: FilesWithDeletes wins when both thresholds met
        let stats = create_test_stats(10, 6, 4);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::FilesWithDeletes(_)
        ));

        // Priority 2: SmallFiles when only it meets threshold
        let stats = create_test_stats(10, 6, 2);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::SmallFiles(_)
        ));

        // Priority 3: Full when no threshold met but fallback enabled
        let stats = create_test_stats(10, 2, 1);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::Full(_)
        ));
    }

    #[test]
    fn test_resolve_returns_none() {
        let config = AutoCompactionConfigBuilder::default()
            .enable_full_fallback(false)
            .build()
            .unwrap();

        // Empty table
        assert!(config.resolve(&create_test_stats(0, 0, 0)).is_none());

        // Single file
        assert!(config.resolve(&create_test_stats(1, 0, 0)).is_none());

        // Multiple files but no threshold met and fallback disabled
        assert!(config.resolve(&create_test_stats(5, 2, 0)).is_none());
    }

    #[test]
    fn test_resolve_fallback_behavior() {
        // Use stats that don't meet default thresholds
        let stats = create_test_stats(10, 2, 0);

        // Fallback enabled -> Full
        let config = AutoCompactionConfigBuilder::default()
            .enable_full_fallback(true)
            .build()
            .unwrap();
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::Full(_)
        ));

        // Fallback disabled -> None
        let config = AutoCompactionConfigBuilder::default()
            .enable_full_fallback(false)
            .build()
            .unwrap();
        assert!(config.resolve(&stats).is_none());
    }

    #[test]
    fn test_resolve_impact_ratio() {
        let config = AutoCompactionConfigBuilder::default()
            .thresholds(AutoThresholds {
                min_files_with_deletes_count: 5,
                min_small_files_count: 5,
                min_impact_ratio: Some(0.10),
            })
            .enable_full_fallback(true)
            .build()
            .unwrap();

        // Low impact (0.5%) -> fallback to Full
        let stats = create_test_stats(10000, 100, 50);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::Full(_)
        ));

        // High impact (80%) -> use specialized strategy
        let stats = create_test_stats(1000, 100, 800);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::FilesWithDeletes(_)
        ));

        // At boundary (10%) -> use specialized strategy
        let stats = create_test_stats(100, 5, 10);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::FilesWithDeletes(_)
        ));

        // Below boundary (9%) -> fallback to Full
        let stats = create_test_stats(100, 5, 9);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::Full(_)
        ));
    }

    #[test]
    fn test_resolve_threshold_boundaries() {
        let config = AutoCompactionConfigBuilder::default()
            .thresholds(AutoThresholds {
                min_files_with_deletes_count: 3,
                min_small_files_count: 5,
                min_impact_ratio: None,
            })
            .build()
            .unwrap();

        // At delete threshold (exactly 3)
        let stats = create_test_stats(10, 0, 3);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::FilesWithDeletes(_)
        ));

        // At small files threshold (exactly 5)
        let stats = create_test_stats(10, 5, 2);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::SmallFiles(_)
        ));

        // Below both thresholds
        let stats = create_test_stats(10, 2, 1);
        assert!(matches!(
            config.resolve(&stats).unwrap(),
            CompactionPlanningConfig::Full(_)
        ));
    }

    #[test]
    fn test_resolve_propagates_config() {
        let config = AutoCompactionConfigBuilder::default()
            .target_file_size_bytes(1_000_000_u64)
            .max_parallelism(8_usize)
            .thresholds(AutoThresholds {
                min_files_with_deletes_count: 2,
                min_small_files_count: 10,
                min_impact_ratio: None,
            })
            .build()
            .unwrap();

        let stats = create_test_stats(10, 1, 3);
        let CompactionPlanningConfig::FilesWithDeletes(cfg) = config.resolve(&stats).unwrap()
        else {
            panic!("Expected FilesWithDeletes");
        };

        assert_eq!(cfg.target_file_size_bytes, 1_000_000);
        assert_eq!(cfg.max_parallelism, 8);
    }
}
