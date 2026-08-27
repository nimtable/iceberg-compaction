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
use crate::{CompactionError, Result};

pub const DEFAULT_PREFIX: &str = "iceberg-compact";
pub const DEFAULT_TARGET_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
pub const DEFAULT_VALIDATE_COMPACTION: bool = false;
pub const DEFAULT_MAX_RECORD_BATCH_ROWS: usize = 1024;
pub const DEFAULT_MAX_CONCURRENT_CLOSES: usize = 4;
// Match Iceberg's default parquet row-group size:
// https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/TableProperties.java
pub const DEFAULT_MAX_ROW_GROUP_BYTES: usize = 128 * 1024 * 1024; // 128 MiB
pub const DEFAULT_NORMALIZED_COLUMN_IDENTIFIERS: bool = true;
pub const DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION: bool = false;
pub const DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR: f64 = 0.3;
pub const DEFAULT_SMALL_FILE_THRESHOLD: u64 = 32 * 1024 * 1024; // 32 MB
// Maximum files per executor input partition, not per Iceberg table partition.
pub const DEFAULT_MAX_FILE_COUNT_PER_PARTITION: usize = 32;
pub const DEFAULT_MAX_CONCURRENT_COMPACTION_PLANS: usize = 4; // default max concurrent compaction plans
pub const DEFAULT_MIN_DELETE_FILE_COUNT_THRESHOLD: usize = 128; // default minimum delete file count for compaction
pub const DEFAULT_ENABLE_PREFETCH: bool = false; // default setting for prefetching data files (set to false while its experimental)

// Strategy configuration defaults
pub const DEFAULT_TARGET_GROUP_SIZE: u64 = 100 * 1024 * 1024 * 1024; // 100GB - BinPack target size

/// Overhead added to split size for bin-packing
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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum GroupingStrategy {
    /// Put all files into a single group.
    #[default]
    Single,
    /// Group files using bin-packing algorithm to target a specific group size.
    BinPack(BinPackConfig),
}

/// File-group boundary used before applying the grouping strategy.
///
/// `Partition` keeps file groups within one Iceberg partition. `Table` allows
/// the grouping strategy to see all selected files together.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileGroupScope {
    /// Group files independently within each Iceberg partition.
    #[default]
    Partition,
    /// Group all selected files at table scope.
    Table,
}

/// Group-level filters applied after grouping by selective strategies.
#[derive(Debug, Clone, Default, PartialEq, Eq, Builder)]
#[builder(setter(into, strip_option), default)]
pub struct GroupFilters {
    /// Minimum total size (in bytes) for a group to be included.
    pub min_group_size_bytes: Option<u64>,
    /// Minimum number of files for a group to be included.
    pub min_group_file_count: Option<usize>,
}

/// Configuration for small files compaction strategy.
///
/// This strategy targets small files for compaction. Common planning pipeline
/// settings are configured through [`CompactionPlanningConfig`].
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct SmallFilesConfig {
    #[builder(default = "DEFAULT_SMALL_FILE_THRESHOLD")]
    pub small_file_threshold_bytes: u64,

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

/// Configuration for files-with-deletes compaction strategy.
///
/// This strategy targets data files that have associated delete files.
/// Common planning pipeline settings are configured through
/// [`CompactionPlanningConfig`].
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct FilesWithDeletesConfig {
    /// Minimum number of delete files required to trigger compaction.
    ///
    /// Zero matches every data file because every file has at least zero delete
    /// files. This differs from [`AutoCompactionConfig`], where zero disables
    /// one arm of the composite candidate predicate.
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

/// Planning configuration for automatic compaction.
///
/// Auto selects the union of small and delete-heavy data files, then sends the
/// unified candidate set through the common planning pipeline. A zero file
/// threshold disables that predicate for the planning run.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct AutoCompactionConfig {
    /// Files smaller than this value participate in the Auto candidate set.
    /// Zero disables the small-file predicate.
    #[builder(default = "DEFAULT_SMALL_FILE_THRESHOLD")]
    pub small_file_threshold_bytes: u64,

    /// Files with at least this many deletes participate in the Auto candidate
    /// set. Zero disables the delete-heavy predicate.
    #[builder(default = "DEFAULT_MIN_DELETE_FILE_COUNT_THRESHOLD")]
    pub min_delete_file_count_threshold: usize,

    #[builder(default, setter(strip_option))]
    pub group_filters: Option<GroupFilters>,
}

impl Default for AutoCompactionConfig {
    fn default() -> Self {
        AutoCompactionConfigBuilder::default()
            .build()
            .expect("AutoCompactionConfig default should always build")
    }
}

/// Helper for default `WriterProperties` (ZSTD compression).
fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_bytes(Some(DEFAULT_MAX_ROW_GROUP_BYTES))
        .set_created_by(
            concat!("iceberg-compaction version ", env!("CARGO_PKG_VERSION")).to_owned(),
        )
        .build()
}

/// Strategy-specific policy used by the common planning pipeline.
///
/// `Full` intentionally has no payload or group gating: it selects every file
/// visible to the planning scope.
#[derive(Debug, Clone, Default)]
pub enum CompactionStrategy {
    #[default]
    Full,
    SmallFiles(SmallFilesConfig),
    FilesWithDeletes(FilesWithDeletesConfig),
    Auto(AutoCompactionConfig),
}

/// Common planning scope and pipeline plus one candidate-selection strategy.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct CompactionPlanningConfig {
    #[builder(default)]
    pub strategy: CompactionStrategy,

    /// Target bytes used to recommend output parallelism for each planned file
    /// group. The writer's rolling threshold is configured independently by
    /// [`CompactionExecutionConfig::target_file_size_bytes`].
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE")]
    pub target_file_size_bytes: u64,

    /// Maximum files per executor input partition when recommending input
    /// parallelism. This does not refer to an Iceberg table partition.
    #[builder(default = "DEFAULT_MAX_FILE_COUNT_PER_PARTITION")]
    pub max_file_count_per_partition: usize,

    /// Maximum parallelism for input (reading) operations.
    /// Defaults to 4x available CPU parallelism.
    #[builder(default = "available_parallelism().get() * 4")]
    pub max_input_parallelism: usize,

    /// Maximum parallelism for output (writing) operations.
    /// Defaults to available CPU parallelism.
    #[builder(default = "available_parallelism().get()")]
    pub max_output_parallelism: usize,

    #[builder(default = "true")]
    pub enable_heuristic_output_parallelism: bool,

    /// How to group selected files before compaction.
    #[builder(default)]
    pub grouping_strategy: GroupingStrategy,

    /// Boundary for file groups before applying [`GroupingStrategy`].
    #[builder(default)]
    pub file_group_scope: FileGroupScope,
}

impl CompactionPlanningConfig {
    /// Creates a planning pipeline with default common settings.
    pub fn new(strategy: CompactionStrategy) -> Self {
        Self {
            strategy,
            ..Self::default()
        }
    }

    /// Validates common planning settings before file selection and parallelism
    /// calculation.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] when a divisor or parallelism limit
    /// is zero.
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            (
                "max_file_count_per_partition",
                self.max_file_count_per_partition,
            ),
            ("max_input_parallelism", self.max_input_parallelism),
            ("max_output_parallelism", self.max_output_parallelism),
        ] {
            if value == 0 {
                return Err(CompactionError::Config(format!(
                    "{name} must be greater than 0"
                )));
            }
        }

        Ok(())
    }
}

impl Default for CompactionPlanningConfig {
    fn default() -> Self {
        CompactionPlanningConfigBuilder::default()
            .build()
            .expect("CompactionPlanningConfig default should always build")
    }
}

/// Execution configuration for compaction operations.
#[derive(Builder, Debug, Clone)]
pub struct CompactionExecutionConfig {
    /// Rolling threshold passed to the data-file writer. Planning uses its own
    /// target to recommend output parallelism; the two values may intentionally
    /// differ when callers want fewer writer tasks that each roll multiple files.
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

    /// Deprecated: this setting is no longer used after switching to the upstream
    /// `RollingFileWriter`.
    ///
    /// It remains temporarily for backward compatibility and will be removed in a
    /// future change.
    #[deprecated(
        note = "unused after switching to the upstream RollingFileWriter; this field is now a no-op and will be removed in a future change"
    )]
    #[builder(default = "DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION")]
    #[builder_setter_attr(deprecated(
        note = "unused after switching to the upstream RollingFileWriter; this setter is now a no-op and will be removed in a future change"
    ))]
    pub enable_dynamic_size_estimation: bool,

    /// Deprecated: this setting is no longer used after switching to the upstream
    /// `RollingFileWriter`.
    ///
    /// It remains temporarily for backward compatibility and will be removed in a
    /// future change.
    #[deprecated(
        note = "unused after switching to the upstream RollingFileWriter; this field is now a no-op and will be removed in a future change"
    )]
    #[builder(default = "DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR")]
    #[builder_setter_attr(deprecated(
        note = "unused after switching to the upstream RollingFileWriter; this setter is now a no-op and will be removed in a future change"
    ))]
    pub size_estimation_smoothing_factor: f64,

    /// Maximum concurrent compaction plans in `compact()` method.
    ///
    /// **Note**: Only applies to managed workflow (`compact()`). Plan-driven workflow
    /// (`plan_compaction()` → `rewrite_plan()` → `commit_rewrite_results()`) manages
    /// concurrency externally.
    ///
    /// Theoretical max read parallelism = `max_input_parallelism` × `max_concurrent_compaction_plans`.
    /// Actual parallelism is typically lower due to per-plan heuristics.
    #[builder(default = "DEFAULT_MAX_CONCURRENT_COMPACTION_PLANS")]
    pub max_concurrent_compaction_plans: usize,

    /// Enable feature to prefetch entire data files before compacting them.
    ///
    /// This improves performance by reducing the number of total HTTP requests required
    /// to read data files. Presently, iceberg-rust will sent multiple sequential HTTP
    /// requests to download the byte ranges of each column from a Parquet file in an object
    /// store. That is sub-optimal if we know we need the entire file for compacting.
    /// Instead of making N HTTP requests for N column chunks, we can make 1 HTTP request
    /// for the entire file.
    ///
    /// It will download one file per concurrent file group being processed. For example,
    /// if 4 parallel executions are running, 4 downloaded files will be held in memory
    /// at once.
    ///
    /// **Note**: This is currently experimental and may not be stable.
    #[builder(default = "DEFAULT_ENABLE_PREFETCH")]
    pub enable_prefetch: bool,

    /// Optional upper bound (in bytes) on `DataFusion` execution memory.
    ///
    /// When set to `Some(n)` with `n > 0`, the `DataFusion` processor runs with a
    /// bounded [`datafusion::execution::memory_pool::FairSpillPool`] of `n`
    /// bytes plus an OS-backed `DiskManager`, so blocking operators — notably
    /// `SortExec`, used when compacting a *sorted* table — spill to disk once
    /// they exceed the budget instead of buffering all decoded Arrow data in
    /// memory. Sorted compaction decodes an entire file group to Arrow
    /// (ZSTD inflation ~5-20x) and then needs ~2x more to sort; without a bound
    /// that can exceed the process memory limit and trigger an OOM kill.
    ///
    /// `None` (default) preserves the previous behavior: an unbounded memory
    /// pool with no spilling. Callers running under a hard memory limit (e.g. a
    /// container cgroup) should set this to a fraction of that limit.
    #[builder(default)]
    pub max_memory_bytes: Option<usize>,

    /// Directory used for on-disk spill files when `max_memory_bytes` is set.
    ///
    /// Only takes effect together with `max_memory_bytes`; ignored otherwise.
    /// `None` (default) uses the OS temporary directory. Set this when the OS
    /// temp dir is unsuitable — e.g. to point spills at a dedicated ephemeral
    /// volume with enough free space, or away from a small/`noexec` `/tmp`.
    #[builder(default)]
    pub spill_dir: Option<std::path::PathBuf>,
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

#[cfg(test)]
mod tests {
    use parquet::schema::types::ColumnPath;

    use super::*;

    #[test]
    fn test_auto_defaults_define_both_predicates() {
        let config = AutoCompactionConfig::default();

        assert_eq!(
            config.small_file_threshold_bytes,
            DEFAULT_SMALL_FILE_THRESHOLD
        );
        assert_eq!(
            config.min_delete_file_count_threshold,
            DEFAULT_MIN_DELETE_FILE_COUNT_THRESHOLD
        );
    }

    #[test]
    fn test_planning_config_defaults_to_full_partition_pipeline() {
        let config = CompactionPlanningConfig::default();

        assert!(matches!(config.strategy, CompactionStrategy::Full));
        assert_eq!(config.file_group_scope, FileGroupScope::Partition);
    }

    #[test]
    fn test_planning_config_rejects_zero_parallelism_inputs() {
        let cases = [
            (
                CompactionPlanningConfigBuilder::default()
                    .max_file_count_per_partition(0_usize)
                    .build()
                    .unwrap(),
                "Invalid configuration: max_file_count_per_partition must be greater than 0",
            ),
            (
                CompactionPlanningConfigBuilder::default()
                    .max_input_parallelism(0_usize)
                    .build()
                    .unwrap(),
                "Invalid configuration: max_input_parallelism must be greater than 0",
            ),
            (
                CompactionPlanningConfigBuilder::default()
                    .max_output_parallelism(0_usize)
                    .build()
                    .unwrap(),
                "Invalid configuration: max_output_parallelism must be greater than 0",
            ),
        ];

        for (config, expected_error) in cases {
            assert_eq!(config.validate().unwrap_err().to_string(), expected_error);
        }
    }

    #[test]
    fn test_execution_default_sets_max_row_group_bytes() {
        let config = CompactionExecutionConfig::default();
        assert_eq!(
            config.write_parquet_properties.max_row_group_bytes(),
            Some(DEFAULT_MAX_ROW_GROUP_BYTES)
        );
        assert_eq!(
            config
                .write_parquet_properties
                .compression(&ColumnPath::new(vec![])),
            Compression::ZSTD(Default::default())
        );
    }
}
