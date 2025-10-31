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

//! Compaction file selection and grouping strategies.
//!
//! Implements a three-stage pipeline:
//! 1. File filters: Exclude files by size, delete count, or minimum file threshold
//! 2. Grouping: Combine files using Noop (all-in-one) or BinPack (First-Fit Decreasing)
//! 3. Group filters: Remove groups below size/count thresholds
//!
//! Parallelism is calculated per group based on file size and count constraints.

use crate::config::{CompactionPlanningConfig, GroupingStrategy};
use crate::{CompactionError, Result};
use iceberg::scan::FileScanTask;

use super::packer::ListPacker;

/// Bundle of data files and associated delete files for compaction.
///
/// Delete files are deduplicated by path during construction. Position deletes
/// have `project_field_ids` cleared; equality deletes use `equality_ids`.
///
/// # Fields
/// - `total_size`: Sum of `data_files[*].length` (excludes delete file sizes)
/// - `executor_parallelism`, `output_parallelism`: Set to 1 by default, calculated by
///   [`with_parallelism`](Self::with_parallelism) or [`with_calculated_parallelism`](Self::with_calculated_parallelism)
#[derive(Debug, Clone)]
pub struct FileGroup {
    pub data_files: Vec<FileScanTask>,
    pub position_delete_files: Vec<FileScanTask>,
    pub equality_delete_files: Vec<FileScanTask>,
    /// Sum of data file sizes only. Use [`input_total_bytes`](Self::input_total_bytes) for all files.
    pub total_size: u64,
    pub data_file_count: usize,
    pub executor_parallelism: usize,
    pub output_parallelism: usize,
}

impl FileGroup {
    /// Constructs a FileGroup from data files.
    ///
    /// Deduplicates delete files by `data_file_path`. Position delete files have
    /// `project_field_ids` reset to empty; equality delete files copy `equality_ids`
    /// to `project_field_ids`.
    ///
    /// Sets `executor_parallelism` and `output_parallelism` to 1.
    pub fn new(data_files: Vec<FileScanTask>) -> Self {
        let total_size = data_files.iter().map(|task| task.length).sum();
        let data_file_count = data_files.len();

        // De-duplicate delete files by path
        let mut position_delete_map = std::collections::HashMap::new();
        let mut equality_delete_map = std::collections::HashMap::new();

        for task in &data_files {
            for delete_task in &task.deletes {
                let mut delete_task = delete_task.as_ref().clone();
                match &delete_task.data_file_content {
                    iceberg::spec::DataContentType::PositionDeletes => {
                        delete_task.project_field_ids = vec![];
                        position_delete_map.insert(delete_task.data_file_path.clone(), delete_task);
                    }
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        delete_task.project_field_ids = delete_task.equality_ids.clone();
                        equality_delete_map.insert(delete_task.data_file_path.clone(), delete_task);
                    }
                    _ => {}
                }
            }
        }

        let position_delete_files = position_delete_map.into_values().collect();
        let equality_delete_files = equality_delete_map.into_values().collect();

        Self {
            data_files,
            position_delete_files,
            equality_delete_files,
            total_size,
            data_file_count,
            executor_parallelism: 1,
            output_parallelism: 1,
        }
    }

    /// Creates a FileGroup with calculated parallelism.
    ///
    /// # Errors
    /// Returns [`CompactionError::Execution`] if `input_total_bytes()` is 0.
    pub fn with_parallelism(
        data_files: Vec<FileScanTask>,
        config: &CompactionPlanningConfig,
    ) -> Result<Self> {
        let mut file_group = Self::new(data_files);
        let (executor_parallelism, output_parallelism) =
            Self::calculate_parallelism(&file_group, config)?;
        file_group.executor_parallelism = executor_parallelism;
        file_group.output_parallelism = output_parallelism;
        Ok(file_group)
    }

    /// Returns an empty FileGroup with parallelism set to 1.
    pub fn empty() -> Self {
        Self {
            data_files: Vec::new(),
            position_delete_files: Vec::new(),
            equality_delete_files: Vec::new(),
            total_size: 0,
            data_file_count: 0,
            executor_parallelism: 1,
            output_parallelism: 1,
        }
    }

    /// Calculates and sets parallelism fields.
    ///
    /// # Errors
    /// Returns [`CompactionError::Execution`] if `input_total_bytes()` is 0.
    pub fn with_calculated_parallelism(
        mut self,
        config: &CompactionPlanningConfig,
    ) -> Result<Self> {
        let (executor_parallelism, output_parallelism) =
            Self::calculate_parallelism(&self, config)?;
        self.executor_parallelism = executor_parallelism;
        self.output_parallelism = output_parallelism;
        Ok(self)
    }

    /// Calculates executor and output parallelism.
    ///
    /// - `partition_by_size` = `ceil(input_total_bytes / min_size_per_partition)`
    /// - `partition_by_count` = `ceil(input_files_count / max_file_count_per_partition)`
    /// - `executor_parallelism` = `min(max(partition_by_size, partition_by_count), max_parallelism)`
    /// - `output_parallelism` = `min(partition_by_size, executor_parallelism, max_parallelism)`
    ///
    /// If `enable_heuristic_output_parallelism` is true and total data file size
    /// is below `target_file_size_bytes`, `output_parallelism` is set to 1.
    ///
    /// # Errors
    /// Returns error if `input_total_bytes()` is 0.
    fn calculate_parallelism(
        files_to_compact: &FileGroup,
        config: &CompactionPlanningConfig,
    ) -> Result<(usize, usize)> {
        let total_file_size_for_partitioning = files_to_compact.input_total_bytes();
        if total_file_size_for_partitioning == 0 {
            return Err(CompactionError::Execution(
                "No files to calculate task parallelism".to_owned(),
            ));
        }

        let partition_by_size = total_file_size_for_partitioning
            .div_ceil(config.min_size_per_partition())
            .max(1) as usize;

        let total_files_count_for_partitioning = files_to_compact.input_files_count();

        let partition_by_count = total_files_count_for_partitioning
            .div_ceil(config.max_file_count_per_partition())
            .max(1);

        let input_parallelism = partition_by_size
            .max(partition_by_count)
            .min(config.max_parallelism());

        let mut output_parallelism = partition_by_size
            .min(input_parallelism)
            .min(config.max_parallelism());

        output_parallelism =
            Self::apply_output_parallelism_heuristic(files_to_compact, config, output_parallelism);

        Ok((input_parallelism, output_parallelism))
    }

    /// Returns 1 if heuristic enabled, current parallelism > 1, and total data file
    /// size < target file size. Otherwise returns current parallelism unchanged.
    fn apply_output_parallelism_heuristic(
        files_to_compact: &FileGroup,
        config: &CompactionPlanningConfig,
        current_output_parallelism: usize,
    ) -> usize {
        if !config.enable_heuristic_output_parallelism() || current_output_parallelism <= 1 {
            return current_output_parallelism;
        }

        let total_data_file_size = files_to_compact
            .data_files
            .iter()
            .map(|f| f.file_size_in_bytes)
            .sum::<u64>();

        if total_data_file_size > 0 && total_data_file_size < config.target_file_size_bytes() {
            1
        } else {
            current_output_parallelism
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data_files.is_empty()
    }

    /// Returns `total_size` in MB (divides by 1024²).
    pub fn total_size_mb(&self) -> u64 {
        self.total_size / 1024 / 1024
    }

    /// Consumes self, returning the data files.
    pub fn into_files(self) -> Vec<FileScanTask> {
        self.data_files
    }

    /// Returns count of data files + position deletes + equality deletes.
    pub fn input_files_count(&self) -> usize {
        self.data_files.len() + self.position_delete_files.len() + self.equality_delete_files.len()
    }

    /// Returns sum of `file_size_in_bytes` for all data, position delete, and equality delete files.
    pub fn input_total_bytes(&self) -> u64 {
        self.data_files
            .iter()
            .chain(&self.position_delete_files)
            .chain(&self.equality_delete_files)
            .map(|task| task.file_size_in_bytes)
            .sum()
    }
}

/// File filter applied before grouping.
///
/// Implementations must be `Debug + Display + Sync + Send`. Applied sequentially
/// by [`PlanStrategy`].
pub trait FileFilterStrategy: std::fmt::Debug + std::fmt::Display + Sync + Send {
    /// Returns filtered subset of data files.
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask>;
}

/// Enum dispatching to grouping strategy implementations.
#[derive(Debug)]
pub enum GroupingStrategyEnum {
    Noop(NoopGroupingStrategy),
    BinPack(BinPackGroupingStrategy),
}

impl GroupingStrategyEnum {
    pub fn group_files<I>(&self, data_files: I) -> Vec<FileGroup>
    where
        I: Iterator<Item = FileScanTask>,
    {
        match self {
            GroupingStrategyEnum::Noop(strategy) => strategy.group_files(data_files),
            GroupingStrategyEnum::BinPack(strategy) => strategy.group_files(data_files),
        }
    }
}

impl std::fmt::Display for GroupingStrategyEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupingStrategyEnum::Noop(strategy) => write!(f, "{}", strategy),
            GroupingStrategyEnum::BinPack(strategy) => write!(f, "{}", strategy),
        }
    }
}

/// Group filter applied after grouping.
///
/// Implementations must be `Debug + Display + Sync + Send`. Applied sequentially
/// by [`PlanStrategy`].
pub trait GroupFilterStrategy: std::fmt::Debug + std::fmt::Display + Sync + Send {
    /// Returns filtered subset of groups.
    fn filter_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup>;
}

/// No-op file filter. Returns input unchanged.
#[derive(Debug)]
pub struct NoopStrategy;

impl FileFilterStrategy for NoopStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask> {
        data_files
    }
}

impl std::fmt::Display for NoopStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Noop")
    }
}

/// No-op grouping strategy. Groups all files into a single FileGroup.
///
/// Returns empty vec if input is empty.
#[derive(Debug)]
pub struct NoopGroupingStrategy;

impl NoopGroupingStrategy {
    pub fn group_files<I>(&self, data_files: I) -> Vec<FileGroup>
    where
        I: Iterator<Item = FileScanTask>,
    {
        let files: Vec<FileScanTask> = data_files.collect();
        if files.is_empty() {
            vec![]
        } else {
            vec![FileGroup::new(files)]
        }
    }
}

impl std::fmt::Display for NoopGroupingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoopGrouping")
    }
}

/// No-op group filter. Returns input unchanged.
#[derive(Debug)]
pub struct NoopGroupFilterStrategy;

impl GroupFilterStrategy for NoopGroupFilterStrategy {
    fn filter_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup> {
        groups
    }
}

impl std::fmt::Display for NoopGroupFilterStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoopGroupFilter")
    }
}

/// Bin-packing grouping using First-Fit Decreasing with lookback=1.
///
/// Uses [`ListPacker`] to pack files by `file_size_in_bytes`. Filters out empty groups.
#[derive(Debug)]
pub struct BinPackGroupingStrategy {
    pub target_group_size: u64,
}

impl BinPackGroupingStrategy {
    pub fn new(target_group_size: u64) -> Self {
        Self { target_group_size }
    }

    pub fn group_files<I>(&self, data_files: I) -> Vec<FileGroup>
    where
        I: Iterator<Item = FileScanTask>,
    {
        let files: Vec<FileScanTask> = data_files.collect();

        if files.is_empty() {
            return vec![];
        }

        let packer = ListPacker::new(self.target_group_size);
        let groups = packer.pack(files, |task| task.file_size_in_bytes);

        groups
            .into_iter()
            .map(FileGroup::new)
            .filter(|group| !group.is_empty())
            .collect()
    }
}

impl std::fmt::Display for BinPackGroupingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BinPackGrouping[target={}MB]",
            self.target_group_size / 1024 / 1024
        )
    }
}

/// File filter by size threshold.
///
/// Filters by `task.length`. Bounds are inclusive. If both `None`, passes all files.
#[derive(Debug)]
pub struct SizeFilterStrategy {
    pub min_size: Option<u64>,
    pub max_size: Option<u64>,
}

impl FileFilterStrategy for SizeFilterStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask> {
        data_files
            .into_iter()
            .filter(|task| {
                let file_size = task.length;
                match (self.min_size, self.max_size) {
                    (Some(min), Some(max)) => file_size >= min && file_size <= max,
                    (Some(min), None) => file_size >= min,
                    (None, Some(max)) => file_size <= max,
                    (None, None) => true,
                }
            })
            .collect()
    }
}

impl std::fmt::Display for SizeFilterStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.min_size, self.max_size) {
            (Some(min), Some(max)) => {
                write!(
                    f,
                    "SizeFilter[{}-{}MB]",
                    min / 1024 / 1024,
                    max / 1024 / 1024
                )
            }
            (Some(min), None) => write!(f, "SizeFilter[>{}MB]", min / 1024 / 1024),
            (None, Some(max)) => write!(f, "SizeFilter[<{}MB]", max / 1024 / 1024),
            (None, None) => write!(f, "SizeFilter[Any]"),
        }
    }
}

/// File filter by delete file count.
///
/// Selects files where `task.deletes.len() >= min_delete_file_count`.
#[derive(Debug)]
pub struct DeleteFileCountFilterStrategy {
    /// Minimum delete count threshold (inclusive).
    pub min_delete_file_count: usize,
}

impl DeleteFileCountFilterStrategy {
    pub fn new(min_delete_file_count: usize) -> Self {
        Self {
            min_delete_file_count,
        }
    }
}

impl FileFilterStrategy for DeleteFileCountFilterStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask> {
        data_files
            .into_iter()
            .filter(|task| {
                let delete_count = task.deletes.len();
                delete_count >= self.min_delete_file_count
            })
            .collect()
    }
}

impl std::fmt::Display for DeleteFileCountFilterStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeleteFileCountFilter[>={} deletes]",
            self.min_delete_file_count
        )
    }
}

/// File filter requiring minimum file count.
///
/// Returns all files if `len >= min_file_count`, otherwise returns empty vec.
#[derive(Debug)]
pub struct MinFileCountStrategy {
    pub min_file_count: usize,
}

impl MinFileCountStrategy {
    pub fn new(min_file_count: usize) -> Self {
        Self { min_file_count }
    }
}

impl FileFilterStrategy for MinFileCountStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask> {
        if data_files.len() >= self.min_file_count {
            data_files
        } else {
            vec![]
        }
    }
}

impl std::fmt::Display for MinFileCountStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MinFileCount[{}]", self.min_file_count)
    }
}

/// Group filter by minimum total size.
///
/// Filters by `group.total_size >= min_group_size`.
#[derive(Debug)]
pub struct MinGroupSizeStrategy {
    pub min_group_size: u64,
}

impl GroupFilterStrategy for MinGroupSizeStrategy {
    fn filter_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup> {
        groups
            .into_iter()
            .filter(|group| group.total_size >= self.min_group_size)
            .collect()
    }
}

impl std::fmt::Display for MinGroupSizeStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MinGroupSize[{}MB]", self.min_group_size / 1024 / 1024)
    }
}

/// Group filter by minimum file count.
///
/// Filters by `group.data_file_count >= min_file_count`.
#[derive(Debug)]
pub struct MinGroupFileCountStrategy {
    pub min_file_count: usize,
}

impl GroupFilterStrategy for MinGroupFileCountStrategy {
    fn filter_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup> {
        groups
            .into_iter()
            .filter(|group| group.data_file_count >= self.min_file_count)
            .collect()
    }
}

impl std::fmt::Display for MinGroupFileCountStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MinGroupFileCount[{}]", self.min_file_count)
    }
}

/// Three-stage pipeline: file filters → grouping → group filters → parallelism calculation.
///
/// Filters and group filters are applied sequentially. See [`execute`](Self::execute) for details.
#[derive(Debug)]
pub struct PlanStrategy {
    file_filters: Vec<Box<dyn FileFilterStrategy>>,
    grouping: GroupingStrategyEnum,
    group_filters: Vec<Box<dyn GroupFilterStrategy>>,
}

impl PlanStrategy {
    pub fn new(
        file_filters: Vec<Box<dyn FileFilterStrategy>>,
        grouping: GroupingStrategyEnum,
        group_filters: Vec<Box<dyn GroupFilterStrategy>>,
    ) -> Self {
        Self {
            file_filters,
            grouping,
            group_filters,
        }
    }

    /// Executes the pipeline:
    /// 1. Apply each file filter sequentially
    /// 2. Group files using grouping strategy
    /// 3. Apply each group filter sequentially
    /// 4. Calculate parallelism for each group via [`FileGroup::with_calculated_parallelism`]
    ///
    /// # Errors
    /// Propagates errors from parallelism calculation (fails if group has 0 bytes).
    pub fn execute(
        &self,
        data_files: Vec<FileScanTask>,
        config: &CompactionPlanningConfig,
    ) -> Result<Vec<FileGroup>> {
        let mut filtered_files = data_files;
        for filter in &self.file_filters {
            filtered_files = filter.filter(filtered_files);
        }

        let file_groups = self.grouping.group_files(filtered_files.into_iter());

        let mut file_groups = file_groups;
        for filter in &self.group_filters {
            file_groups = filter.filter_groups(file_groups);
        }

        file_groups
            .into_iter()
            .map(|group| group.with_calculated_parallelism(config))
            .collect()
    }

    /// Constructs grouping enum and optional group filters from config.
    ///
    /// For `BinPack`, adds `MinGroupSizeStrategy` if `min_group_size_bytes > 0`,
    /// and `MinGroupFileCountStrategy` if `min_group_file_count > 0`.
    fn build_grouping_and_filters(
        grouping_strategy: &GroupingStrategy,
    ) -> (GroupingStrategyEnum, Vec<Box<dyn GroupFilterStrategy>>) {
        let grouping = match grouping_strategy {
            GroupingStrategy::Noop => GroupingStrategyEnum::Noop(NoopGroupingStrategy),
            GroupingStrategy::BinPack(bin_config) => GroupingStrategyEnum::BinPack(
                BinPackGroupingStrategy::new(bin_config.target_group_size_bytes),
            ),
        };

        let mut group_filters: Vec<Box<dyn GroupFilterStrategy>> = vec![];

        if let GroupingStrategy::BinPack(bin_config) = grouping_strategy {
            if let Some(min_size) = bin_config.min_group_size_bytes {
                if min_size > 0 {
                    group_filters.push(Box::new(MinGroupSizeStrategy {
                        min_group_size: min_size,
                    }));
                }
            }

            if let Some(min_count) = bin_config.min_group_file_count {
                if min_count > 0 {
                    group_filters.push(Box::new(MinGroupFileCountStrategy {
                        min_file_count: min_count,
                    }));
                }
            }
        }

        (grouping, group_filters)
    }

    /// Constructs strategy for small files compaction.
    ///
    /// Adds `SizeFilterStrategy` with `max_size = small_file_threshold_bytes`.
    /// Adds `MinFileCountStrategy` if `min_file_count > 0`.
    pub fn from_small_files(config: &crate::config::SmallFilesConfig) -> Self {
        let mut file_filters: Vec<Box<dyn FileFilterStrategy>> =
            vec![Box::new(SizeFilterStrategy {
                min_size: None,
                max_size: Some(config.small_file_threshold_bytes),
            })];

        if config.min_file_count > 0 {
            file_filters.push(Box::new(MinFileCountStrategy::new(config.min_file_count)));
        }

        let (grouping, group_filters) = Self::build_grouping_and_filters(&config.grouping_strategy);

        Self::new(file_filters, grouping, group_filters)
    }

    /// Constructs strategy for full compaction.
    ///
    /// No file filters. Grouping and group filters from config.
    pub fn from_full(config: &crate::config::FullCompactionConfig) -> Self {
        let file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![];

        let (grouping, group_filters) = Self::build_grouping_and_filters(&config.grouping_strategy);

        Self::new(file_filters, grouping, group_filters)
    }

    /// Constructs strategy for files with delete files.
    ///
    /// Adds `DeleteFileCountFilterStrategy` if `min_delete_file_count_threshold > 0`.
    pub fn from_files_with_deletes(config: &crate::config::FilesWithDeletesConfig) -> Self {
        let mut file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![];

        if config.min_delete_file_count_threshold > 0 {
            file_filters.push(Box::new(DeleteFileCountFilterStrategy::new(
                config.min_delete_file_count_threshold,
            )));
        }

        let (grouping, group_filters) = Self::build_grouping_and_filters(&config.grouping_strategy);

        Self::new(file_filters, grouping, group_filters)
    }

    /// Test-only builder accepting raw filter parameters.
    ///
    /// # Arguments
    /// - `size_filter`: `(min_size, max_size)` for `SizeFilterStrategy`
    /// - `delete_file_count_filter`: Threshold for `DeleteFileCountFilterStrategy`
    /// - `min_file_count`: Threshold for `MinFileCountStrategy`
    /// - `grouping_strategy`: Grouping algorithm config
    #[cfg(test)]
    pub fn new_custom(
        size_filter: Option<(Option<u64>, Option<u64>)>,
        delete_file_count_filter: Option<usize>,
        min_file_count: Option<usize>,
        grouping_strategy: GroupingStrategy,
    ) -> Self {
        let mut file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![];

        if let Some((min_size, max_size)) = size_filter {
            file_filters.push(Box::new(SizeFilterStrategy { min_size, max_size }));
        }

        if let Some(min_delete_count) = delete_file_count_filter {
            file_filters.push(Box::new(DeleteFileCountFilterStrategy::new(
                min_delete_count,
            )));
        }

        if let Some(min_file_count) = min_file_count {
            file_filters.push(Box::new(MinFileCountStrategy::new(min_file_count)));
        }

        let (grouping, group_filters) = Self::build_grouping_and_filters(&grouping_strategy);

        Self::new(file_filters, grouping, group_filters)
    }
}

impl From<&CompactionPlanningConfig> for PlanStrategy {
    fn from(config: &CompactionPlanningConfig) -> Self {
        match config {
            CompactionPlanningConfig::SmallFiles(small_files_config) => {
                PlanStrategy::from_small_files(small_files_config)
            }
            CompactionPlanningConfig::Full(full_config) => PlanStrategy::from_full(full_config),

            CompactionPlanningConfig::FilesWithDeletes(deletes_config) => {
                PlanStrategy::from_files_with_deletes(deletes_config)
            }
        }
    }
}

impl std::fmt::Display for PlanStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let file_filter_desc = if self.file_filters.is_empty() {
            "NoFileFilters".to_owned()
        } else {
            self.file_filters
                .iter()
                .map(|filter| filter.to_string())
                .collect::<Vec<_>>()
                .join(" -> ")
        };

        let group_filter_desc = if self.group_filters.is_empty() {
            "NoGroupFilters".to_owned()
        } else {
            self.group_filters
                .iter()
                .map(|filter| filter.to_string())
                .collect::<Vec<_>>()
                .join(" -> ")
        };

        write!(
            f,
            "{} -> {} -> {}",
            file_filter_desc, self.grouping, group_filter_desc
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompactionPlanningConfig, SmallFilesConfigBuilder};

    // Lazy static schema to avoid rebuilding it for every test
    use std::sync::{Arc, OnceLock};
    static TEST_SCHEMA: OnceLock<Arc<iceberg::spec::Schema>> = OnceLock::new();

    fn get_test_schema() -> Arc<iceberg::spec::Schema> {
        TEST_SCHEMA
            .get_or_init(|| Arc::new(iceberg::spec::Schema::builder().build().unwrap()))
            .clone()
    }

    /// Builder for creating test `FileScanTask` with fluent API
    #[derive(Debug)]
    pub struct TestFileBuilder {
        path: String,
        size: u64,
        has_deletes: bool,
        delete_types: Vec<iceberg::spec::DataContentType>,
    }

    impl TestFileBuilder {
        pub fn new(path: &str) -> Self {
            Self {
                path: path.to_owned(),
                size: 10 * 1024 * 1024, // Default 10MB
                has_deletes: false,
                delete_types: vec![],
            }
        }

        pub fn size(mut self, size: u64) -> Self {
            self.size = size;
            self
        }

        pub fn with_equality_deletes(mut self) -> Self {
            self.has_deletes = true;
            self.delete_types
                .push(iceberg::spec::DataContentType::EqualityDeletes);
            self
        }

        pub fn with_deletes(self) -> Self {
            self.with_equality_deletes()
        }

        pub fn build(self) -> FileScanTask {
            use iceberg::spec::{DataContentType, DataFileFormat};
            use std::sync::Arc;

            let deletes = if self.has_deletes {
                self.delete_types
                    .into_iter()
                    .enumerate()
                    .map(|(i, delete_type)| {
                        Arc::new(FileScanTask {
                            start: 0,
                            length: 1024,
                            record_count: Some(10),
                            data_file_path: format!(
                                "{}_{}_delete.parquet",
                                self.path.replace(".parquet", ""),
                                i
                            ),
                            data_file_content: delete_type,
                            data_file_format: DataFileFormat::Parquet,
                            schema: get_test_schema(),
                            project_field_ids: if delete_type == DataContentType::EqualityDeletes {
                                vec![1, 2]
                            } else {
                                vec![1]
                            },
                            predicate: None,
                            deletes: vec![],
                            sequence_number: 1,
                            equality_ids: if delete_type == DataContentType::EqualityDeletes {
                                vec![1, 2]
                            } else {
                                vec![]
                            },
                            file_size_in_bytes: 1024,
                        })
                    })
                    .collect()
            } else {
                vec![]
            };

            FileScanTask {
                start: 0,
                length: self.size,
                record_count: Some(100),
                data_file_path: self.path,
                data_file_content: DataContentType::Data,
                data_file_format: DataFileFormat::Parquet,
                schema: get_test_schema(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes,
                sequence_number: 1,
                equality_ids: vec![],
                file_size_in_bytes: self.size,
            }
        }
    }

    /// Helper functions for common test scenarios
    pub struct TestUtils;

    impl TestUtils {
        /// Execute strategy and return flattened files for testing
        pub fn execute_strategy_flat(
            strategy: &PlanStrategy,
            data_files: Vec<FileScanTask>,
        ) -> Vec<FileScanTask> {
            let config = CompactionPlanningConfig::default();

            strategy
                .execute(data_files, &config)
                .unwrap()
                .into_iter()
                .flat_map(|group| group.into_files())
                .collect()
        }

        /// Create test config with common defaults
        pub fn create_test_config() -> CompactionPlanningConfig {
            CompactionPlanningConfig::default()
        }

        /// Assert file paths (ordered) equal expected strings
        pub fn assert_paths_eq(expected: &[&str], files: &[FileScanTask]) {
            assert_eq!(files.len(), expected.len(), "length mismatch");
            for (i, (e, f)) in expected.iter().zip(files.iter()).enumerate() {
                assert_eq!(f.data_file_path, *e, "File {} should be {}", i, e);
            }
        }
    }

    #[test]
    fn test_noop_strategy() {
        let strategy = NoopStrategy;
        let data_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(20 * 1024 * 1024)
                .build(),
        ];

        let result_data: Vec<FileScanTask> = vec![strategy.filter(data_files.clone())]
            .into_iter()
            .flatten()
            .collect();

        assert_eq!(result_data.len(), data_files.len());
        assert_eq!(strategy.to_string(), "Noop");
    }

    #[test]
    fn test_size_filter_strategy() {
        let strategy = SizeFilterStrategy {
            min_size: Some(5 * 1024 * 1024),
            max_size: Some(50 * 1024 * 1024),
        };

        assert_eq!(strategy.to_string(), "SizeFilter[5-50MB]");

        let test_files = vec![
            TestFileBuilder::new("too_small.parquet")
                .size(2 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("min_edge.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("medium1.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("medium2.parquet")
                .size(30 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("max_edge.parquet")
                .size(50 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("too_large.parquet")
                .size(100 * 1024 * 1024)
                .build(),
        ];

        let result: Vec<FileScanTask> = strategy.filter(test_files);

        assert_eq!(result.len(), 4);
        let expected_files = [
            "min_edge.parquet",
            "medium1.parquet",
            "medium2.parquet",
            "max_edge.parquet",
        ];
        TestUtils::assert_paths_eq(&expected_files, &result);

        for file in &result {
            assert!(file.length >= 5 * 1024 * 1024 && file.length <= 50 * 1024 * 1024);
        }

        let boundary_files = vec![
            TestFileBuilder::new("exact_min.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("exact_max.parquet")
                .size(50 * 1024 * 1024)
                .build(),
        ];

        let boundary_result = strategy.filter(boundary_files);
        assert_eq!(boundary_result.len(), 2);
    }

    #[test]
    fn test_file_strategy_factory() {
        let small_files_config =
            CompactionPlanningConfig::SmallFiles(crate::config::SmallFilesConfig::default());

        let small_files_strategy = PlanStrategy::from(&small_files_config);
        let small_files_desc = small_files_strategy.to_string();
        assert!(small_files_desc.contains("SizeFilter"));

        let display_output = format!("{}", small_files_strategy);
        assert_eq!(display_output, small_files_desc);

        let full_config =
            CompactionPlanningConfig::Full(crate::config::FullCompactionConfig::default());
        let routed_full = PlanStrategy::from(&full_config);

        assert!(!routed_full.to_string().is_empty());

        let full_display = format!("{}", routed_full);
        assert_eq!(full_display, routed_full.to_string());
    }

    #[test]
    fn test_binpack_grouping_size_limit() {
        // Test that BinPack grouping can limit the total size per group
        use crate::config::BinPackConfig;

        let strategy = PlanStrategy::new_custom(
            None,                                                            // no size filter
            None, // no delete file count filter
            None, // no min file count
            GroupingStrategy::BinPack(BinPackConfig::new(25 * 1024 * 1024)), // 25MB target group size
        );

        // Test BinPack grouping behavior: groups should stay close to the target size, and files exceeding the target should be grouped separately
        let test_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB
            TestFileBuilder::new("file2.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB
            TestFileBuilder::new("file3.parquet")
                .size(5 * 1024 * 1024)
                .build(), // 5MB
            TestFileBuilder::new("file4.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB
            TestFileBuilder::new("file5.parquet")
                .size(5 * 1024 * 1024)
                .build(), // 5MB
        ]; // Total: 40MB

        let config = TestUtils::create_test_config();
        let groups = strategy.execute(test_files, &config).unwrap();

        // Should create multiple groups, each trying to stay around 25MB
        assert!(
            groups.len() >= 2,
            "Should create at least 2 groups for 40MB with 25MB target"
        );

        // Verify all files are included
        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 5, "All 5 files should be in groups");

        // Test with files that individually exceed target size
        let large_files = vec![
            TestFileBuilder::new("huge1.parquet")
                .size(30 * 1024 * 1024)
                .build(), // 30MB - exceeds 25MB target
            TestFileBuilder::new("huge2.parquet")
                .size(30 * 1024 * 1024)
                .build(), // 30MB - exceeds 25MB target
        ];

        let large_groups = strategy.execute(large_files, &config).unwrap();

        // Each large file should be in its own group
        assert_eq!(
            large_groups.len(),
            2,
            "Large files should be grouped separately"
        );
        assert_eq!(large_groups[0].data_file_count, 1);
        assert_eq!(large_groups[1].data_file_count, 1);
    }

    #[test]
    fn test_min_file_count_strategy() {
        let strategy = MinFileCountStrategy::new(3);

        assert_eq!(strategy.to_string(), "MinFileCount[3]");

        let exact_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(20 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file3.parquet")
                .size(15 * 1024 * 1024)
                .build(),
        ];

        let exact_result: Vec<FileScanTask> = strategy.filter(exact_files.clone());
        assert_eq!(exact_result.len(), 3);
        for (i, file) in exact_result.iter().enumerate() {
            assert_eq!(file.data_file_path, exact_files[i].data_file_path);
        }

        let more_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(20 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file3.parquet")
                .size(15 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file4.parquet")
                .size(25 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file5.parquet")
                .size(30 * 1024 * 1024)
                .build(),
        ];

        let more_result: Vec<FileScanTask> = strategy.filter(more_files);
        assert_eq!(more_result.len(), 5);

        let fewer_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(20 * 1024 * 1024)
                .build(),
        ];

        let fewer_result: Vec<FileScanTask> = strategy.filter(fewer_files);
        assert_eq!(fewer_result.len(), 0);

        let zero_strategy = MinFileCountStrategy::new(0);
        assert_eq!(zero_strategy.to_string(), "MinFileCount[0]");

        let single_file = vec![TestFileBuilder::new("single.parquet")
            .size(10 * 1024 * 1024)
            .build()];
        let zero_result: Vec<FileScanTask> = zero_strategy.filter(single_file.clone());
        assert_eq!(zero_result.len(), 1, "min_count=0 should always pass files");
        TestUtils::assert_paths_eq(&["single.parquet"], &zero_result);

        // Test edge case: min_count = 1 (minimal threshold)
        let one_strategy = MinFileCountStrategy::new(1);
        let one_result: Vec<FileScanTask> = one_strategy.filter(single_file);
        assert_eq!(one_result.len(), 1, "min_count=1 should pass single file");
    }

    #[test]
    fn test_small_files_strategy_comprehensive() {
        // Test small files strategy with basic functionality
        let small_files_config = SmallFilesConfigBuilder::default()
            .small_file_threshold_bytes(20 * 1024 * 1024_u64) // 20MB threshold
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::SmallFiles(small_files_config);

        let strategy = PlanStrategy::from(&config);

        // Description should reflect core filters
        let desc = strategy.to_string();
        assert!(desc.contains("SizeFilter"));

        let test_files = vec![
            TestFileBuilder::new("small1.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("small2.parquet")
                .size(15 * 1024 * 1024)
                .with_deletes()
                .build(),
            TestFileBuilder::new("small3.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("large.parquet")
                .size(25 * 1024 * 1024)
                .build(),
        ];

        let result = TestUtils::execute_strategy_flat(&strategy, test_files);

        // Now includes small2 with deletes since we removed the NoDeleteFilesStrategy
        assert_eq!(result.len(), 3);
        let mut paths: Vec<_> = result.iter().map(|f| f.data_file_path.as_str()).collect();
        paths.sort();
        assert_eq!(
            paths,
            vec!["small1.parquet", "small2.parquet", "small3.parquet"]
        );

        // Verify all selected files meet criteria
        let small_file_threshold = match config {
            CompactionPlanningConfig::SmallFiles(sf_config) => sf_config.small_file_threshold_bytes,
            _ => panic!("Expected small files config"),
        };
        for file in &result {
            assert!(file.length <= small_file_threshold);
            // Don't check deletes.is_empty() as small files can have deletes
        }

        // Test min_file_count behavior
        let min_count_small_files_config = SmallFilesConfigBuilder::default()
            .small_file_threshold_bytes(20 * 1024 * 1024_u64)
            .min_file_count(3_usize)
            .build()
            .unwrap();
        let min_count_config = CompactionPlanningConfig::SmallFiles(min_count_small_files_config);

        let min_count_strategy = PlanStrategy::from(&min_count_config);

        // Test with insufficient files
        let insufficient_files = vec![
            TestFileBuilder::new("small1.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("small2.parquet")
                .size(10 * 1024 * 1024)
                .build(),
        ];
        let insufficient_result =
            TestUtils::execute_strategy_flat(&min_count_strategy, insufficient_files);
        assert_eq!(insufficient_result.len(), 0);

        // Test with sufficient files
        let sufficient_files = vec![
            TestFileBuilder::new("small1.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("small2.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("small3.parquet")
                .size(15 * 1024 * 1024)
                .build(),
        ];
        let sufficient_result =
            TestUtils::execute_strategy_flat(&min_count_strategy, sufficient_files);
        assert_eq!(sufficient_result.len(), 3);

        // Also validate default configuration behavior (merge former test_config_behavior)
        let default_small_files_config = SmallFilesConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::Noop)
            .build()
            .unwrap();
        let default_config = CompactionPlanningConfig::SmallFiles(default_small_files_config);

        // Verify min_file_count default
        match &default_config {
            CompactionPlanningConfig::SmallFiles(config) => {
                assert_eq!(config.min_file_count, 0);
            }
            _ => panic!("Expected small files config"),
        }

        let default_strategy = PlanStrategy::from(&default_config);

        // Single file passes
        let single_file = vec![TestFileBuilder::new("single.parquet")
            .size(5 * 1024 * 1024)
            .build()];
        let single_result = TestUtils::execute_strategy_flat(&default_strategy, single_file);
        assert_eq!(single_result.len(), 1);
        TestUtils::assert_paths_eq(&["single.parquet"], &single_result);

        // Multiple files all pass
        let multiple_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(8 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file3.parquet")
                .size(12 * 1024 * 1024)
                .build(),
        ];
        let multi_result = TestUtils::execute_strategy_flat(&default_strategy, multiple_files);
        assert_eq!(multi_result.len(), 3);
        let small_file_threshold_default = match &default_config {
            CompactionPlanningConfig::SmallFiles(config) => config.small_file_threshold_bytes,
            _ => panic!("Expected small files config"),
        };
        for file in &multi_result {
            assert!(file.length <= small_file_threshold_default);
            assert!(file.deletes.is_empty());
        }
    }

    #[test]
    fn test_group_filter_strategies_with_file_groups() {
        // Test that the FileGroup-based group filter strategies work correctly

        // Create test file groups
        let small_group = FileGroup::new(vec![
            TestFileBuilder::new("small1.parquet")
                .size(5 * 1024 * 1024)
                .build(), // 5MB
            TestFileBuilder::new("small2.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB
        ]);

        let large_group = FileGroup::new(vec![
            TestFileBuilder::new("large1.parquet")
                .size(50 * 1024 * 1024)
                .build(), // 50MB
            TestFileBuilder::new("large2.parquet")
                .size(100 * 1024 * 1024)
                .build(), // 100MB
            TestFileBuilder::new("large3.parquet")
                .size(75 * 1024 * 1024)
                .build(), // 75MB
        ]);

        let single_file_group = FileGroup::new(vec![
            TestFileBuilder::new("single.parquet")
                .size(20 * 1024 * 1024)
                .build(), // 20MB
        ]);

        let groups = vec![
            small_group.clone(),
            large_group.clone(),
            single_file_group.clone(),
        ];

        // Test MinGroupSizeStrategy
        let min_size_strategy = MinGroupSizeStrategy {
            min_group_size: 100 * 1024 * 1024,
        }; // 100MB min
        let filtered_by_size = min_size_strategy.filter_groups(groups.clone());
        assert_eq!(filtered_by_size.len(), 1); // Only large_group should pass (225MB total)
        assert_eq!(filtered_by_size[0].total_size, large_group.total_size);

        // Test MinGroupFileCountStrategy
        let min_file_count_strategy = MinGroupFileCountStrategy { min_file_count: 2 };
        let filtered_by_min_count = min_file_count_strategy.filter_groups(groups.clone());
        assert_eq!(filtered_by_min_count.len(), 2); // small_group (2 files) and large_group (3 files) should pass

        // Test NoopGroupFilterStrategy
        let noop_strategy = NoopGroupFilterStrategy;
        let noop_result = noop_strategy.filter_groups(groups.clone());
        assert_eq!(noop_result.len(), 3); // All groups should pass through

        // Test that descriptions are formatted correctly
        assert_eq!(min_size_strategy.to_string(), "MinGroupSize[100MB]");
        assert_eq!(min_file_count_strategy.to_string(), "MinGroupFileCount[2]");
        assert_eq!(noop_strategy.to_string(), "NoopGroupFilter");
    }

    #[test]
    fn test_create_custom_strategy_comprehensive() {
        use crate::config::GroupingStrategy;

        // Test create_custom_strategy with basic parameter combinations

        // Test case 1: With size filter
        let strategy_with_size_filter = PlanStrategy::new_custom(
            Some((Some(1024 * 1024), Some(100 * 1024 * 1024))), // size_filter: 1MB-100MB
            None,                                               // no delete file count filter
            None,                                               // no min_file_count
            GroupingStrategy::Noop,
        );

        let description = strategy_with_size_filter.to_string();
        assert!(description.contains("SizeFilter"));

        // Test case 2: Minimal filters
        let strategy_minimal = PlanStrategy::new_custom(None, None, None, GroupingStrategy::Noop);

        // Test functional behavior
        let test_files = vec![
            TestFileBuilder::new("small.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("medium.parquet")
                .size(50 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("large.parquet")
                .size(200 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("with_deletes.parquet")
                .size(30 * 1024 * 1024)
                .with_deletes()
                .build(),
        ];

        // Strategy with size filter should exclude large files (>100MB)
        let result_filtered =
            TestUtils::execute_strategy_flat(&strategy_with_size_filter, test_files.clone());
        assert_eq!(result_filtered.len(), 3); // small, medium, and with_deletes (all under 100MB)

        // Minimal strategy should pass all files
        let result_minimal = TestUtils::execute_strategy_flat(&strategy_minimal, test_files);
        assert_eq!(result_minimal.len(), 4);
    }

    #[test]
    fn test_binpack_grouping_comprehensive() {
        // Test Case 1: Normal target size behavior
        let normal_strategy = BinPackGroupingStrategy::new(20 * 1024 * 1024); // 20MB

        let small_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file3.parquet")
                .size(15 * 1024 * 1024)
                .build(),
        ]; // Total: 30MB

        let groups = normal_strategy.group_files(small_files.into_iter());
        assert_eq!(groups.len(), 2); // ceil(30MB / 20MB) = 2 groups
        let mut counts: Vec<usize> = groups.iter().map(|g| g.data_file_count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![1, 2]);

        // Test Case 2: Zero target size (regression test)
        let zero_strategy = BinPackGroupingStrategy::new(0);

        let test_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(10 * 1024 * 1024)
                .build(),
        ];

        let start = std::time::Instant::now();
        let zero_groups = zero_strategy.group_files(test_files.into_iter());
        let duration = start.elapsed();

        assert!(duration.as_secs() < 1); // Should complete quickly
        assert_eq!(zero_groups.len(), 1); // Should create single group
        assert_eq!(zero_groups[0].data_file_count, 2);

        // Test Case 3: Large target group size - all files fit in one group
        let large_target_strategy = BinPackGroupingStrategy::new(1024 * 1024 * 1024); // 1GB

        let many_files = vec![
            TestFileBuilder::new("f1.parquet").size(1024 * 1024).build(),
            TestFileBuilder::new("f2.parquet").size(1024 * 1024).build(),
            TestFileBuilder::new("f3.parquet").size(1024 * 1024).build(),
            TestFileBuilder::new("f4.parquet").size(1024 * 1024).build(),
            TestFileBuilder::new("f5.parquet").size(1024 * 1024).build(),
        ];

        let groups = large_target_strategy.group_files(many_files.into_iter());

        // With 1GB target and only 5MB total, all files go into 1 group
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].data_file_count, 5);

        assert_eq!(normal_strategy.to_string(), "BinPackGrouping[target=20MB]");

        // Test Case 4: Large total size relative to target produces multiple groups (via factory)
        // Mirrors prior factory-based test but keeps coverage centralized here.
        use crate::config::BinPackConfig;
        let bin_pack_strategy = PlanStrategy::new_custom(
            None,
            None,
            None,
            GroupingStrategy::BinPack(BinPackConfig::new(64 * 1024 * 1024)), // 64MB target, no filters
        );

        let config = TestUtils::create_test_config();
        let large_files = vec![
            TestFileBuilder::new("large1.parquet")
                .size(50 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("large2.parquet")
                .size(50 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("large3.parquet")
                .size(50 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("large4.parquet")
                .size(50 * 1024 * 1024)
                .build(),
        ]; // 200MB total

        let large_groups = bin_pack_strategy.execute(large_files, &config).unwrap();
        assert!(large_groups.len() >= 3);
        let total_files: usize = large_groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 4);
    }

    // Removed standalone Noop grouping test; covered by enum variant test below

    #[test]
    fn test_grouping_strategy_enum() {
        let data_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(5 * 1024 * 1024)
                .build(), // 5MB
            TestFileBuilder::new("file2.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB
        ];

        // Test Noop variant
        let noop_enum = GroupingStrategyEnum::Noop(NoopGroupingStrategy);
        let noop_groups = noop_enum.group_files(data_files.clone().into_iter());
        assert_eq!(
            noop_groups.len(),
            1,
            "Noop should create one single group containing all files"
        );
        assert_eq!(noop_groups[0].data_file_count, 2);
        assert_eq!(noop_enum.to_string(), "NoopGrouping");

        // Single-file case for Noop
        let single = vec![TestFileBuilder::new("single.parquet")
            .size(20 * 1024 * 1024)
            .build()];
        let single_groups = noop_enum.group_files(single.into_iter());
        assert_eq!(single_groups.len(), 1);
        assert_eq!(single_groups[0].data_file_count, 1);
        TestUtils::assert_paths_eq(&["single.parquet"], &single_groups[0].data_files);

        // Test Noop variant with empty files
        let empty_files: Vec<FileScanTask> = vec![];
        let noop_empty_groups = noop_enum.group_files(empty_files.into_iter());
        assert_eq!(
            noop_empty_groups.len(),
            0,
            "Noop should return empty for empty input"
        );

        // Test BinPack variant
        let binpack_enum =
            GroupingStrategyEnum::BinPack(BinPackGroupingStrategy::new(20 * 1024 * 1024));
        let binpack_groups = binpack_enum.group_files(data_files.into_iter());
        assert!(
            !binpack_groups.is_empty(),
            "BinPack should create at least one group"
        );
        assert_eq!(binpack_enum.to_string(), "BinPackGrouping[target=20MB]");
    }

    #[test]
    fn test_strategy_descriptions_and_edge_cases() {
        // Combined test for filter strategies descriptions and edge cases

        // Size filter edge cases
        let min_only = SizeFilterStrategy {
            min_size: Some(10 * 1024 * 1024), // 10MB min
            max_size: None,
        };
        assert_eq!(min_only.to_string(), "SizeFilter[>10MB]");

        let max_only = SizeFilterStrategy {
            min_size: None,
            max_size: Some(50 * 1024 * 1024), // 50MB max
        };
        assert_eq!(max_only.to_string(), "SizeFilter[<50MB]");

        let no_filter = SizeFilterStrategy {
            min_size: None,
            max_size: None,
        };
        assert_eq!(no_filter.to_string(), "SizeFilter[Any]");

        // Test functional behavior for edge cases
        let test_files = vec![
            TestFileBuilder::new("small.parquet")
                .size(5 * 1024 * 1024)
                .build(), // 5MB
            TestFileBuilder::new("medium.parquet")
                .size(15 * 1024 * 1024)
                .build(), // 15MB
            TestFileBuilder::new("large.parquet")
                .size(100 * 1024 * 1024)
                .build(), // 100MB
        ];

        // Test min-only filter
        let min_result = min_only.filter(test_files.clone());
        assert_eq!(min_result.len(), 2); // medium and large should pass
        assert_eq!(min_result[0].data_file_path, "medium.parquet");
        assert_eq!(min_result[1].data_file_path, "large.parquet");

        // Test max-only filter
        let max_result = max_only.filter(test_files.clone());
        assert_eq!(max_result.len(), 2); // small and medium should pass
        assert_eq!(max_result[0].data_file_path, "small.parquet");
        assert_eq!(max_result[1].data_file_path, "medium.parquet");

        // Test no-filter (should pass all)
        let no_filter_result = no_filter.filter(test_files.clone());
        assert_eq!(no_filter_result.len(), 3);

        // Task size limit description and behavior covered in test_task_size_limit_strategy
    }

    #[test]
    fn test_file_group_parallelism_calculation() {
        // Test FileGroup::calculate_parallelism functionality
        let small_files_config = SmallFilesConfigBuilder::default()
            .min_size_per_partition(10 * 1024 * 1024_u64) // 10MB per partition
            .max_file_count_per_partition(5_usize) // 5 files per partition
            .max_parallelism(8_usize) // Max 8 parallel tasks
            .enable_heuristic_output_parallelism(true)
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::SmallFiles(small_files_config);

        // Test normal case
        let files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(15 * 1024 * 1024)
                .build(), // 15MB
            TestFileBuilder::new("file2.parquet")
                .size(20 * 1024 * 1024)
                .build(), // 20MB
            TestFileBuilder::new("file3.parquet")
                .size(25 * 1024 * 1024)
                .build(), // 25MB
        ];

        let group = FileGroup::new(files);
        let (executor_parallelism, output_parallelism) =
            FileGroup::calculate_parallelism(&group, &config).unwrap();

        assert!(executor_parallelism >= 1);
        assert!(output_parallelism >= 1);
        assert!(output_parallelism <= executor_parallelism);
        assert!(executor_parallelism <= config.max_parallelism());

        // Test error case - empty group
        let empty_group = FileGroup::empty();
        let result = FileGroup::calculate_parallelism(&empty_group, &config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No files to calculate task parallelism"));
    }

    #[test]
    fn test_file_group_delete_files_extraction() {
        // Test that FileGroup correctly extracts and organizes delete files
        use iceberg::spec::{DataContentType, DataFileFormat};
        use std::sync::Arc;

        // Create a data file with both position and equality delete files
        let position_delete = Arc::new(FileScanTask {
            start: 0,
            length: 1024,
            record_count: Some(10),
            data_file_path: "pos_delete.parquet".to_owned(),
            data_file_content: DataContentType::PositionDeletes,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            sequence_number: 1,
            equality_ids: vec![],
            file_size_in_bytes: 1024,
        });

        let equality_delete = Arc::new(FileScanTask {
            start: 0,
            length: 2048,
            record_count: Some(20),
            data_file_path: "eq_delete.parquet".to_owned(),
            data_file_content: DataContentType::EqualityDeletes,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![],
            sequence_number: 1,
            equality_ids: vec![1, 2],
            file_size_in_bytes: 2048,
        });

        let data_file = FileScanTask {
            start: 0,
            length: 10 * 1024 * 1024, // 10MB
            record_count: Some(1000),
            data_file_path: "data.parquet".to_owned(),
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![position_delete, equality_delete],
            sequence_number: 1,
            equality_ids: vec![],
            file_size_in_bytes: 10 * 1024 * 1024,
        };

        let group = FileGroup::new(vec![data_file]);

        // Verify that delete files were extracted correctly
        assert_eq!(group.position_delete_files.len(), 1);
        assert_eq!(group.equality_delete_files.len(), 1);

        assert_eq!(
            group.position_delete_files[0].data_file_path,
            "pos_delete.parquet"
        );
        assert_eq!(
            group.equality_delete_files[0].data_file_path,
            "eq_delete.parquet"
        );

        // Verify total bytes calculation includes delete files
        let expected_total = 10 * 1024 * 1024 + 1024 + 2048; // data + pos_delete + eq_delete
        assert_eq!(group.input_total_bytes(), expected_total);

        // Verify file count includes delete files
        assert_eq!(group.input_files_count(), 3); // 1 data + 1 pos_delete + 1 eq_delete
    }

    #[test]
    fn test_file_group_delete_files_dedup_and_heuristic_output_parallelism() {
        // Build two data files referencing the same delete file path to ensure dedup
        use iceberg::spec::{DataContentType, DataFileFormat};
        use std::sync::Arc;

        let shared_pos_delete = Arc::new(FileScanTask {
            start: 0,
            length: 512,
            record_count: Some(10),
            data_file_path: "shared_pos_delete.parquet".to_owned(),
            data_file_content: DataContentType::PositionDeletes,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            sequence_number: 1,
            equality_ids: vec![],
            file_size_in_bytes: 512,
        });

        let f1 = FileScanTask {
            start: 0,
            length: 4 * 1024 * 1024,
            record_count: Some(100),
            data_file_path: "d1.parquet".to_owned(),
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![shared_pos_delete.clone()],
            sequence_number: 1,
            equality_ids: vec![],
            file_size_in_bytes: 4 * 1024 * 1024,
        };

        let f2 = FileScanTask {
            start: 0,
            length: 4 * 1024 * 1024,
            record_count: Some(100),
            data_file_path: "d2.parquet".to_owned(),
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![shared_pos_delete],
            sequence_number: 1,
            equality_ids: vec![],
            file_size_in_bytes: 4 * 1024 * 1024,
        };

        let group = FileGroup::new(vec![f1, f2]);
        // Dedup should keep one position delete
        assert_eq!(group.position_delete_files.len(), 1);

        // Heuristic output parallelism: data total is 8MB, below default 1GB target, so 1 output
        let small_files_config = SmallFilesConfigBuilder::default()
            .min_size_per_partition(1_u64) // allow partitioning to be driven by counts
            .max_file_count_per_partition(1_usize)
            .max_parallelism(8_usize)
            .enable_heuristic_output_parallelism(true)
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::SmallFiles(small_files_config);

        let (exec_p, out_p) = FileGroup::calculate_parallelism(&group, &config).unwrap();
        assert!(exec_p >= 1);
        assert_eq!(
            out_p, 1,
            "Heuristic should force single output when data is tiny"
        );
    }

    #[test]
    fn test_full_compaction_with_binpack_grouping() {
        // Test that Full Compaction can use BinPack to split files into multiple groups
        // and that each group's parallelism is calculated independently
        let binpack_config = crate::config::BinPackConfig {
            target_group_size_bytes: 50 * 1024 * 1024, // 50MB per group
            min_group_size_bytes: None,
            min_group_file_count: None, // No filter - we want to see all groups
        };
        let full_config = crate::config::FullCompactionConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::BinPack(binpack_config))
            .min_size_per_partition(20 * 1024 * 1024_u64) // 20MB per partition
            .max_file_count_per_partition(1_usize) // 1 file per partition
            .max_parallelism(8_usize) // Max 8 parallel tasks per group
            .enable_heuristic_output_parallelism(true)
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::Full(full_config);

        let strategy = PlanStrategy::from(&config);

        // Create test files with varying sizes to trigger different parallelism calculations
        // First-Fit Decreasing with lookback=1:
        // After sorting: [30MB, 30MB, 30MB, 10MB]
        // Group 1: file1 (30MB), remaining 20MB
        // Group 2: file2 (30MB), remaining 20MB (file2 doesn't fit in Group 1 due to lookback=1)
        // Group 3: file3 (30MB), remaining 20MB (file3 doesn't fit in Group 2 due to lookback=1)
        //          file4 (10MB) fits in Group 3 (10MB < 20MB remaining)
        // Result: 3 groups
        let test_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(30 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(30 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file3.parquet")
                .size(30 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file4.parquet")
                .size(10 * 1024 * 1024)
                .build(),
        ];

        let groups = strategy.execute(test_files.clone(), &config).unwrap();

        // Verify multiple groups were created
        assert_eq!(
            3,
            groups.len(),
            "BinPack with lookback=1 should create 3 groups, but {} groups were created",
            groups.len()
        );

        // Verify all files are included (no filtering)
        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 4, "Full compaction should include all files");

        // Verify total size is preserved
        let total_size: u64 = groups.iter().map(|g| g.total_size).sum();
        assert_eq!(
            total_size,
            100 * 1024 * 1024,
            "Total size should be preserved"
        );

        // Verify each group respects constraints
        // Note: We don't verify max_group_file_count here because BinPack doesn't enforce
        // a max file count per group - it only enforces min_group_file_count as a filter

        // **KEY VERIFICATION: Each group's parallelism is calculated independently**
        for (idx, group) in groups.iter().enumerate() {
            // Each group should have valid parallelism values
            assert!(
                group.executor_parallelism >= 1,
                "Group {} executor_parallelism must be at least 1",
                idx
            );
            assert!(
                group.output_parallelism >= 1,
                "Group {} output_parallelism must be at least 1",
                idx
            );

            // Parallelism should respect max_parallelism per group
            assert!(
                group.executor_parallelism <= config.max_parallelism(),
                "Group {} executor_parallelism ({}) should not exceed max_parallelism ({})",
                idx,
                group.executor_parallelism,
                config.max_parallelism()
            );

            // Output parallelism should not exceed executor parallelism
            assert!(
                group.output_parallelism <= group.executor_parallelism,
                "Group {} output_parallelism ({}) should not exceed executor_parallelism ({})",
                idx,
                group.output_parallelism,
                group.executor_parallelism
            );
        }

        // Verify that groups with different sizes may have different parallelism
        // (This is the key property of independent calculation)
        let parallelisms: Vec<_> = groups
            .iter()
            .map(|g| (g.executor_parallelism, g.output_parallelism))
            .collect();

        // Assert that parallelisms are non-empty and valid
        assert!(
            !parallelisms.is_empty(),
            "Should have at least one group with parallelism calculated"
        );

        // Verify that at least one group has parallelism > 1
        // (This confirms parallelism calculation is actually working)
        let has_parallelism = parallelisms.iter().any(|(exec_p, _)| *exec_p > 1);
        assert!(
            has_parallelism,
            "At least one group should have executor_parallelism > 1, got: {:?}",
            parallelisms
        );

        // Verify description mentions BinPack
        assert!(strategy.to_string().contains("BinPack"));
    }

    #[test]
    fn test_delete_file_count_filter_strategy() {
        // Test description
        let strategy = DeleteFileCountFilterStrategy::new(3);
        assert_eq!(strategy.to_string(), "DeleteFileCountFilter[>=3 deletes]");

        // Test with files having varying delete file counts
        let test_files = vec![
            TestFileBuilder::new("no_deletes.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 0 delete files
            TestFileBuilder::new("one_delete.parquet")
                .size(10 * 1024 * 1024)
                .with_deletes()
                .build(), // 1 delete file
            TestFileBuilder::new("three_deletes.parquet")
                .size(10 * 1024 * 1024)
                .build(), // Will manually add 3 deletes
            TestFileBuilder::new("five_deletes.parquet")
                .size(10 * 1024 * 1024)
                .build(), // Will manually add 5 deletes
        ];

        // Manually add multiple delete files to test files
        use iceberg::spec::{DataContentType, DataFileFormat};
        use std::sync::Arc;

        let mut files_with_deletes = vec![
            test_files[0].clone(), // no deletes
            test_files[1].clone(), // one delete (already added)
        ];

        // Add file with 3 deletes
        let mut file_with_3_deletes = test_files[2].clone();
        for i in 0..3 {
            file_with_3_deletes.deletes.push(Arc::new(FileScanTask {
                start: 0,
                length: 1024,
                record_count: Some(10),
                data_file_path: format!("delete_{}.parquet", i),
                data_file_content: DataContentType::EqualityDeletes,
                data_file_format: DataFileFormat::Parquet,
                schema: get_test_schema(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                sequence_number: 1,
                equality_ids: vec![1, 2],
                file_size_in_bytes: 1024,
            }));
        }
        files_with_deletes.push(file_with_3_deletes);

        // Add file with 5 deletes
        let mut file_with_5_deletes = test_files[3].clone();
        for i in 0..5 {
            file_with_5_deletes.deletes.push(Arc::new(FileScanTask {
                start: 0,
                length: 1024,
                record_count: Some(10),
                data_file_path: format!("delete_{}.parquet", i),
                data_file_content: DataContentType::EqualityDeletes,
                data_file_format: DataFileFormat::Parquet,
                schema: get_test_schema(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                sequence_number: 1,
                equality_ids: vec![1, 2],
                file_size_in_bytes: 1024,
            }));
        }
        files_with_deletes.push(file_with_5_deletes);

        // Apply filter - should only pass files with >= 3 delete files
        let result = strategy.filter(files_with_deletes);

        assert_eq!(result.len(), 2); // Only files with 3 and 5 deletes should pass
        assert_eq!(result[0].data_file_path, "three_deletes.parquet");
        assert_eq!(result[1].data_file_path, "five_deletes.parquet");

        // Verify delete counts
        assert_eq!(result[0].deletes.len(), 3);
        assert_eq!(result[1].deletes.len(), 5);

        // Test edge case: threshold = 0 (should pass all files)
        let zero_threshold_strategy = DeleteFileCountFilterStrategy::new(0);
        let all_files = vec![
            TestFileBuilder::new("no_deletes.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("with_deletes.parquet")
                .size(10 * 1024 * 1024)
                .with_deletes()
                .build(),
        ];
        let zero_result = zero_threshold_strategy.filter(all_files);
        assert_eq!(zero_result.len(), 2);

        // Test edge case: threshold = 1 (should pass files with 1+ deletes)
        let one_threshold_strategy = DeleteFileCountFilterStrategy::new(1);
        let test_files = vec![
            TestFileBuilder::new("no_deletes.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("with_deletes.parquet")
                .size(10 * 1024 * 1024)
                .with_deletes()
                .build(),
        ];
        let one_result = one_threshold_strategy.filter(test_files);
        assert_eq!(one_result.len(), 1);
        assert_eq!(one_result[0].data_file_path, "with_deletes.parquet");
    }

    #[test]
    fn test_files_with_deletes_strategy() {
        // Test the factory method for files with deletes strategy
        use crate::config::FilesWithDeletesConfigBuilder;

        let files_with_deletes_config = FilesWithDeletesConfigBuilder::default()
            .min_delete_file_count_threshold(2_usize)
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::FilesWithDeletes(files_with_deletes_config);

        let strategy = PlanStrategy::from(&config);

        // Verify description mentions the delete file count filter
        let desc = strategy.to_string();
        assert!(desc.contains("DeleteFileCountFilter"));

        // Create test files with different delete counts
        use iceberg::spec::{DataContentType, DataFileFormat};
        use std::sync::Arc;

        let file_no_deletes = TestFileBuilder::new("no_deletes.parquet")
            .size(10 * 1024 * 1024)
            .build();

        let mut file_one_delete = TestFileBuilder::new("one_delete.parquet")
            .size(10 * 1024 * 1024)
            .build();
        file_one_delete.deletes.push(Arc::new(FileScanTask {
            start: 0,
            length: 1024,
            record_count: Some(10),
            data_file_path: "delete_1.parquet".to_owned(),
            data_file_content: DataContentType::EqualityDeletes,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![],
            sequence_number: 1,
            equality_ids: vec![1, 2],
            file_size_in_bytes: 1024,
        }));

        let mut file_two_deletes = TestFileBuilder::new("two_deletes.parquet")
            .size(10 * 1024 * 1024)
            .build();
        for i in 0..2 {
            file_two_deletes.deletes.push(Arc::new(FileScanTask {
                start: 0,
                length: 1024,
                record_count: Some(10),
                data_file_path: format!("delete_{}.parquet", i),
                data_file_content: DataContentType::EqualityDeletes,
                data_file_format: DataFileFormat::Parquet,
                schema: get_test_schema(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                sequence_number: 1,
                equality_ids: vec![1, 2],
                file_size_in_bytes: 1024,
            }));
        }

        let test_files = vec![file_no_deletes, file_one_delete, file_two_deletes];

        // Execute strategy
        let result = TestUtils::execute_strategy_flat(&strategy, test_files);

        // Should only select file with >= 2 delete files
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data_file_path, "two_deletes.parquet");
        assert_eq!(result[0].deletes.len(), 2);
    }

    #[test]
    fn test_full_compaction_with_noop_grouping() {
        // Test that Full Compaction with Noop grouping creates a single group
        let full_config = crate::config::FullCompactionConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::Noop)
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::Full(full_config);

        let strategy = PlanStrategy::from(&config);

        let test_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(20 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file3.parquet")
                .size(30 * 1024 * 1024)
                .with_deletes()
                .build(),
            TestFileBuilder::new("file4.parquet")
                .size(15 * 1024 * 1024)
                .build(),
        ];

        let result = strategy.execute(test_files.clone(), &config).unwrap();

        // Full compaction should include ALL files, creating groups but not filtering any
        let total_files_in_groups: usize = result.iter().map(|g| g.data_file_count).sum();
        assert_eq!(
            total_files_in_groups, 4,
            "Full compaction should include all 4 files without any filtering"
        );

        // Verify files with deletes are included
        let has_files_with_deletes = result
            .iter()
            .flat_map(|g| &g.data_files)
            .any(|f| !f.deletes.is_empty());
        assert!(
            has_files_with_deletes,
            "Full compaction should include files with deletes"
        );

        // Verify Noop grouping creates a single group
        assert_eq!(
            result.len(),
            1,
            "Noop grouping should create a single group"
        );

        // Verify strategy description doesn't mention any filters
        let desc = strategy.to_string();
        assert!(
            !desc.contains("MinGroupSize") && !desc.contains("MinGroupFileCount"),
            "Full compaction strategy should not have group filters, got: {}",
            desc
        );
        assert!(
            !desc.contains("SizeFilter"),
            "Full compaction strategy should not have file filters, got: {}",
            desc
        );
    }
}
