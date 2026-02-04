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
//! 2. Grouping: Combine files using Single (all-in-one) or `BinPack` (First-Fit Decreasing)
//! 3. Group filters: Remove groups below size/count thresholds
//!
//! Parallelism is calculated per group based on file size and count constraints.

use std::collections::HashMap;

use iceberg::scan::FileScanTask;

use super::packer::ListPacker;
use crate::config::{CompactionPlanningConfig, GroupingStrategy};
use crate::{CompactionError, Result};

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
    /// Constructs a `FileGroup` from data files.
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
                match &delete_task.data_file_content {
                    iceberg::spec::DataContentType::PositionDeletes => {
                        position_delete_map
                            .entry(&delete_task.data_file_path)
                            .or_insert(delete_task);
                    }
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        equality_delete_map
                            .entry(&delete_task.data_file_path)
                            .or_insert(delete_task);
                    }
                    _ => {}
                }
            }
        }

        let position_delete_files = position_delete_map
            .into_values()
            .map(|file| {
                let mut file = file.as_ref().clone();
                file.project_field_ids = vec![];
                file
            })
            .collect::<Vec<FileScanTask>>();

        let equality_delete_files = equality_delete_map
            .into_values()
            .map(|file| {
                let mut file = file.as_ref().clone();
                file.project_field_ids = file.equality_ids.clone().unwrap_or_default();
                file
            })
            .collect::<Vec<FileScanTask>>();

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

    /// Creates a `FileGroup` with calculated parallelism.
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

    /// Returns an empty `FileGroup` with parallelism set to 1.
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
    Single(SingleGroupingStrategy),
    BinPack(BinPackGroupingStrategy),
}

impl GroupingStrategyEnum {
    pub fn group_files<I>(&self, data_files: I) -> Vec<FileGroup>
    where I: Iterator<Item = FileScanTask> {
        // Note that if the table is unpartitioned, the hash map will have a single entry.
        group_files_by_partition(data_files)
            .into_iter()
            .flat_map(|(_partition_key, files)| match self {
                GroupingStrategyEnum::Single(strategy) => strategy.group_files(files.into_iter()),
                GroupingStrategyEnum::BinPack(strategy) => strategy.group_files(files.into_iter()),
            })
            .collect()
    }
}

impl std::fmt::Display for GroupingStrategyEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupingStrategyEnum::Single(strategy) => write!(f, "{}", strategy),
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

/// Single grouping strategy. Groups all files into a single `FileGroup`.
///
/// Returns empty vec if input is empty.
#[derive(Debug)]
pub struct SingleGroupingStrategy;

impl SingleGroupingStrategy {
    pub fn group_files<I>(&self, data_files: I) -> Vec<FileGroup>
    where I: Iterator<Item = FileScanTask> {
        let files: Vec<FileScanTask> = data_files.collect();
        if files.is_empty() {
            vec![]
        } else {
            vec![FileGroup::new(files)]
        }
    }
}

impl std::fmt::Display for SingleGroupingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SingleGrouping")
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
    where I: Iterator<Item = FileScanTask> {
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
/// Filters by `task.length`.
///
/// - `min_size` is inclusive (`>=`)
/// - `max_size` is exclusive (`<`)
///
/// If both `None`, passes all files.
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
                    (Some(min), Some(max)) => file_size >= min && file_size < max,
                    (Some(min), None) => file_size >= min,
                    (None, Some(max)) => file_size < max,
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

/// Group filter by minimum total size.
///
/// Filters by `group.total_size >= min_group_size`.
#[derive(Debug)]
pub struct MinGroupSizeStrategy {
    pub min_group_size_bytes: u64,
}

impl GroupFilterStrategy for MinGroupSizeStrategy {
    fn filter_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup> {
        groups
            .into_iter()
            .filter(|group| group.total_size >= self.min_group_size_bytes)
            .collect()
    }
}

impl std::fmt::Display for MinGroupSizeStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MinGroupSize[{}MB]",
            self.min_group_size_bytes / 1024 / 1024
        )
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
    /// 1. Pre-group files by partition (if the a partition spec is present)
    /// 2. Apply each file filter sequentially
    /// 3. Group files using grouping strategy
    /// 4. Apply each group filter sequentially
    /// 5. Calculate parallelism for each group via [`FileGroup::with_calculated_parallelism`]
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

    /// Constructs grouping enum and group filters from config.
    ///
    /// # Arguments
    /// - `grouping_strategy`: Determines how files are partitioned into groups
    /// - `group_filters`: Optional filters to apply after grouping
    fn build_grouping_and_filters(
        grouping_strategy: &GroupingStrategy,
        group_filters: Option<&crate::config::GroupFilters>,
    ) -> (GroupingStrategyEnum, Vec<Box<dyn GroupFilterStrategy>>) {
        // Build grouping strategy enum
        let grouping = match grouping_strategy {
            GroupingStrategy::Single => GroupingStrategyEnum::Single(SingleGroupingStrategy),
            GroupingStrategy::BinPack(config) => GroupingStrategyEnum::BinPack(
                BinPackGroupingStrategy::new(config.target_group_size_bytes),
            ),
        };

        let mut group_filter_strategies: Vec<Box<dyn GroupFilterStrategy>> = vec![];

        // Apply group filters if provided
        if let Some(group_filters) = group_filters {
            // Add size filter if specified
            if let Some(min_group_size_bytes) =
                group_filters.min_group_size_bytes.filter(|&size| size > 0)
            {
                group_filter_strategies.push(Box::new(MinGroupSizeStrategy {
                    min_group_size_bytes,
                }));
            }

            // Add file count filter if specified
            if let Some(min_file_count) = group_filters
                .min_group_file_count
                .filter(|&count| count > 0)
            {
                group_filter_strategies
                    .push(Box::new(MinGroupFileCountStrategy { min_file_count }));
            }
        }

        (grouping, group_filter_strategies)
    }

    fn build(
        file_filters: Vec<Box<dyn FileFilterStrategy>>,
        grouping_strategy: &GroupingStrategy,
        group_filters: Option<&crate::config::GroupFilters>,
    ) -> Self {
        let (grouping, group_filters) =
            Self::build_grouping_and_filters(grouping_strategy, group_filters);
        Self::new(file_filters, grouping, group_filters)
    }

    /// Constructs strategy for small files compaction.
    ///
    /// Selects small files where `size < small_file_threshold_bytes`.
    pub fn from_small_files(config: &crate::config::SmallFilesConfig) -> Self {
        let file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![Box::new(SizeFilterStrategy {
            min_size: None,
            max_size: Some(config.small_file_threshold_bytes),
        })];

        Self::build(
            file_filters,
            &config.grouping_strategy,
            config.group_filters.as_ref(),
        )
    }

    /// Constructs strategy for maintenance compaction.
    ///
    /// Targets only undersized data files (`size < undersized_threshold_bytes`) and applies
    /// a minimum group input gating threshold via `min_rewrite_input_bytes`.
    pub fn from_maintenance(config: &crate::config::MaintenanceCompactionConfig) -> Self {
        let undersized_threshold_bytes = config
            .undersized_threshold_bytes
            .unwrap_or(config.target_file_size_bytes);
        let min_rewrite_input_bytes = config
            .min_rewrite_input_bytes
            .unwrap_or(config.target_file_size_bytes);

        let file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![Box::new(SizeFilterStrategy {
            min_size: None,
            max_size: Some(undersized_threshold_bytes),
        })];

        let group_filters = crate::config::GroupFilters {
            min_group_size_bytes: Some(min_rewrite_input_bytes),
            min_group_file_count: None,
        };

        Self::build(
            file_filters,
            &config.grouping_strategy,
            Some(&group_filters),
        )
    }

    /// Constructs strategy for full compaction.
    ///
    /// No file filters. No group filters (full compaction processes all groups).
    pub fn from_full(config: &crate::config::FullCompactionConfig) -> Self {
        // Full compaction never uses group filters.
        Self::build(vec![], &config.grouping_strategy, None)
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

        Self::build(
            file_filters,
            &config.grouping_strategy,
            config.group_filters.as_ref(),
        )
    }

    /// Test-only builder accepting raw filter parameters.
    ///
    /// # Arguments
    /// - `size_filter`: `(min_size, max_size)` for `SizeFilterStrategy`
    /// - `delete_file_count_filter`: Threshold for `DeleteFileCountFilterStrategy`
    /// - `grouping_strategy`: Grouping algorithm config
    /// - `group_filters`: Optional group-level filters
    #[cfg(test)]
    pub fn new_custom(
        size_filter: Option<(Option<u64>, Option<u64>)>,
        delete_file_count_filter: Option<usize>,
        grouping_strategy: GroupingStrategy,
        group_filters: Option<crate::config::GroupFilters>,
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

        Self::build(file_filters, &grouping_strategy, group_filters.as_ref())
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
            CompactionPlanningConfig::Maintenance(maintenance_config) => {
                PlanStrategy::from_maintenance(maintenance_config)
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

#[derive(Eq, Hash, PartialEq)]
struct PartitionKey(iceberg::spec::Struct);

/// Groups files by their partition key, which is based on the values of their
/// partition fields.
///
/// If the table is unpartitioned, all files will be grouped together. It's
/// possible for data files not have have a partition, such as in the case of a
/// table created without a partition, but then with one added later. The data
/// files in the initial version would not have a partition.
fn group_files_by_partition<I>(files: I) -> HashMap<PartitionKey, Vec<FileScanTask>>
where I: Iterator<Item = FileScanTask> {
    let mut partitioned_groups: HashMap<PartitionKey, Vec<FileScanTask>> = HashMap::new();
    for file in files {
        let partition_key = match &file.partition {
            Some(partition) => PartitionKey(partition.clone()),
            // All FileScanTask files will have a partition value, even if it's empty. None doesn't
            // seem to be a valid value (although that could change). For now, set it to the default
            // value for an unpartitioned file, which is an empty Struct.
            None => PartitionKey(iceberg::spec::Struct::empty()),
        };
        partitioned_groups
            .entry(partition_key)
            .or_default()
            .push(file);
    }

    partitioned_groups
}

#[cfg(test)]
mod tests {
    // Lazy static schema to avoid rebuilding it for every test
    use std::sync::{Arc, OnceLock};

    use super::*;
    use crate::config::{CompactionPlanningConfig, SmallFilesConfigBuilder};
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
        partition: Option<iceberg::spec::Struct>,
        schema: Option<Arc<iceberg::spec::Schema>>,
    }

    impl TestFileBuilder {
        pub fn new(path: &str) -> Self {
            Self {
                path: path.to_owned(),
                size: 10 * 1024 * 1024, // Default 10MB
                has_deletes: false,
                delete_types: vec![],
                partition: None,
                schema: None,
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

        pub fn with_partition(mut self, partition: iceberg::spec::Struct) -> Self {
            self.partition = Some(partition);
            self
        }

        pub fn with_schema(mut self, schema: Arc<iceberg::spec::Schema>) -> Self {
            self.schema = Some(schema);
            self
        }

        pub fn build(self) -> FileScanTask {
            use std::sync::Arc;

            use iceberg::spec::{DataContentType, DataFileFormat};

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
                            schema: self.schema.clone().unwrap_or(get_test_schema()),
                            project_field_ids: if delete_type == DataContentType::EqualityDeletes {
                                vec![1, 2]
                            } else {
                                vec![1]
                            },
                            predicate: None,
                            deletes: vec![],
                            sequence_number: 1,
                            equality_ids: if delete_type == DataContentType::EqualityDeletes {
                                Some(vec![1, 2])
                            } else {
                                None
                            },
                            file_size_in_bytes: 1024,
                            partition: None,
                            partition_spec: None,
                            name_mapping: None,
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
                schema: self.schema.unwrap_or(get_test_schema()),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes,
                sequence_number: 1,
                equality_ids: None,
                file_size_in_bytes: self.size,
                partition: self.partition,
                partition_spec: None,
                name_mapping: None,
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

        /// Create a test delete file with given path and type
        pub fn create_delete_file(
            path: String,
            content_type: iceberg::spec::DataContentType,
        ) -> Arc<FileScanTask> {
            use iceberg::spec::DataFileFormat;
            Arc::new(FileScanTask {
                start: 0,
                length: 1024,
                record_count: Some(10),
                data_file_path: path,
                data_file_content: content_type,
                data_file_format: DataFileFormat::Parquet,
                schema: get_test_schema(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                sequence_number: 1,
                equality_ids: Some(vec![1, 2]),
                file_size_in_bytes: 1024,
                partition: None,
                partition_spec: None,
                name_mapping: None,
            })
        }

        /// Add n delete files to a `FileScanTask`
        fn add_delete_files(mut task: FileScanTask, count: usize) -> FileScanTask {
            use iceberg::spec::DataContentType;
            for i in 0..count {
                task.deletes.push(Self::create_delete_file(
                    format!("delete_{}.parquet", i),
                    DataContentType::EqualityDeletes,
                ));
            }
            task
        }
    }

    #[test]
    fn test_size_filter_strategy() {
        // Table-driven test for various size filter configurations
        let test_cases = vec![
            // (min, max, description_expected)
            (
                Some(5 * 1024 * 1024),
                Some(50 * 1024 * 1024),
                "SizeFilter[5-50MB]",
            ),
            (Some(10 * 1024 * 1024), None, "SizeFilter[>10MB]"),
            (None, Some(50 * 1024 * 1024), "SizeFilter[<50MB]"),
            (None, None, "SizeFilter[Any]"),
            (
                Some(10 * 1024 * 1024),
                Some(10 * 1024 * 1024),
                "SizeFilter[10-10MB]",
            ), // min = max
            (
                Some(50 * 1024 * 1024),
                Some(10 * 1024 * 1024),
                "SizeFilter[50-10MB]",
            ), // min > max
        ];

        for (min_size, max_size, expected_desc) in test_cases {
            let strategy = SizeFilterStrategy { min_size, max_size };
            assert_eq!(strategy.to_string(), expected_desc);
        }

        // Test normal range filtering (5-50MB)
        let strategy = SizeFilterStrategy {
            min_size: Some(5 * 1024 * 1024),
            max_size: Some(50 * 1024 * 1024),
        };

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
        assert_eq!(result.len(), 3);
        TestUtils::assert_paths_eq(
            &["min_edge.parquet", "medium1.parquet", "medium2.parquet"],
            &result,
        );

        for file in &result {
            assert!(file.length >= 5 * 1024 * 1024 && file.length < 50 * 1024 * 1024);
        }

        // Test max-only (exclusive)
        let max_only_strategy = SizeFilterStrategy {
            min_size: None,
            max_size: Some(50 * 1024 * 1024),
        };
        let test_files = vec![
            TestFileBuilder::new("below.parquet")
                .size(49 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("at_max.parquet")
                .size(50 * 1024 * 1024)
                .build(),
        ];
        let result = max_only_strategy.filter(test_files);
        assert_eq!(result.len(), 1);
        TestUtils::assert_paths_eq(&["below.parquet"], &result);

        // Test min-only (inclusive)
        let min_only_strategy = SizeFilterStrategy {
            min_size: Some(50 * 1024 * 1024),
            max_size: None,
        };
        let test_files = vec![
            TestFileBuilder::new("below.parquet")
                .size(49 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("at_min.parquet")
                .size(50 * 1024 * 1024)
                .build(),
        ];
        let result = min_only_strategy.filter(test_files);
        assert_eq!(result.len(), 1);
        TestUtils::assert_paths_eq(&["at_min.parquet"], &result);

        // Test min = max (empty range)
        let exact_strategy = SizeFilterStrategy {
            min_size: Some(10 * 1024 * 1024),
            max_size: Some(10 * 1024 * 1024),
        };
        let test_files = vec![
            TestFileBuilder::new("too_small.parquet")
                .size(9 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("exact.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("too_large.parquet")
                .size(11 * 1024 * 1024)
                .build(),
        ];
        let result = exact_strategy.filter(test_files);
        assert_eq!(result.len(), 0);

        // Test min > max (invalid range - should return empty)
        let invalid_strategy = SizeFilterStrategy {
            min_size: Some(50 * 1024 * 1024),
            max_size: Some(10 * 1024 * 1024),
        };
        let test_files = vec![
            TestFileBuilder::new("any.parquet")
                .size(30 * 1024 * 1024)
                .build(),
        ];
        let result = invalid_strategy.filter(test_files);
        assert_eq!(result.len(), 0, "Invalid range should filter out all files");
    }

    #[test]
    fn test_file_strategy_factory() {
        let small_files_config =
            CompactionPlanningConfig::SmallFiles(crate::config::SmallFilesConfig::default());

        let small_files_strategy = PlanStrategy::from(&small_files_config);
        let small_files_desc = small_files_strategy.to_string();
        assert!(small_files_desc.contains("SizeFilter"));
        assert!(
            small_files_desc.contains("SingleGrouping")
                || small_files_desc.contains("BinPackGrouping")
        );

        let full_config =
            CompactionPlanningConfig::Full(crate::config::FullCompactionConfig::default());
        let routed_full = PlanStrategy::from(&full_config);

        let full_desc = routed_full.to_string();
        assert!(!full_desc.is_empty());
        assert!(full_desc.contains("NoFileFilters") || full_desc.contains("FileFilter"));

        // Verify different configs produce different strategies
        assert_ne!(
            small_files_desc, full_desc,
            "Different configs should produce different strategy descriptions"
        );

        // Verify FilesWithDeletes config
        let deletes_config = CompactionPlanningConfig::FilesWithDeletes(
            crate::config::FilesWithDeletesConfig::default(),
        );
        let deletes_strategy = PlanStrategy::from(&deletes_config);
        let deletes_desc = deletes_strategy.to_string();

        // Should have different description than the other two
        assert_ne!(deletes_desc, small_files_desc);
        assert_ne!(deletes_desc, full_desc);
    }

    #[test]
    fn test_binpack_grouping_size_limit() {
        use crate::config::BinPackConfig;

        let strategy = PlanStrategy::new_custom(
            None,
            None,
            GroupingStrategy::BinPack(BinPackConfig::new(25 * 1024 * 1024)),
            None, // no group filters
        );

        let test_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file3.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file4.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file5.parquet")
                .size(5 * 1024 * 1024)
                .build(),
        ];

        let config = TestUtils::create_test_config();

        let groups = strategy.execute(test_files, &config).unwrap();

        assert_eq!(groups.len(), 2);
        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 5);

        for (i, group) in groups.iter().enumerate() {
            assert!(
                group.total_size <= 30 * 1024 * 1024,
                "Group {} exceeds margin",
                i
            );
        }

        let large_files = vec![
            TestFileBuilder::new("huge1.parquet")
                .size(30 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("huge2.parquet")
                .size(30 * 1024 * 1024)
                .build(),
        ];

        let large_groups = strategy.execute(large_files, &config).unwrap();
        assert_eq!(large_groups.len(), 2);
        assert_eq!(large_groups[0].data_file_count, 1);
        assert_eq!(large_groups[1].data_file_count, 1);

        let group_sizes: Vec<u64> = large_groups.iter().map(|g| g.total_size).collect();
        assert!(group_sizes.iter().all(|&size| size == 30 * 1024 * 1024));
    }

    #[test]
    fn test_min_group_file_count_with_single() {
        assert_eq!(
            MinGroupFileCountStrategy { min_file_count: 3 }.to_string(),
            "MinGroupFileCount[3]"
        );
        assert_eq!(
            MinGroupFileCountStrategy { min_file_count: 0 }.to_string(),
            "MinGroupFileCount[0]"
        );

        let test_cases = vec![
            (3, 3, 1), // min=3, input=3 files, expect 1 group
            (3, 5, 1), // min=3, input=5 files, expect 1 group
            (3, 2, 0), // min=3, input=2 files, expect 0 groups (filtered out)
            (0, 1, 1), // min=0, input=1 file, expect 1 group
            (1, 1, 1), // min=1, input=1 file, expect 1 group
        ];

        for (min_count, input_count, expected_groups) in test_cases {
            // Use SmallFilesConfig with Single grouping + group filters
            let small_files_config = SmallFilesConfigBuilder::default()
                .grouping_strategy(GroupingStrategy::Single)
                .group_filters(crate::config::GroupFilters {
                    min_group_file_count: Some(min_count),
                    min_group_size_bytes: None,
                })
                .build()
                .unwrap();
            let strategy = PlanStrategy::from_small_files(&small_files_config);

            let files: Vec<FileScanTask> = (0..input_count)
                .map(|i| {
                    TestFileBuilder::new(&format!("file{}.parquet", i))
                        .size((10 + i as u64 * 5) * 1024 * 1024)
                        .build()
                })
                .collect();

            let result = strategy
                .execute(
                    files,
                    &CompactionPlanningConfig::SmallFiles(small_files_config.clone()),
                )
                .unwrap();
            assert_eq!(
                result.len(),
                expected_groups,
                "min_count={}, input={} files should yield {} groups",
                min_count,
                input_count,
                expected_groups
            );

            if expected_groups > 0 {
                assert_eq!(result[0].data_file_count, input_count);
            }
        }
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
            // Upper bound is exclusive (`size < threshold`), so exact-threshold file is excluded.
            TestFileBuilder::new("exact_threshold.parquet")
                .size(20 * 1024 * 1024)
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
        assert_eq!(paths, vec![
            "small1.parquet",
            "small2.parquet",
            "small3.parquet"
        ]);

        let small_file_threshold = match config {
            CompactionPlanningConfig::SmallFiles(sf_config) => sf_config.small_file_threshold_bytes,
            _ => panic!("Expected small files config"),
        };
        for file in &result {
            assert!(file.length < small_file_threshold);
        }

        let min_count_small_files_config = SmallFilesConfigBuilder::default()
            .small_file_threshold_bytes(20 * 1024 * 1024_u64)
            .grouping_strategy(GroupingStrategy::Single)
            .group_filters(crate::config::GroupFilters {
                min_group_file_count: Some(3),
                min_group_size_bytes: None,
            })
            .build()
            .unwrap();
        let min_count_config = CompactionPlanningConfig::SmallFiles(min_count_small_files_config);
        let min_count_strategy = PlanStrategy::from(&min_count_config);

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

        let default_small_files_config = SmallFilesConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::Single)
            .build()
            .unwrap();
        let default_config = CompactionPlanningConfig::SmallFiles(default_small_files_config);

        match &default_config {
            CompactionPlanningConfig::SmallFiles(config) => {
                // Default config should have Single grouping strategy with no group filters
                assert!(matches!(
                    config.grouping_strategy,
                    crate::config::GroupingStrategy::Single
                ));
                assert!(config.group_filters.is_none());
            }
            _ => panic!("Expected small files config"),
        }

        let default_strategy = PlanStrategy::from(&default_config);

        let single_file = vec![
            TestFileBuilder::new("single.parquet")
                .size(5 * 1024 * 1024)
                .build(),
        ];
        let single_result = TestUtils::execute_strategy_flat(&default_strategy, single_file);
        assert_eq!(single_result.len(), 1);
        TestUtils::assert_paths_eq(&["single.parquet"], &single_result);

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
            assert!(file.length < small_file_threshold_default);
            assert!(file.deletes.is_empty());
        }
    }

    #[test]
    fn test_group_filter_strategies_with_file_groups() {
        let groups = vec![
            FileGroup::new(vec![
                TestFileBuilder::new("small1.parquet")
                    .size(5 * 1024 * 1024)
                    .build(),
                TestFileBuilder::new("small2.parquet")
                    .size(10 * 1024 * 1024)
                    .build(),
            ]), // 15MB, 2 files
            FileGroup::new(vec![
                TestFileBuilder::new("large1.parquet")
                    .size(50 * 1024 * 1024)
                    .build(),
                TestFileBuilder::new("large2.parquet")
                    .size(100 * 1024 * 1024)
                    .build(),
                TestFileBuilder::new("large3.parquet")
                    .size(75 * 1024 * 1024)
                    .build(),
            ]), // 225MB, 3 files
            FileGroup::new(vec![
                TestFileBuilder::new("single.parquet")
                    .size(20 * 1024 * 1024)
                    .build(),
            ]), // 20MB, 1 file
        ];

        let test_cases = vec![
            (
                "MinGroupSize[100MB]",
                MinGroupSizeStrategy {
                    min_group_size_bytes: 100 * 1024 * 1024,
                }
                .to_string(),
            ),
            (
                "MinGroupFileCount[2]",
                MinGroupFileCountStrategy { min_file_count: 2 }.to_string(),
            ),
        ];

        for (expected_desc, actual_desc) in test_cases {
            assert_eq!(actual_desc, expected_desc);
        }

        let min_size_strategy = MinGroupSizeStrategy {
            min_group_size_bytes: 100 * 1024 * 1024,
        };
        assert_eq!(min_size_strategy.filter_groups(groups.clone()).len(), 1);

        let min_file_count_strategy = MinGroupFileCountStrategy { min_file_count: 2 };
        assert_eq!(
            min_file_count_strategy.filter_groups(groups.clone()).len(),
            2
        );
    }

    #[test]
    fn test_create_custom_strategy_comprehensive() {
        use crate::config::GroupingStrategy;

        // Test create_custom_strategy with basic parameter combinations

        // Test case 1: With size filter
        let strategy_with_size_filter = PlanStrategy::new_custom(
            Some((Some(1024 * 1024), Some(100 * 1024 * 1024))), // size_filter: 1MB-100MB
            None,                                               // no delete file count filter
            GroupingStrategy::Single,
            None, // no group filters
        );

        let description = strategy_with_size_filter.to_string();
        assert!(description.contains("SizeFilter"));

        // Test case 2: Minimal filters
        let strategy_minimal = PlanStrategy::new_custom(
            None,
            None,
            GroupingStrategy::Single,
            None, // no group filters
        );

        // Test functional behavior
        let test_files = vec![
            TestFileBuilder::new("small.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("medium.parquet")
                .size(50 * 1024 * 1024)
                .build(),
            // Upper bound is exclusive (`size < max`), so exact-max file is excluded.
            TestFileBuilder::new("exact_max.parquet")
                .size(100 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("large.parquet")
                .size(200 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("with_deletes.parquet")
                .size(30 * 1024 * 1024)
                .with_deletes()
                .build(),
        ];

        // Strategy with size filter should exclude large files (>= 100MB, max is exclusive)
        let result_filtered =
            TestUtils::execute_strategy_flat(&strategy_with_size_filter, test_files.clone());
        assert_eq!(result_filtered.len(), 3); // small, medium, and with_deletes (all under 100MB)

        // Minimal strategy should pass all files
        let result_minimal =
            TestUtils::execute_strategy_flat(&strategy_minimal, test_files.clone());
        assert_eq!(result_minimal.len(), 5);
    }

    #[test]
    fn test_binpack_grouping_comprehensive() {
        // Test Case 1: Normal target size behavior
        let normal_strategy = BinPackGroupingStrategy::new(20 * 1024 * 1024);

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
        ];

        let groups = normal_strategy.group_files(small_files.into_iter());
        assert_eq!(groups.len(), 2);
        let mut counts: Vec<usize> = groups.iter().map(|g| g.data_file_count).collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![1, 2]);

        let total_size: u64 = groups.iter().map(|g| g.total_size).sum();
        assert_eq!(total_size, 30 * 1024 * 1024);

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
        let zero_groups = zero_strategy.group_files(test_files.into_iter());
        assert_eq!(zero_groups.len(), 1);
        assert_eq!(zero_groups[0].data_file_count, 2);

        // Test Case 3: Empty input
        let empty_files: Vec<FileScanTask> = vec![];
        let empty_groups = normal_strategy.group_files(empty_files.into_iter());
        assert_eq!(
            empty_groups.len(),
            0,
            "Empty input should produce no groups"
        );

        // Test Case 4: Large target group size - all files fit in one group
        let large_target_strategy = BinPackGroupingStrategy::new(1024 * 1024 * 1024);
        let many_files = vec![
            TestFileBuilder::new("f1.parquet").size(1024 * 1024).build(),
            TestFileBuilder::new("f2.parquet").size(1024 * 1024).build(),
            TestFileBuilder::new("f3.parquet").size(1024 * 1024).build(),
            TestFileBuilder::new("f4.parquet").size(1024 * 1024).build(),
            TestFileBuilder::new("f5.parquet").size(1024 * 1024).build(),
        ];
        let groups = large_target_strategy.group_files(many_files.into_iter());
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].data_file_count, 5);

        assert_eq!(normal_strategy.to_string(), "BinPackGrouping[target=20MB]");

        // Test Case 5: Large files produce multiple groups
        use crate::config::BinPackConfig;
        let bin_pack_strategy = PlanStrategy::new_custom(
            None,
            None,
            GroupingStrategy::BinPack(BinPackConfig::new(64 * 1024 * 1024)),
            None, // no group filters
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
        ];

        let large_groups = bin_pack_strategy.execute(large_files, &config).unwrap();
        assert_eq!(
            large_groups.len(),
            4,
            "Each 50MB file should be in its own group with 64MB target"
        );
        let total_files: usize = large_groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 4);

        for group in &large_groups {
            assert_eq!(group.data_file_count, 1);
            assert_eq!(group.total_size, 50 * 1024 * 1024);
        }
    }

    #[test]
    fn test_grouping_strategy_enum() {
        let data_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(5 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(10 * 1024 * 1024)
                .build(),
        ];

        // Test Single variant
        let single_enum = GroupingStrategyEnum::Single(SingleGroupingStrategy);
        let groups = single_enum.group_files(data_files.clone().into_iter());
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].data_file_count, 2);
        assert_eq!(single_enum.to_string(), "SingleGrouping");

        // Single-file case for Single
        let single = vec![
            TestFileBuilder::new("single.parquet")
                .size(20 * 1024 * 1024)
                .build(),
        ];
        let single_file_groups = single_enum.group_files(single.into_iter());
        assert_eq!(single_file_groups.len(), 1);
        assert_eq!(single_file_groups[0].data_file_count, 1);
        TestUtils::assert_paths_eq(&["single.parquet"], &single_file_groups[0].data_files);

        // Empty input
        let empty_files: Vec<FileScanTask> = vec![];
        let single_empty_groups = single_enum.group_files(empty_files.into_iter());
        assert_eq!(single_empty_groups.len(), 0);

        // Test BinPack variant
        let binpack_enum =
            GroupingStrategyEnum::BinPack(BinPackGroupingStrategy::new(20 * 1024 * 1024));
        let binpack_groups = binpack_enum.group_files(data_files.into_iter());
        assert!(!binpack_groups.is_empty());
        assert_eq!(binpack_enum.to_string(), "BinPackGrouping[target=20MB]");
    }

    #[test]
    fn test_maintenance_skips_underfilled_groups() {
        let target = 100 * 1024 * 1024_u64;
        let maintenance_config = crate::config::MaintenanceCompactionConfigBuilder::default()
            .target_file_size_bytes(target)
            .grouping_strategy(GroupingStrategy::Single)
            .min_rewrite_input_bytes(target)
            .build()
            .unwrap();

        let strategy = PlanStrategy::from_maintenance(&maintenance_config);

        let files = vec![
            TestFileBuilder::new("small1.parquet")
                .size(30 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("small2.parquet")
                .size(20 * 1024 * 1024)
                .build(),
        ];

        let result = strategy
            .execute(
                files,
                &CompactionPlanningConfig::Maintenance(maintenance_config),
            )
            .unwrap();

        assert!(
            result.is_empty(),
            "Underfilled maintenance group should be skipped"
        );
    }

    #[test]
    fn test_maintenance_selects_only_undersized_exclusive() {
        let target = 100 * 1024 * 1024_u64;
        let maintenance_config = crate::config::MaintenanceCompactionConfigBuilder::default()
            .target_file_size_bytes(target)
            .grouping_strategy(GroupingStrategy::Single)
            .min_rewrite_input_bytes(1_u64)
            .build()
            .unwrap();

        let strategy = PlanStrategy::from_maintenance(&maintenance_config);

        let files = vec![
            TestFileBuilder::new("undersized.parquet")
                .size(40 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("exact_target.parquet")
                .size(target)
                .build(),
        ];

        let result = strategy
            .execute(
                files,
                &CompactionPlanningConfig::Maintenance(maintenance_config),
            )
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data_file_count, 1);
        TestUtils::assert_paths_eq(&["undersized.parquet"], &result[0].data_files);
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

        // Verify error message is specific
        match result {
            Err(CompactionError::Execution(msg)) => {
                assert!(msg.contains("No files to calculate task parallelism"));
            }
            _ => panic!("Expected CompactionError::Execution"),
        }
    }

    #[test]
    fn test_file_group_parallelism_immutability() {
        // Verify that with_calculated_parallelism doesn't mutate original
        let files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(15 * 1024 * 1024)
                .build(),
        ];

        let original_group = FileGroup::new(files);
        let original_exec_p = original_group.executor_parallelism;
        let original_output_p = original_group.output_parallelism;

        let small_files_config = SmallFilesConfigBuilder::default()
            .min_size_per_partition(10 * 1024 * 1024_u64)
            .max_file_count_per_partition(5_usize)
            .max_parallelism(8_usize)
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::SmallFiles(small_files_config);

        let new_group = original_group
            .clone()
            .with_calculated_parallelism(&config)
            .unwrap();

        // Original should be unchanged
        assert_eq!(original_group.executor_parallelism, original_exec_p);
        assert_eq!(original_group.output_parallelism, original_output_p);

        // New group should have calculated values
        assert!(new_group.executor_parallelism >= 1);
        assert!(new_group.output_parallelism >= 1);
    }

    #[test]
    fn test_file_group_delete_files_extraction() {
        // Test that FileGroup correctly extracts and organizes delete files
        use std::sync::Arc;

        use iceberg::spec::{DataContentType, DataFileFormat};

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
            equality_ids: None,
            file_size_in_bytes: 1024,
            partition: None,
            partition_spec: None,
            name_mapping: None,
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
            equality_ids: Some(vec![1, 2]),
            file_size_in_bytes: 2048,
            partition: None,
            partition_spec: None,
            name_mapping: None,
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
            equality_ids: None,
            file_size_in_bytes: 10 * 1024 * 1024,
            partition: None,
            partition_spec: None,
            name_mapping: None,
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
        use std::sync::Arc;

        use iceberg::spec::{DataContentType, DataFileFormat};

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
            equality_ids: None,
            file_size_in_bytes: 512,
            partition: None,
            partition_spec: None,
            name_mapping: None,
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
            equality_ids: None,
            file_size_in_bytes: 4 * 1024 * 1024,
            partition: None,
            partition_spec: None,
            name_mapping: None,
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
            equality_ids: None,
            file_size_in_bytes: 4 * 1024 * 1024,
            partition: None,
            partition_spec: None,
            name_mapping: None,
        };

        let group = FileGroup::new(vec![f1, f2]);
        // Dedup should keep one position delete
        assert_eq!(group.position_delete_files.len(), 1);
        assert_eq!(
            group.position_delete_files[0].data_file_path,
            "shared_pos_delete.parquet"
        );

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
    fn test_file_group_delete_files_dedup_mixed_types() {
        // Test deduplication of equality deletes and mixed delete types
        use std::sync::Arc;

        use iceberg::spec::{DataContentType, DataFileFormat};

        let shared_eq_delete = Arc::new(FileScanTask {
            start: 0,
            length: 1024,
            record_count: Some(5),
            data_file_path: "shared_eq_delete.parquet".to_owned(),
            data_file_content: DataContentType::EqualityDeletes,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![],
            sequence_number: 1,
            equality_ids: Some(vec![1, 2]),
            file_size_in_bytes: 1024,
            partition: None,
            partition_spec: None,
            name_mapping: None,
        });

        let pos_delete = Arc::new(FileScanTask {
            start: 0,
            length: 512,
            record_count: Some(3),
            data_file_path: "pos_delete.parquet".to_owned(),
            data_file_content: DataContentType::PositionDeletes,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            sequence_number: 1,
            equality_ids: None,
            file_size_in_bytes: 512,
            partition: None,
            partition_spec: None,
            name_mapping: None,
        });

        let f1 = FileScanTask {
            start: 0,
            length: 5 * 1024 * 1024,
            record_count: Some(100),
            data_file_path: "d1.parquet".to_owned(),
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![shared_eq_delete.clone(), pos_delete.clone()],
            sequence_number: 1,
            equality_ids: None,
            file_size_in_bytes: 5 * 1024 * 1024,
            partition: None,
            partition_spec: None,
            name_mapping: None,
        };

        let f2 = FileScanTask {
            start: 0,
            length: 5 * 1024 * 1024,
            record_count: Some(100),
            data_file_path: "d2.parquet".to_owned(),
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![shared_eq_delete, pos_delete],
            sequence_number: 1,
            equality_ids: None,
            file_size_in_bytes: 5 * 1024 * 1024,
            partition: None,
            partition_spec: None,
            name_mapping: None,
        };

        let group = FileGroup::new(vec![f1, f2]);

        // Both delete types should be deduplicated
        assert_eq!(group.position_delete_files.len(), 1);
        assert_eq!(group.equality_delete_files.len(), 1);

        // Verify correct paths
        assert_eq!(
            group.position_delete_files[0].data_file_path,
            "pos_delete.parquet"
        );
        assert_eq!(
            group.equality_delete_files[0].data_file_path,
            "shared_eq_delete.parquet"
        );

        // Verify total counts
        assert_eq!(group.data_file_count, 2);
        assert_eq!(group.input_files_count(), 4); // 2 data + 1 pos + 1 eq
    }

    #[test]
    fn test_full_compaction_with_binpack_grouping() {
        let binpack_config = crate::config::BinPackConfig::new(50 * 1024 * 1024);
        let full_config = crate::config::FullCompactionConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::BinPack(binpack_config))
            .min_size_per_partition(20 * 1024 * 1024_u64)
            .max_file_count_per_partition(1_usize)
            .max_parallelism(8_usize)
            .enable_heuristic_output_parallelism(true)
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::Full(full_config);

        let strategy = PlanStrategy::from(&config);

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

        assert_eq!(
            groups.len(),
            3,
            "BinPack with lookback=1 should create 3 groups"
        );

        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 4, "Full compaction should include all files");

        let total_size: u64 = groups.iter().map(|g| g.total_size).sum();
        assert_eq!(
            total_size,
            100 * 1024 * 1024,
            "Total size should be preserved"
        );

        for (idx, group) in groups.iter().enumerate() {
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
            assert!(
                group.executor_parallelism <= config.max_parallelism(),
                "Group {} should respect max_parallelism",
                idx
            );
            assert!(
                group.output_parallelism <= group.executor_parallelism,
                "Group {} output_parallelism should not exceed executor_parallelism",
                idx
            );
        }

        let parallelisms: Vec<_> = groups
            .iter()
            .map(|g| (g.executor_parallelism, g.output_parallelism))
            .collect();
        assert!(!parallelisms.is_empty());

        for (idx, group) in groups.iter().enumerate() {
            let partition_by_size = (group
                .input_total_bytes()
                .div_ceil(config.min_size_per_partition()))
            .max(1) as usize;
            let partition_by_count = group
                .input_files_count()
                .div_ceil(config.max_file_count_per_partition())
                .max(1);
            let expected_exec_p = partition_by_size
                .max(partition_by_count)
                .min(config.max_parallelism());

            assert_eq!(
                group.executor_parallelism, expected_exec_p,
                "Group {}: parallelism mismatch",
                idx
            );
            assert!(group.output_parallelism <= group.executor_parallelism);
        }

        let has_parallelism = parallelisms.iter().any(|(exec_p, _)| *exec_p > 1);
        assert!(
            has_parallelism,
            "At least one group should have executor_parallelism > 1"
        );

        assert!(strategy.to_string().contains("BinPack"));
    }

    #[test]
    fn test_delete_file_count_filter_strategy() {
        let strategy = DeleteFileCountFilterStrategy::new(3);
        assert_eq!(strategy.to_string(), "DeleteFileCountFilter[>=3 deletes]");

        let test_files = vec![
            TestFileBuilder::new("no_deletes.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("one_delete.parquet")
                .size(10 * 1024 * 1024)
                .with_deletes()
                .build(),
            TestUtils::add_delete_files(
                TestFileBuilder::new("three_deletes.parquet")
                    .size(10 * 1024 * 1024)
                    .build(),
                3,
            ),
            TestUtils::add_delete_files(
                TestFileBuilder::new("five_deletes.parquet")
                    .size(10 * 1024 * 1024)
                    .build(),
                5,
            ),
        ];

        let result = strategy.filter(test_files);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "three_deletes.parquet");
        assert_eq!(result[1].data_file_path, "five_deletes.parquet");
        assert_eq!(result[0].deletes.len(), 3);
        assert_eq!(result[1].deletes.len(), 5);

        // Edge cases: threshold = 0 and threshold = 1
        let test_cases = vec![
            (0, 2), // threshold 0 passes all files
            (1, 1), // threshold 1 passes files with 1+ deletes
        ];

        for (threshold, expected_count) in test_cases {
            let strategy = DeleteFileCountFilterStrategy::new(threshold);
            let test_files = vec![
                TestFileBuilder::new("no_deletes.parquet")
                    .size(10 * 1024 * 1024)
                    .build(),
                TestFileBuilder::new("with_deletes.parquet")
                    .size(10 * 1024 * 1024)
                    .with_deletes()
                    .build(),
            ];
            let result = strategy.filter(test_files);
            assert_eq!(result.len(), expected_count);
        }
    }

    #[test]
    fn test_files_with_deletes_strategy() {
        use crate::config::FilesWithDeletesConfigBuilder;

        let files_with_deletes_config = FilesWithDeletesConfigBuilder::default()
            .min_delete_file_count_threshold(2_usize)
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::FilesWithDeletes(files_with_deletes_config);
        let strategy = PlanStrategy::from(&config);

        assert!(strategy.to_string().contains("DeleteFileCountFilter"));

        let test_files = vec![
            TestFileBuilder::new("no_deletes.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestUtils::add_delete_files(
                TestFileBuilder::new("one_delete.parquet")
                    .size(10 * 1024 * 1024)
                    .build(),
                1,
            ),
            TestUtils::add_delete_files(
                TestFileBuilder::new("two_deletes.parquet")
                    .size(10 * 1024 * 1024)
                    .build(),
                2,
            ),
        ];

        let result = TestUtils::execute_strategy_flat(&strategy, test_files);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data_file_path, "two_deletes.parquet");
        assert_eq!(result[0].deletes.len(), 2);
    }

    #[test]
    fn test_full_compaction_with_single_grouping() {
        // Test that Full Compaction with Single grouping creates a single group
        let full_config = crate::config::FullCompactionConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::Single)
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

        assert_eq!(result.len(), 1,);

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

    #[test]
    fn test_parallelism_calculation_overflow_safety() {
        // Test that extremely large file sizes don't cause overflow in div_ceil
        let small_files_config = SmallFilesConfigBuilder::default()
            .min_size_per_partition(1_u64)
            .max_file_count_per_partition(1_usize)
            .max_parallelism(1000_usize)
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::SmallFiles(small_files_config);

        // Create a group with very large file size (near u64::MAX / 2)
        let huge_file = TestFileBuilder::new("huge.parquet")
            .size(u64::MAX / 2)
            .build();

        let group = FileGroup::new(vec![huge_file]);
        let result = FileGroup::calculate_parallelism(&group, &config);

        // Should not panic or overflow, should calculate valid parallelism
        assert!(result.is_ok());
        let (exec_p, out_p) = result.unwrap();
        assert!(exec_p >= 1);
        assert!(out_p >= 1);
        assert!(exec_p <= config.max_parallelism());
    }

    // ##############################################################
    // Tests for Grouping Partitioned Files
    // ##############################################################

    // NOTE: The unit tests above already cover the case of Tables which do not have a partition
    // and thus should not be pre-grouped by partition before applying filters.

    static TEST_SCHEMA_WITH_PARTITION: OnceLock<Arc<iceberg::spec::Schema>> = OnceLock::new();

    fn get_test_schema_with_partition() -> Arc<iceberg::spec::Schema> {
        TEST_SCHEMA_WITH_PARTITION
            .get_or_init(|| {
                Arc::new(
                    iceberg::spec::Schema::builder()
                        .with_fields(vec![Arc::new(iceberg::spec::NestedField::new(
                            1,
                            "num",
                            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                            true,
                        ))])
                        .build()
                        .unwrap(),
                )
            })
            .clone()
    }

    /// Create a bucket partition value
    fn create_partition_value(bucket_value: i32) -> iceberg::spec::Struct {
        let fields: Vec<Option<iceberg::spec::Literal>> = vec![Some(
            iceberg::spec::Literal::Primitive(iceberg::spec::PrimitiveLiteral::Int(bucket_value)),
        )];
        iceberg::spec::Struct::from_iter(fields)
    }

    #[test]
    fn test_single_strategy_groups_by_partition() {
        // Validates that the Single group strategy (without filters) groups files by partition.

        // 1. Create files in 5 partitions with varying counts.
        let build_file = |name: &str, partition_num: i32| {
            TestFileBuilder::new(name)
                .size(1024 * 1024) // size doesn't matter for this test
                .with_partition(create_partition_value(partition_num))
                .with_schema(get_test_schema_with_partition())
                .build()
        };
        let files = vec![
            // Partition 0
            build_file("p0_file1.parquet", 0),
            build_file("p0_file2.parquet", 0),
            build_file("p0_file3.parquet", 0),
            // Partition 1
            build_file("p1_file1.parquet", 1),
            // Partition 2
            build_file("p2_file1.parquet", 2),
            build_file("p2_file2.parquet", 2),
            build_file("p2_file3.parquet", 2),
            build_file("p2_file4.parquet", 2),
            // Partition 3
            build_file("p3_file1.parquet", 3),
            build_file("p3_file2.parquet", 3),
            // Partition 4
            build_file("p4_file1.parquet", 4),
        ];

        // 2. Create strategy with Single grouping, and no group filters.
        let strategy = PlanStrategy::new_custom(
            None, // no file filters
            None, // no delete file count filter
            GroupingStrategy::Single,
            None, // no group filters
        );
        let config = CompactionPlanningConfig::default();

        // 3. Execute strategy
        let groups = strategy.execute(files, &config).unwrap();

        // 4. Assertions
        assert_eq!(
            groups.len(),
            5,
            "Should have 5 groups (one per partition), not 1 combined group"
        );
    }

    #[test]
    fn test_single_group_groups_and_applies_min_file_count_filter_to_partitions() {
        // Validates that min_group_file_count filter applies to groups created within each partition.

        // 1. Create files in 5 partitions with varying counts.
        let build_file = |name: &str, partition_num: i32| {
            TestFileBuilder::new(name)
                .size(1024 * 1024) // size doesn't matter for this test
                .with_partition(create_partition_value(partition_num))
                .with_schema(get_test_schema_with_partition())
                .build()
        };
        let files = vec![
            // Partition 0
            build_file("p0_file1.parquet", 0),
            build_file("p0_file2.parquet", 0),
            build_file("p0_file3.parquet", 0),
            // Partition 1
            build_file("p1_file1.parquet", 1),
            // Partition 2
            build_file("p2_file1.parquet", 2),
            build_file("p2_file2.parquet", 2),
            build_file("p2_file3.parquet", 2),
            build_file("p2_file4.parquet", 2),
            // Partition 3
            build_file("p3_file1.parquet", 3),
            build_file("p3_file2.parquet", 3),
            // Partition 4
            build_file("p4_file1.parquet", 4),
        ];

        // 2. Create strategy with Single grouping + min_group_file_count filter
        let small_files_config = SmallFilesConfigBuilder::default()
            .grouping_strategy(GroupingStrategy::Single)
            .group_filters(crate::config::GroupFilters {
                min_group_file_count: Some(2),
                min_group_size_bytes: None,
            })
            .build()
            .unwrap();
        let config = CompactionPlanningConfig::SmallFiles(small_files_config.clone());
        let strategy = PlanStrategy::from_small_files(&small_files_config);

        // 3. Execute strategy
        let groups = strategy.execute(files, &config).unwrap();

        // 4. Assertions
        assert_eq!(groups.len(), 3, "Should have 3 groups (partitions 0, 2, 3)");

        // Collect file counts per group
        let mut file_counts: Vec<usize> = groups.iter().map(|g| g.data_file_count).collect();
        file_counts.sort();

        assert_eq!(
            file_counts,
            vec![2, 3, 4],
            "File counts should be [2, 3, 4] for partitions 3, 0, 2 respectively"
        );

        // Verify total files in output
        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(
            total_files, 9,
            "Should have 9 total files (partitions 1 and 4 filtered out)"
        );
    }

    #[test]
    fn test_binpack_groups_by_partition_does_not_mix_across_partitions() {
        // Validates that BinPack creates separate groups per partition and doesn't
        // mix files from different partitions, even when it could optimize packing.

        // 1. Create files in 3 partitions, 2 files each
        let build_file = |name: &str, partition_num: i32, size_mb: u64| {
            TestFileBuilder::new(name)
                .size(size_mb * 1024 * 1024)
                .with_partition(create_partition_value(partition_num))
                .with_schema(get_test_schema_with_partition())
                .build()
        };

        let files = vec![
            // Partition 0: 2 files @ 10MB each = 20MB total
            build_file("p0_file1.parquet", 0, 10),
            build_file("p0_file2.parquet", 0, 20),
            // Partition 1: 2 files @ 10MB each = 20MB total
            build_file("p1_file1.parquet", 1, 10),
            build_file("p1_file2.parquet", 1, 20),
            // Partition 2: 2 files @ 10MB each = 20MB total
            build_file("p2_file1.parquet", 2, 10),
            build_file("p2_file2.parquet", 2, 20),
        ];

        // 2. Create strategy with BinPack grouping, and no group filters.
        // Target size = 50MB. Without partitions, it would optimally pack into 50MB groups.
        let strategy = PlanStrategy::new_custom(
            None, // no file filters
            None, // no delete file count filter
            GroupingStrategy::BinPack(crate::config::BinPackConfig::new(50 * 1024 * 1024)),
            None, // no group filters
        );

        // 3. Execute strategy
        let config = CompactionPlanningConfig::default();
        let groups = strategy.execute(files, &config).unwrap();

        // 4. Assertions
        assert_eq!(
            groups.len(),
            3,
            "Should have 3 groups (one per partition), not 1 combined group"
        );

        // Verify the groups
        for group in &groups {
            assert_eq!(
                group.data_file_count, 2,
                "Each partition should have 2 files grouped together"
            );
            assert_eq!(
                group.total_size,
                30 * 1024 * 1024,
                "Each group should be 30MB"
            );
        }

        // Verify total files
        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 6, "Should have all 6 files in output");
    }

    #[test]
    fn test_binpack_optimizes_files_within_partition() {
        // Validates that BinPacking will optimize file groups by size within a partition.

        // 1. Create files in 3 partitions, 2 files each
        let build_file = |name: &str, partition_num: i32, size_mb: u64| {
            TestFileBuilder::new(name)
                .size(size_mb * 1024 * 1024)
                .with_partition(create_partition_value(partition_num))
                .with_schema(get_test_schema_with_partition())
                .build()
        };

        let files = vec![
            // Partition 0: BinPack to [50MB, 5MB]
            build_file("p0_file1.parquet", 0, 10),
            build_file("p0_file2.parquet", 0, 20),
            build_file("p0_file3.parquet", 0, 5),
            build_file("p0_file4.parquet", 0, 5),
            build_file("p0_file5.parquet", 0, 15),
            // Partition 1: BinPack to [40MB, 35MB]
            build_file("p1_file1.parquet", 1, 10),
            build_file("p1_file2.parquet", 1, 20),
            build_file("p1_file3.parquet", 1, 15),
            build_file("p1_file4.parquet", 1, 5),
            build_file("p1_file5.parquet", 1, 20),
            build_file("p1_file6.parquet", 1, 5),
        ];

        // 2. Create strategy with BinPack grouping, and no group filters. Target size = 50MB.
        let strategy = PlanStrategy::new_custom(
            None, // no file filters
            None, // no delete file count filter
            GroupingStrategy::BinPack(crate::config::BinPackConfig::new(50 * 1024 * 1024)),
            None, // no group filters
        );

        // 3. Execute strategy
        let config = CompactionPlanningConfig::default();
        let groups = strategy.execute(files, &config).unwrap();

        // 4. Assertions
        assert_eq!(groups.len(), 4, "Should have 4 groups (two per partition)");

        // Collect the byte size of each group
        let mut group_sizes: Vec<u64> = groups.iter().map(|g| g.total_size_mb()).collect();
        group_sizes.sort();

        assert_eq!(
            group_sizes,
            vec![5, 35, 40, 50],
            "Binpacked group byte sizes should be [5, 35, 40, 50]"
        );
    }

    #[test]
    fn test_binpack_with_min_file_count_filter() {
        // Validates that BinPacking will optimize file groups by size within a partition, and
        // then filter groups which do not meet the minimum file count.

        // 1. Create files in 3 partitions, 2 files each
        let build_file = |name: &str, partition_num: i32, size_mb: u64| {
            TestFileBuilder::new(name)
                .size(size_mb * 1024 * 1024)
                .with_partition(create_partition_value(partition_num))
                .with_schema(get_test_schema_with_partition())
                .build()
        };

        let files = vec![
            // Partition 0: BinPacks to a group of 4 files (50MB), and 1 file (10MB)
            build_file("p0_file1.parquet", 0, 10),
            build_file("p0_file2.parquet", 0, 20),
            build_file("p0_file3.parquet", 0, 5),
            build_file("p0_file4.parquet", 0, 5),
            build_file("p0_file5.parquet", 0, 15),
            // Partition 1: BinPacks to a group of 3 files (40MB), and 2 files (35MB)
            build_file("p1_file1.parquet", 1, 10),
            build_file("p1_file2.parquet", 1, 20),
            build_file("p1_file3.parquet", 1, 15),
            build_file("p1_file4.parquet", 1, 5),
            build_file("p1_file5.parquet", 1, 20),
            build_file("p1_file6.parquet", 1, 5),
        ];

        // 2. Create strategy with BinPack grouping, and no group filters. Target size = 50MB.
        let strategy = PlanStrategy::new_custom(
            None, // no file filters
            None, // no delete file count filter
            GroupingStrategy::BinPack(crate::config::BinPackConfig::new(50 * 1024 * 1024)),
            Some(crate::config::GroupFilters {
                min_group_file_count: Some(2), // Requires at least 2 files per group
                min_group_size_bytes: None,
            }),
        );

        // 3. Execute strategy
        let config = CompactionPlanningConfig::default();
        let groups = strategy.execute(files, &config).unwrap();

        // 4. Assertions
        // Verify a 5MB file from Partition 0 was filtered out.
        assert_eq!(
            groups.len(),
            3,
            "Should have 3 groups (1 for partition 0; 2 for partition 1)"
        );

        // Collect the byte size of each group
        let mut group_sizes: Vec<u64> = groups.iter().map(|g| g.total_size_mb()).collect();
        group_sizes.sort();

        assert_eq!(
            group_sizes,
            vec![35, 40, 50],
            "Binpacked group byte sizes should be [35, 40, 50]"
        );
    }

    #[test]
    fn test_binpack_with_min_file_count_and_min_size_filters() {
        // Validates that BinPacking will optimize file groups by size within a partition
        // and can apply filters to the groups

        let target_filters = crate::config::GroupFilters {
            min_group_file_count: Some(2), // Requires at least 2 files per group
            min_group_size_bytes: Some(20 * 1024 * 1024), // Requires at least 15MB per group
        };

        // 1. Create files in 3 partitions, 2 files each
        let build_file = |name: &str, partition_num: i32, size_mb: u64| {
            TestFileBuilder::new(name)
                .size(size_mb * 1024 * 1024)
                .with_partition(create_partition_value(partition_num))
                .with_schema(get_test_schema_with_partition())
                .build()
        };

        let files = vec![
            // Partition 0: Enough data, but not enough files
            build_file("p0_file1.parquet", 0, 30),
            // Partition 1: Enough files, but not enough data
            build_file("p1_file1.parquet", 1, 5),
            build_file("p1_file2.parquet", 1, 5),
            // Partition 2: Enough data, and enough files
            build_file("p2_file1.parquet", 2, 10),
            build_file("p2_file2.parquet", 2, 10),
            // Partition 3: First group has enough data/files, 2nd group has too little data
            build_file("p3_file1.parquet", 3, 25),
            build_file("p3_file2.parquet", 3, 25),
            build_file("p3_file3.parquet", 3, 5),
            build_file("p3_file4.parquet", 3, 5),
            // Partition 4: First group has enough data/files, 2nd group has too few files
            build_file("p4_file1.parquet", 4, 25),
            build_file("p4_file2.parquet", 4, 25),
            build_file("p4_file3.parquet", 4, 30),
        ];

        // 2. Create strategy with BinPack grouping, and no group filters. Target size = 50MB.
        let strategy = PlanStrategy::new_custom(
            None, // no file filters
            None, // no delete file count filter
            GroupingStrategy::BinPack(crate::config::BinPackConfig::new(50 * 1024 * 1024)),
            Some(target_filters),
        );

        // 3. Execute strategy
        let config = CompactionPlanningConfig::default();
        let groups = strategy.execute(files, &config).unwrap();

        // 4. Assertions
        assert_eq!(groups.len(), 3, "Should have 3 groups (partitions 2, 3, 4)");

        // Collect the byte size of each group
        let mut group_sizes: Vec<u64> = groups.iter().map(|g| g.total_size_mb()).collect();
        group_sizes.sort();

        assert_eq!(
            group_sizes,
            vec![20, 50, 50],
            "Binpacked group byte sizes should be [20, 50, 50]"
        );
    }

    #[test]
    fn test_single_strategy_with_mixed_unpartitioned_and_partitioned_files() {
        let build_file = |name: &str, partition: Option<iceberg::spec::Struct>| {
            TestFileBuilder::new(name)
                .size(10 * 1024 * 1024)
                .with_schema(get_test_schema_with_partition())
                .with_partition(partition.unwrap_or_else(iceberg::spec::Struct::empty))
                .build()
        };

        // Create files with empty partition with the standard default value (empty struct)
        let file_empty_1 = build_file("empty_1.parquet", Some(iceberg::spec::Struct::empty()));
        let file_empty_2 = build_file("empty_2.parquet", Some(iceberg::spec::Struct::empty()));

        // Create files with a `None` partition value, to validate we handle this edge case
        let mut file_none_1 = build_file("none_1.parquet", None);
        file_none_1.partition = None; // Explicitly set to None
        let mut file_none_2 = build_file("none_2.parquet", None);
        file_none_2.partition = None;

        // Create files with actual partition values
        let file_p0 = build_file("partition_0.parquet", Some(create_partition_value(0)));
        let file_p1 = build_file("partition_1.parquet", Some(create_partition_value(1)));

        let files = vec![
            file_none_1,
            file_empty_1,
            file_p0,
            file_none_2,
            file_empty_2,
            file_p1,
        ];

        // Create Single grouping strategy
        let strategy = PlanStrategy::new_custom(None, None, GroupingStrategy::Single, None);

        let config = CompactionPlanningConfig::default();
        let groups = strategy.execute(files, &config).unwrap();

        // Should have 3 groups:
        // - Group 1: unpartitioned files (None + empty partition)
        // - Group 2: partition 0
        // - Group 3: partition 1
        assert_eq!(groups.len(), 3, "Should have 3 groups");

        // Find the unpartitioned group (should have 4 files)
        let unpartitioned_group = groups.iter().find(|g| g.data_file_count == 4);
        assert!(
            unpartitioned_group.is_some(),
            "Should have a group with 4 unpartitioned files"
        );

        // Verify partition groups each have 1 file
        let partition_groups: Vec<_> = groups.iter().filter(|g| g.data_file_count == 1).collect();
        assert_eq!(
            partition_groups.len(),
            2,
            "Should have 2 partition groups with 1 file each"
        );
    }

    #[test]
    fn test_binpack_strategy_with_mixed_unpartitioned_and_partitioned_files() {
        let build_file = |name: &str, size_mb: u64, partition: Option<iceberg::spec::Struct>| {
            TestFileBuilder::new(name)
                .size(size_mb * 1024 * 1024)
                .with_schema(get_test_schema_with_partition())
                .with_partition(partition.unwrap_or_else(iceberg::spec::Struct::empty))
                .build()
        };

        // Create files with empty partition with the standard default value (empty struct)
        let file_empty_1 = build_file("empty_1.parquet", 10, Some(iceberg::spec::Struct::empty()));
        let file_empty_2 = build_file("empty_2.parquet", 25, Some(iceberg::spec::Struct::empty()));

        // Create files with a `None` partition value, to validate we handle this edge case
        let mut file_none_1 = build_file("none_1.parquet", 15, None);
        file_none_1.partition = None;
        let mut file_none_2 = build_file("none_2.parquet", 20, None);
        file_none_2.partition = None;

        // Create partitioned files
        let file_p0_1 = build_file("p0_file1.parquet", 15, Some(create_partition_value(0)));
        let file_p0_2 = build_file("p0_file2.parquet", 20, Some(create_partition_value(0)));
        let file_p1 = build_file("p1_file1.parquet", 30, Some(create_partition_value(1)));

        let files = vec![
            file_none_1,  // 15MB
            file_empty_1, // 10MB
            file_p0_1,    // 15MB
            file_none_2,  // 20MB
            file_empty_2, // 25MB
            file_p0_2,    // 20MB
            file_p1,      // 30MB
        ];

        // Create BinPack strategy with 50MB target
        let strategy = PlanStrategy::new_custom(
            None,
            None,
            GroupingStrategy::BinPack(crate::config::BinPackConfig::new(50 * 1024 * 1024)),
            None,
        );

        let config = CompactionPlanningConfig::default();
        let groups = strategy.execute(files, &config).unwrap();

        // Verify partitions are kept separate:
        // - Unpartitioned (None + empty): 70MB total -> should be binpacked into groups
        // - Partition 0: 35MB total
        // - Partition 1: 30MB total

        // Total should be at least 3 groups (one per partition type)
        assert!(groups.len() >= 3, "Should have at least 3 groups");

        // Find groups by checking partition values
        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 7, "All 7 files should be in groups");

        // Verify unpartitioned files are grouped together
        let unpartitioned_files_count: usize = groups
            .iter()
            .filter(|g| {
                // Check if any file in the group has None or empty partition
                g.data_files.iter().any(|f| {
                    f.partition.is_none()
                        || f.partition.as_ref().is_some_and(|p| p.fields().is_empty())
                })
            })
            .map(|g| g.data_file_count)
            .sum();

        assert_eq!(
            unpartitioned_files_count, 4,
            "Should have 4 unpartitioned files grouped together"
        );
    }
}
