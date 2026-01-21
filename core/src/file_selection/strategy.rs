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

use iceberg::scan::FileScanTask;

use super::packer::ListPacker;
use crate::config::{
    CompactionPlanningConfig, GroupingStrategy, MAX_FILE_SIZE_DEFAULT_RATIO,
    MIN_FILE_SIZE_DEFAULT_RATIO, SPLIT_OVERHEAD,
};
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

    /// Calculates executor and output parallelism using Iceberg's algorithms.
    ///
    /// ## Output Parallelism (from Iceberg's `expectedOutputFiles`)
    /// - Uses target file size and input size to determine optimal output file count
    /// - Handles remainder distribution intelligently to avoid tiny files
    /// - Algorithm considers min/max file size constraints
    ///
    /// ## Input/Executor Parallelism (from Iceberg's `inputSplitSize`)
    /// - Calculates optimal split size for reading: `input_size / expected_output_files + SPLIT_OVERHEAD`
    /// - Input parallelism = `ceil(input_total_bytes / input_split_size)`
    /// - Also considers file count constraints: `ceil(files_count / max_file_count_per_partition)`
    /// - Takes the max of size-based and count-based parallelism
    ///
    /// Input parallelism is capped by `max_input_parallelism`, output parallelism is capped by `max_output_parallelism`.
    ///
    /// # Errors
    /// Returns error if `input_total_bytes()` is 0.
    fn calculate_parallelism(
        files_to_compact: &FileGroup,
        config: &CompactionPlanningConfig,
    ) -> Result<(usize, usize)> {
        let input_size = files_to_compact.input_total_bytes();
        if input_size == 0 {
            return Err(CompactionError::Execution(
                "No files to calculate task parallelism".to_owned(),
            ));
        }

        let target_file_size = config.target_file_size_bytes();
        let min_file_size = Self::default_min_file_size(target_file_size);
        let max_file_size = Self::default_max_file_size(target_file_size);

        // Calculate output parallelism using Iceberg's expectedOutputFiles algorithm
        let output_parallelism =
            Self::expected_output_files(input_size, target_file_size, min_file_size, max_file_size)
                .min(config.max_output_parallelism())
                .max(1);

        // Apply output parallelism heuristic if enabled
        let output_parallelism =
            Self::apply_output_parallelism_heuristic(files_to_compact, config, output_parallelism);

        // Calculate input split size using Iceberg's inputSplitSize algorithm
        let split_size =
            Self::input_split_size(input_size, target_file_size, min_file_size, max_file_size);

        // Input parallelism based on split size
        let partition_by_size = input_size.div_ceil(split_size).max(1) as usize;

        // Also consider file count constraints
        let total_files_count = files_to_compact.input_files_count();
        let partition_by_count = total_files_count
            .div_ceil(config.max_file_count_per_partition())
            .max(1);

        // Input parallelism is the max of size-based and count-based, capped by max_input_parallelism
        let input_parallelism = partition_by_size
            .max(partition_by_count)
            .min(config.max_input_parallelism());

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

    /// Calculates the write maximum file size.
    ///
    /// This is the target file size plus half the difference between max and target,
    /// used to avoid creating tiny remainder files. From Iceberg's `writeMaxFileSize()`.
    ///
    /// Formula: `target_file_size + 0.5 * (max_file_size - target_file_size)`
    fn write_max_file_size(target_file_size: u64, max_file_size: u64) -> u64 {
        let diff = max_file_size.saturating_sub(target_file_size);
        target_file_size + diff / 2
    }

    /// Calculates the expected number of output files for the given input size.
    ///
    /// 1. If input size < target file size, return 1
    /// 2. Calculate files with and without remainder
    /// 3. If remainder > min file size, round up
    /// 4. Otherwise, if average file size without remainder is acceptable, round down
    /// 5. Otherwise, round up
    fn expected_output_files(
        input_size: u64,
        target_file_size: u64,
        min_file_size: u64,
        max_file_size: u64,
    ) -> usize {
        if input_size < target_file_size {
            return 1;
        }

        let num_files_with_remainder = input_size.div_ceil(target_file_size);
        let num_files_without_remainder = input_size / target_file_size;

        if num_files_without_remainder == 0 {
            return 1;
        }

        let remainder = input_size % target_file_size;
        let avg_file_size_without_remainder = input_size / num_files_without_remainder;
        let write_max = Self::write_max_file_size(target_file_size, max_file_size);

        // If remainder is larger than minimum file size, we should round up
        if remainder > min_file_size {
            num_files_with_remainder as usize
        } else if avg_file_size_without_remainder
            <= ((target_file_size as f64 * 1.1) as u64).min(write_max)
        {
            // If the average file size without remainder is acceptable (< 1.1 * target or < writeMax),
            // we can round down and distribute the remainder
            num_files_without_remainder as usize
        } else {
            // Otherwise, round up
            num_files_with_remainder as usize
        }
    }

    /// Calculates the input split size for reading files.
    ///
    /// From Iceberg's `inputSplitSize()`. This determines how to partition input
    /// for parallel reading.
    ///
    /// Formula: `max(target_file_size, min(estimated_split_size, write_max_file_size))`
    /// where `estimated_split_size = (input_size / expected_output_files) + SPLIT_OVERHEAD`
    fn input_split_size(
        input_size: u64,
        target_file_size: u64,
        min_file_size: u64,
        max_file_size: u64,
    ) -> u64 {
        let expected_files =
            Self::expected_output_files(input_size, target_file_size, min_file_size, max_file_size);
        let estimated_split_size = (input_size / expected_files.max(1) as u64) + SPLIT_OVERHEAD;
        let write_max = Self::write_max_file_size(target_file_size, max_file_size);

        if estimated_split_size < target_file_size {
            target_file_size
        } else {
            estimated_split_size.min(write_max)
        }
    }

    /// Calculates `min_file_size` from `target_file_size` using default ratio.
    fn default_min_file_size(target_file_size: u64) -> u64 {
        (target_file_size as f64 * MIN_FILE_SIZE_DEFAULT_RATIO) as u64
    }

    /// Calculates `max_file_size` from `target_file_size` using default ratio.
    fn default_max_file_size(target_file_size: u64) -> u64 {
        (target_file_size as f64 * MAX_FILE_SIZE_DEFAULT_RATIO) as u64
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
        match self {
            GroupingStrategyEnum::Single(strategy) => strategy.group_files(data_files),
            GroupingStrategyEnum::BinPack(strategy) => strategy.group_files(data_files),
        }
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

    /// Constructs strategy for small files compaction.
    ///
    /// Adds `SizeFilterStrategy` with `max_size = small_file_threshold_bytes`.
    pub fn from_small_files(config: &crate::config::SmallFilesConfig) -> Self {
        let file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![Box::new(SizeFilterStrategy {
            min_size: None,
            max_size: Some(config.small_file_threshold_bytes),
        })];

        let (grouping, group_filters) = Self::build_grouping_and_filters(
            &config.grouping_strategy,
            config.group_filters.as_ref(),
        );

        Self::new(file_filters, grouping, group_filters)
    }

    /// Constructs strategy for full compaction.
    ///
    /// No file filters. No group filters (full compaction processes all groups).
    pub fn from_full(config: &crate::config::FullCompactionConfig) -> Self {
        let file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![];

        // Full compaction never uses group filters
        let (grouping, group_filters) =
            Self::build_grouping_and_filters(&config.grouping_strategy, None);

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

        let (grouping, group_filters) = Self::build_grouping_and_filters(
            &config.grouping_strategy,
            config.group_filters.as_ref(),
        );

        Self::new(file_filters, grouping, group_filters)
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

        let (grouping, group_filter_strategies) =
            Self::build_grouping_and_filters(&grouping_strategy, group_filters.as_ref());

        Self::new(file_filters, grouping, group_filter_strategies)
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
                                Some(vec![1, 2])
                            } else {
                                None
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
                equality_ids: None,
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
        assert_eq!(result.len(), 4);
        TestUtils::assert_paths_eq(
            &[
                "min_edge.parquet",
                "medium1.parquet",
                "medium2.parquet",
                "max_edge.parquet",
            ],
            &result,
        );

        for file in &result {
            assert!(file.length >= 5 * 1024 * 1024 && file.length <= 50 * 1024 * 1024);
        }

        // Test min = max (exact match only)
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
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data_file_path, "exact.parquet");

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
            assert!(file.length <= small_file_threshold);
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
            assert!(file.length <= small_file_threshold_default);
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
    fn test_file_group_parallelism_calculation() {
        // Test FileGroup::calculate_parallelism functionality
        let small_files_config = SmallFilesConfigBuilder::default()
            .min_size_per_partition(10 * 1024 * 1024_u64) // 10MB per partition
            .max_file_count_per_partition(5_usize) // 5 files per partition
            .max_input_parallelism(8_usize) // Max 8 parallel input tasks
            .max_output_parallelism(8_usize) // Max 8 parallel output tasks
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
        assert!(executor_parallelism <= config.max_input_parallelism());

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
            .max_input_parallelism(8_usize)
            .max_output_parallelism(8_usize)
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
            .max_input_parallelism(8_usize)
            .max_output_parallelism(8_usize)
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
            .max_input_parallelism(8_usize)
            .max_output_parallelism(8_usize)
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
                group.executor_parallelism <= config.max_input_parallelism(),
                "Group {} should respect max_input_parallelism",
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
                .min(config.max_input_parallelism());

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
            .max_input_parallelism(1000_usize)
            .max_output_parallelism(1000_usize)
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
        assert!(exec_p <= config.max_input_parallelism());
    }

    // Test 1: Output parallelism limiting small files
    #[test]
    fn test_output_parallelism_prevents_small_files() {
        let config = SmallFilesConfigBuilder::default()
            .target_file_size_bytes(1024 * 1024 * 1024_u64) // 1GB target
            .max_input_parallelism(32_usize)
            .max_output_parallelism(4_usize) // Limit to 4 output files
            .enable_heuristic_output_parallelism(false)
            .build()
            .unwrap();
        let planning_config = CompactionPlanningConfig::SmallFiles(config);

        // Case 1: Many tiny files - should consolidate efficiently
        // 1000 files * 1MB = 1GB total
        // Despite having many files, total size is small (< target)
        // Expected: Should produce 1 output file (consolidate all tiny files)
        let tiny_files: Vec<FileScanTask> = (0..1000)
            .map(|i| {
                TestFileBuilder::new(&format!("tiny{}.parquet", i))
                    .size(1024 * 1024) // 1MB each - very small files
                    .build()
            })
            .collect();

        let tiny_group = FileGroup::new(tiny_files);
        let (_, tiny_output_p) =
            FileGroup::calculate_parallelism(&tiny_group, &planning_config).unwrap();

        assert_eq!(
            tiny_output_p, 1,
            "1000 tiny files (1GB total < 1GB target) should consolidate into 1 output file"
        );

        // Verify total size is reasonable for consolidation
        let tiny_total_size = tiny_group.input_total_bytes();
        assert_eq!(
            tiny_total_size,
            1000 * 1024 * 1024,
            "Total size should be 1000MB"
        );
        assert!(
            tiny_total_size < planning_config.target_file_size_bytes(),
            "Total size should be less than target, justifying single output file"
        );

        // Case 2: Large input that would naturally produce many files
        // 100 files * 100MB = 10GB total
        // Without cap: would produce ~10 output files
        // With cap: should produce exactly 4 output files
        // Result: 10GB / 4 = 2.5GB per file (no small files!)
        let files: Vec<FileScanTask> = (0..100)
            .map(|i| {
                TestFileBuilder::new(&format!("file{}.parquet", i))
                    .size(100 * 1024 * 1024) // 100MB each
                    .build()
            })
            .collect();

        let group = FileGroup::new(files);
        let (_, output_parallelism) =
            FileGroup::calculate_parallelism(&group, &planning_config).unwrap();

        assert_eq!(
            output_parallelism, 4,
            "Output parallelism should be capped to prevent creating ~10 files"
        );

        // Verify each output file would be >= target size
        let avg_output_size = group.input_total_bytes() / output_parallelism as u64;
        assert!(
            avg_output_size >= planning_config.target_file_size_bytes(),
            "Each output file should be >= target ({}GB >= 1GB)",
            avg_output_size / 1024 / 1024 / 1024
        );

        // Case 2: Very small input should produce only 1 output file
        let small_files = vec![
            TestFileBuilder::new("small.parquet")
                .size(100 * 1024 * 1024) // 100MB (< 1GB target)
                .build(),
        ];

        let small_group = FileGroup::new(small_files);
        let (_, small_output_p) =
            FileGroup::calculate_parallelism(&small_group, &planning_config).unwrap();

        assert_eq!(
            small_output_p, 1,
            "Small input (<target) should produce 1 output file"
        );

        // Case 3: Input at exact target boundary
        let exact_files: Vec<FileScanTask> = (0..4)
            .map(|i| {
                TestFileBuilder::new(&format!("exact{}.parquet", i))
                    .size(1024 * 1024 * 1024) // Exactly 1GB each
                    .build()
            })
            .collect();

        let exact_group = FileGroup::new(exact_files);
        let (_, exact_output_p) =
            FileGroup::calculate_parallelism(&exact_group, &planning_config).unwrap();

        assert_eq!(
            exact_output_p, 4,
            "4GB input with 1GB target and max=4 should produce 4 files"
        );

        // Case 4: Small remainder that should be absorbed
        // 10.1GB total (10GB + 100MB small remainder)
        // The small 100MB remainder should be absorbed into existing files
        // rather than creating a tiny output file
        let files_small_remainder = vec![
            TestFileBuilder::new("base1.parquet")
                .size(1024 * 1024 * 1024)
                .build(), // 1GB
            TestFileBuilder::new("base2.parquet")
                .size(1024 * 1024 * 1024)
                .build(), // 1GB
            TestFileBuilder::new("base3.parquet")
                .size(1024 * 1024 * 1024)
                .build(), // 1GB
            TestFileBuilder::new("tiny_remainder.parquet")
                .size(100 * 1024 * 1024)
                .build(), // 100MB tiny remainder
        ];

        let small_remainder_group = FileGroup::new(files_small_remainder);
        let (_, small_remainder_output_p) =
            FileGroup::calculate_parallelism(&small_remainder_group, &planning_config).unwrap();

        // Should still be capped at 4, and the small remainder should be absorbed
        assert_eq!(
            small_remainder_output_p, 3,
            "10.1GB input with tiny remainder should still produce 4 files, absorbing the small remainder"
        );

        let total_with_small_remainder = small_remainder_group.input_total_bytes();
        let avg_with_small_remainder = total_with_small_remainder / small_remainder_output_p as u64;

        // Each file should be > 1GB (since we're distributing 10.1GB across 4 files)
        assert!(
            avg_with_small_remainder > planning_config.target_file_size_bytes(),
            "Small remainder should be absorbed, making each file > target size"
        );
    }

    /// Test 2: Input parallelism is constrained by multiple factors
    #[test]
    fn test_input_parallelism_multiple_constraints() {
        let config = SmallFilesConfigBuilder::default()
            .target_file_size_bytes(1024 * 1024 * 1024_u64) // 1GB target
            .min_size_per_partition(200 * 1024 * 1024_u64) // 200MB per partition
            .max_file_count_per_partition(10_usize) // Max 10 files per partition
            .max_input_parallelism(8_usize) // Cap at 8
            .max_output_parallelism(4_usize)
            .enable_heuristic_output_parallelism(false)
            .build()
            .unwrap();
        let planning_config = CompactionPlanningConfig::SmallFiles(config);

        // Case 1: Count constraint dominates and hits cap
        // 100 files * 10MB = 1GB total
        // Count-based: ceil(100 / 10) = 10 partitions needed
        // Cap: 8
        // Expected: 8 (count-based capped by max_input_parallelism)
        let many_small_files: Vec<FileScanTask> = (0..100)
            .map(|i| {
                TestFileBuilder::new(&format!("small{}.parquet", i))
                    .size(10 * 1024 * 1024) // 10MB each
                    .build()
            })
            .collect();

        let group1 = FileGroup::new(many_small_files);
        let (input_p1, _) = FileGroup::calculate_parallelism(&group1, &planning_config).unwrap();

        assert_eq!(
            input_p1, 8,
            "100 files (count needs 10 partitions) should be capped at max_input_parallelism=8"
        );

        // Case 2: Count constraint without hitting cap
        // 20 files * 50MB = 1GB total
        // Count-based: ceil(20 / 10) = 2 partitions
        // Expected: >= 2 (respects count constraint, under cap)
        let medium_count_files: Vec<FileScanTask> = (0..20)
            .map(|i| {
                TestFileBuilder::new(&format!("medium{}.parquet", i))
                    .size(50 * 1024 * 1024) // 50MB each
                    .build()
            })
            .collect();

        let group2 = FileGroup::new(medium_count_files);
        let (input_p2, _) = FileGroup::calculate_parallelism(&group2, &planning_config).unwrap();

        assert!(
            input_p2 >= 2,
            "20 files should yield at least ceil(20/10)=2 parallelism, got {}",
            input_p2
        );
        assert!(input_p2 <= 8, "Should not exceed max_input_parallelism");

        // Case 3: Few files, small total size
        // 5 files * 100MB = 500MB total
        // Count-based: ceil(5 / 10) = 1 partition
        // Size-based: also small
        // Expected: 1-2 (minimal parallelism needed)
        let few_files: Vec<FileScanTask> = (0..5)
            .map(|i| {
                TestFileBuilder::new(&format!("few{}.parquet", i))
                    .size(100 * 1024 * 1024) // 100MB each
                    .build()
            })
            .collect();

        let group3 = FileGroup::new(few_files);
        let (input_p3, _) = FileGroup::calculate_parallelism(&group3, &planning_config).unwrap();

        assert!(input_p3 >= 1, "Should have at least 1 parallelism");
        assert!(
            input_p3 < 8,
            "Small input shouldn't need to hit the cap, got {}",
            input_p3
        );

        // Case 4: Large files (size constraint dominates)
        // 50 files * 500MB = 25GB total
        // Size-based: 25GB is large, will compute high parallelism
        // Expected: hits cap at 8
        let large_files: Vec<FileScanTask> = (0..50)
            .map(|i| {
                TestFileBuilder::new(&format!("large{}.parquet", i))
                    .size(500 * 1024 * 1024) // 500MB each
                    .build()
            })
            .collect();

        let group4 = FileGroup::new(large_files);
        let (input_p4, _) = FileGroup::calculate_parallelism(&group4, &planning_config).unwrap();

        assert_eq!(
            input_p4, 8,
            "25GB input should hit max_input_parallelism cap"
        );

        // Summary verification: all parallelism values respect the cap
        assert!(
            input_p1 <= 8 && input_p2 <= 8 && input_p3 <= 8 && input_p4 <= 8,
            "All cases must respect max_input_parallelism"
        );
    }
}
