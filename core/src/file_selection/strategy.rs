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

use crate::compaction::CompactionType;
use crate::config::{CompactionPlanningConfig, GroupingStrategy};
use crate::{CompactionError, Result};
use iceberg::scan::FileScanTask;

/// A group of files selected for compaction
///
/// This struct encapsulates a collection of files that should be compacted together,
/// along with useful metadata about the group and associated delete files.
#[derive(Debug, Clone)]
pub struct FileGroup {
    /// The data files in this group
    pub data_files: Vec<FileScanTask>,
    /// Position delete files associated with the data files
    pub position_delete_files: Vec<FileScanTask>,
    /// Equality delete files associated with the data files
    pub equality_delete_files: Vec<FileScanTask>,
    /// Total size of all files in this group (in bytes)
    pub total_size: u64,
    /// Number of data files in this group
    pub data_file_count: usize,
    /// Executor parallelism for this file group
    pub executor_parallelism: usize,
    /// Output parallelism for this file group
    pub output_parallelism: usize,
}

impl FileGroup {
    /// Create a new file group from a collection of data files
    /// This will automatically extract and organize delete files from the data files
    pub fn new(data_files: Vec<FileScanTask>) -> Self {
        use std::collections::HashMap;

        let total_size = data_files.iter().map(|task| task.length).sum();
        let data_file_count = data_files.len();

        let mut position_delete_files = HashMap::new();
        let mut equality_delete_files = HashMap::new();

        // Extract delete files from data files (similar to build_input_file_scan_tasks logic)
        for task in &data_files {
            for delete_task in &task.deletes {
                let mut delete_task = delete_task.as_ref().clone();
                match &delete_task.data_file_content {
                    iceberg::spec::DataContentType::PositionDeletes => {
                        delete_task.project_field_ids = vec![];
                        position_delete_files
                            .insert(delete_task.data_file_path.clone(), delete_task);
                    }
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        delete_task.project_field_ids = delete_task.equality_ids.clone();
                        equality_delete_files
                            .insert(delete_task.data_file_path.clone(), delete_task);
                    }
                    _ => {
                        // Skip other types
                    }
                }
            }
        }

        Self {
            data_files,
            position_delete_files: position_delete_files.into_values().collect(),
            equality_delete_files: equality_delete_files.into_values().collect(),
            total_size,
            data_file_count,
            executor_parallelism: 1, // default value, will be calculated later
            output_parallelism: 1,   // default value, will be calculated later
        }
    }

    /// Create a new file group with calculated parallelism
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

    /// Create an empty file group
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

    /// Calculate parallelism for the file group based on configuration
    fn calculate_parallelism(
        files_to_compact: &FileGroup,
        config: &CompactionPlanningConfig,
    ) -> Result<(usize, usize)> {
        let total_file_size_for_partitioning = files_to_compact.input_total_bytes();
        if total_file_size_for_partitioning == 0 {
            // If the total data file size is 0, we cannot partition by size.
            // This means there are no data files to compact.
            return Err(CompactionError::Execution(
                "No files to calculate_task_parallelism".to_owned(),
            ));
        }

        let partition_by_size = total_file_size_for_partitioning
            .div_ceil(config.min_size_per_partition)
            .max(1) as usize; // Ensure at least one partition.

        let total_files_count_for_partitioning = files_to_compact.input_files_count();

        let partition_by_count = total_files_count_for_partitioning
            .div_ceil(config.max_file_count_per_partition)
            .max(1); // Ensure at least one partition.

        let input_parallelism = partition_by_size
            .max(partition_by_count)
            .min(config.max_parallelism);

        // `output_parallelism` should not exceed `input_parallelism`
        // and should also not exceed max_parallelism.
        // It's primarily driven by size to avoid small output files.
        let mut output_parallelism = partition_by_size
            .min(input_parallelism)
            .min(config.max_parallelism);

        // Heuristic: If the total task data size is very small (less than target_file_size_bytes),
        // force output_parallelism to 1 to encourage merging into a single, larger output file.
        if config.enable_heuristic_output_parallelism {
            let total_data_file_size = files_to_compact
                .data_files
                .iter()
                .map(|f| f.file_size_in_bytes)
                .sum::<u64>();

            if total_data_file_size > 0 // Only apply if there's data
            && total_data_file_size < config.base.target_file_size
            && output_parallelism > 1
            {
                output_parallelism = 1;
            }
        }

        Ok((input_parallelism, output_parallelism))
    }

    /// Check if the group is empty
    pub fn is_empty(&self) -> bool {
        self.data_files.is_empty()
    }

    /// Get the total size in MB for display purposes
    pub fn total_size_mb(&self) -> u64 {
        self.total_size / 1024 / 1024
    }

    /// Convert this group back to a flat list of data files
    pub fn into_files(self) -> Vec<FileScanTask> {
        self.data_files
    }

    /// Get a reference to the data files in this group
    pub fn files(&self) -> &[FileScanTask] {
        &self.data_files
    }

    /// Get a reference to the data files in this group (alias for backward compatibility)
    pub fn data_files(&self) -> &[FileScanTask] {
        &self.data_files
    }

    /// Get total count of all input files (data + delete files)
    pub fn input_files_count(&self) -> usize {
        self.data_files.len() + self.position_delete_files.len() + self.equality_delete_files.len()
    }

    /// Get total bytes of all input files (data + delete files)
    pub fn input_total_bytes(&self) -> u64 {
        self.data_files
            .iter()
            .chain(&self.position_delete_files)
            .chain(&self.equality_delete_files)
            .map(|task| task.file_size_in_bytes)
            .sum()
    }
}

/// Object-safe trait for file filtering strategies
///
/// This trait enables dynamic dispatch for composable file filter chains.
pub trait FileFilterStrategy: std::fmt::Debug {
    /// Filter the input data files, returning collected results
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask>;

    /// Get a description of this strategy for logging/debugging
    fn description(&self) -> String;
}

/// Enum-based grouping strategy for object-safe usage
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

    pub fn description(&self) -> String {
        match self {
            GroupingStrategyEnum::Noop(strategy) => strategy.description(),
            GroupingStrategyEnum::BinPack(strategy) => strategy.description(),
        }
    }
}

/// Object-safe trait for group filtering strategies
///
/// This trait enables dynamic dispatch for composable group filter chains.
pub trait GroupFilterStrategy: std::fmt::Debug {
    /// Filter groups of files based on group-level criteria
    fn filter_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup>;

    /// Get a description of this group filter strategy for logging/debugging
    fn description(&self) -> String;
}

/// No-op strategy that passes through all files unchanged
#[derive(Debug)]
pub struct NoopStrategy;

impl FileFilterStrategy for NoopStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask> {
        data_files
    }

    fn description(&self) -> String {
        "Noop".to_owned()
    }
}

/// No-op grouping strategy that places each file in its own group
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

    pub fn description(&self) -> String {
        "NoopGrouping".to_owned()
    }
}

/// No-op group filter strategy that passes through all groups
#[derive(Debug)]
pub struct NoopGroupFilterStrategy;

impl GroupFilterStrategy for NoopGroupFilterStrategy {
    fn filter_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup> {
        groups
    }

    fn description(&self) -> String {
        "NoopGroupFilter".to_owned()
    }
}

/// BinPack grouping strategy that optimizes file size distribution
#[derive(Debug)]
pub struct BinPackGroupingStrategy {
    pub target_group_size: u64,
    pub max_files_per_group: usize,
}

impl BinPackGroupingStrategy {
    pub fn group_files<I>(&self, data_files: I) -> Vec<FileGroup>
    where
        I: Iterator<Item = FileScanTask>,
    {
        use std::cmp::{Ordering, Reverse};
        use std::collections::BinaryHeap;

        #[derive(Default)]
        struct FileScanTaskGroup {
            idx: usize,
            tasks: Vec<FileScanTask>,
            total_length: u64,
        }

        impl Ord for FileScanTaskGroup {
            fn cmp(&self, other: &Self) -> Ordering {
                if self.total_length == other.total_length {
                    self.idx.cmp(&other.idx)
                } else {
                    self.total_length.cmp(&other.total_length)
                }
            }
        }

        impl PartialOrd for FileScanTaskGroup {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Eq for FileScanTaskGroup {}

        impl PartialEq for FileScanTaskGroup {
            fn eq(&self, other: &Self) -> bool {
                self.total_length == other.total_length
            }
        }

        let files: Vec<FileScanTask> = data_files.collect();

        // Calculate optimal number of groups based on total size and target group size
        let total_size: u64 = files.iter().map(|task| task.length).sum();

        // Handle edge case: when target_group_size is 0, use a single group
        // to avoid creating excessive number of groups
        let mut split_num = if self.target_group_size == 0 {
            1 // Use single group when target_group_size is 0
        } else {
            (total_size / self.target_group_size).max(1) as usize
        };

        // Ensure we don't exceed max files per group constraints
        if !files.is_empty() {
            let max_possible_groups = files.len() / self.max_files_per_group.max(1);
            split_num = split_num.max(max_possible_groups).max(1);
        }

        let mut heap = BinaryHeap::new();

        // Push all groups into heap
        for idx in 0..split_num {
            heap.push(Reverse(FileScanTaskGroup {
                idx,
                tasks: vec![],
                total_length: 0,
            }));
        }

        for file_task in files {
            let mut group = heap.peek_mut().unwrap();
            group.0.total_length += file_task.length;
            group.0.tasks.push(file_task);
        }

        // Convert heap into vec and extract tasks, then create FileGroups
        heap.into_vec()
            .into_iter()
            .map(|reverse_group| FileGroup::new(reverse_group.0.tasks))
            .filter(|group| !group.is_empty()) // Filter out empty groups
            .collect()
    }

    pub fn description(&self) -> String {
        format!(
            "BinPackGrouping[target={}MB, max_files={}]",
            self.target_group_size / 1024 / 1024,
            self.max_files_per_group
        )
    }
}

/// Strategy for filtering out files that have associated delete files
#[derive(Debug)]
pub struct NoDeleteFilesStrategy;

impl FileFilterStrategy for NoDeleteFilesStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask> {
        data_files
            .into_iter()
            .filter(|task| task.deletes.is_empty())
            .collect()
    }

    fn description(&self) -> String {
        "NoDeleteFiles".to_owned()
    }
}

/// Strategy for filtering files by size threshold
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

    fn description(&self) -> String {
        match (self.min_size, self.max_size) {
            (Some(min), Some(max)) => {
                format!("SizeFilter[{}-{}MB]", min / 1024 / 1024, max / 1024 / 1024)
            }
            (Some(min), None) => format!("SizeFilter[>{}MB]", min / 1024 / 1024),
            (None, Some(max)) => format!("SizeFilter[<{}MB]", max / 1024 / 1024),
            (None, None) => "SizeFilter[Any]".to_owned(),
        }
    }
}

/// Strategy for limiting total task size
#[derive(Debug)]
pub struct TaskSizeLimitStrategy {
    pub max_total_size: u64,
}

impl TaskSizeLimitStrategy {
    pub fn description(&self) -> String {
        format!(
            "TaskSizeLimit[{}GB]",
            self.max_total_size / 1024 / 1024 / 1024
        )
    }
}

impl FileFilterStrategy for TaskSizeLimitStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask> {
        let mut current_total = 0u64;
        data_files
            .into_iter()
            .take_while(|task| {
                let file_size = task.length;
                if current_total + file_size <= self.max_total_size {
                    current_total += file_size;
                    true
                } else {
                    false
                }
            })
            .collect()
    }

    fn description(&self) -> String {
        format!(
            "TaskSizeLimit[{}GB]",
            self.max_total_size / 1024 / 1024 / 1024
        )
    }
}

/// Strategy for ensuring minimum file count. If fewer than `min_file_count` files are available, no files will be returned.
#[derive(Debug)]
pub struct MinFileCountStrategy {
    pub min_file_count: usize,
}

impl MinFileCountStrategy {
    /// Create a new `MinFileCountStrategy` with minimum file count requirement
    pub fn new(min_file_count: usize) -> Self {
        Self { min_file_count }
    }

    pub fn description(&self) -> String {
        format!("MinFileCount[{}]", self.min_file_count)
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

    fn description(&self) -> String {
        format!("MinFileCount[{}]", self.min_file_count)
    }
}

/// Group filter strategy that filters groups based on minimum group size
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

    fn description(&self) -> String {
        format!("MinGroupSize[{}MB]", self.min_group_size / 1024 / 1024)
    }
}

/// Group filter strategy that filters groups based on maximum group size
#[derive(Debug)]
pub struct MaxGroupSizeStrategy {
    pub max_group_size: u64,
}

impl GroupFilterStrategy for MaxGroupSizeStrategy {
    fn filter_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup> {
        groups
            .into_iter()
            .filter(|group| group.total_size <= self.max_group_size)
            .collect()
    }

    fn description(&self) -> String {
        format!("MaxGroupSize[{}MB]", self.max_group_size / 1024 / 1024)
    }
}

/// Group filter strategy that filters groups based on minimum file count
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

    fn description(&self) -> String {
        format!("MinGroupFileCount[{}]", self.min_file_count)
    }
}

/// Group filter strategy that filters groups based on maximum file count
#[derive(Debug)]
pub struct MaxGroupFileCountStrategy {
    pub max_file_count: usize,
}

impl GroupFilterStrategy for MaxGroupFileCountStrategy {
    fn filter_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup> {
        groups
            .into_iter()
            .filter(|group| group.data_file_count <= self.max_file_count)
            .collect()
    }

    fn description(&self) -> String {
        format!("MaxGroupFileCount[{}]", self.max_file_count)
    }
}

/// Factory for creating file strategies based on compaction type and configuration
pub struct FileStrategyFactory;

/// Three-layer strategy that combines file filtering, grouping, and group filtering
///
/// This strategy provides a flexible composition architecture where:
/// - File filters are composable using Vec<Box<dyn FileFilterStrategy>>
/// - Grouping is handled by a single enum-based strategy
/// - Group filters are composable using Vec<Box<dyn GroupFilterStrategy>>
#[derive(Debug)]
pub struct ThreeLayerStrategy {
    file_filters: Vec<Box<dyn FileFilterStrategy>>,
    grouping: GroupingStrategyEnum,
    group_filters: Vec<Box<dyn GroupFilterStrategy>>,
}

impl ThreeLayerStrategy {
    /// Create a new three-layer strategy
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

    /// Apply the three-layer strategy to filter and group files
    pub fn execute(
        &self,
        data_files: Vec<FileScanTask>,
        config: &CompactionPlanningConfig,
    ) -> Result<Vec<FileGroup>> {
        // Step 1: Apply file filtering
        let mut filtered_files = data_files;
        for filter in &self.file_filters {
            filtered_files = filter.filter(filtered_files);
        }

        // Step 2: Apply grouping - now returns FileGroup directly
        let mut file_groups = self.grouping.group_files(filtered_files.into_iter());

        // Step 3: Calculate parallelism for each group
        for group in &mut file_groups {
            let (executor_parallelism, output_parallelism) =
                FileGroup::calculate_parallelism(group, config)?;
            group.executor_parallelism = executor_parallelism;
            group.output_parallelism = output_parallelism;
        }

        // Step 4: Apply group filtering
        for filter in &self.group_filters {
            file_groups = filter.filter_groups(file_groups);
        }

        Ok(file_groups)
    }

    /// Get a description of this strategy for logging/debugging
    pub fn description(&self) -> String {
        let file_filter_desc = if self.file_filters.is_empty() {
            "NoFileFilters".to_string()
        } else {
            self.file_filters
                .iter()
                .map(|f| f.description())
                .collect::<Vec<_>>()
                .join(" -> ")
        };

        let group_filter_desc = if self.group_filters.is_empty() {
            "NoGroupFilters".to_string()
        } else {
            self.group_filters
                .iter()
                .map(|f| f.description())
                .collect::<Vec<_>>()
                .join(" -> ")
        };

        format!(
            "{} -> {} -> {}",
            file_filter_desc,
            self.grouping.description(),
            group_filter_desc
        )
    }
}

impl FileStrategyFactory {
    /// Create strategy for small files compaction
    ///
    /// Returns a three-layer strategy that filters out delete files,
    /// applies size filtering, and limits total task size.
    /// For small files, we use more permissive group filtering.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use iceberg_compaction_core::file_selection::strategy::FileStrategyFactory;
    /// # use iceberg_compaction_core::config::CompactionPlanningConfig;
    /// # let config = CompactionPlanningConfig::default();
    /// let strategy = FileStrategyFactory::create_small_files_strategy(&config);
    /// ```
    pub fn create_small_files_strategy(config: &CompactionPlanningConfig) -> ThreeLayerStrategy {
        // File filtering layer
        let mut file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![];
        file_filters.push(Box::new(NoDeleteFilesStrategy));
        file_filters.push(Box::new(SizeFilterStrategy {
            min_size: None,
            max_size: Some(config.small_file_threshold),
        }));
        file_filters.push(Box::new(MinFileCountStrategy::new(config.min_file_count)));
        file_filters.push(Box::new(TaskSizeLimitStrategy {
            max_total_size: config.max_task_total_size,
        }));

        // Grouping layer
        let grouping = match config.grouping_strategy {
            GroupingStrategy::Noop => GroupingStrategyEnum::Noop(NoopGroupingStrategy),
            GroupingStrategy::BinPack => GroupingStrategyEnum::BinPack(BinPackGroupingStrategy {
                target_group_size: config.max_group_size,
                max_files_per_group: config.max_group_file_count,
            }),
        };

        // Group filtering layer - for small files, use more permissive settings
        let mut group_filters: Vec<Box<dyn GroupFilterStrategy>> = vec![];
        // For small files, allow very small groups (any size above 0)
        group_filters.push(Box::new(MinGroupSizeStrategy { min_group_size: 0 }));
        group_filters.push(Box::new(MaxGroupSizeStrategy {
            max_group_size: config.max_group_size,
        }));
        // For small files, allow single-file groups
        group_filters.push(Box::new(MinGroupFileCountStrategy { min_file_count: 1 }));
        group_filters.push(Box::new(MaxGroupFileCountStrategy {
            max_file_count: config.max_group_file_count,
        }));

        ThreeLayerStrategy::new(file_filters, grouping, group_filters)
    }

    /// Create a no-op strategy that passes all files through
    pub fn create_noop_strategy() -> ThreeLayerStrategy {
        ThreeLayerStrategy::new(
            vec![Box::new(NoopStrategy)],
            GroupingStrategyEnum::Noop(NoopGroupingStrategy),
            vec![Box::new(NoopGroupFilterStrategy)],
        )
    }

    /// Create a custom strategy with flexible configuration
    ///
    /// This method provides fine-grained control over which filters to include.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use iceberg_compaction_core::file_selection::strategy::FileStrategyFactory;
    /// # use iceberg_compaction_core::config::GroupingStrategy;
    /// let strategy = FileStrategyFactory::create_custom_strategy(
    ///     true,                                  // exclude_delete_files
    ///     Some((None, Some(100 * 1024 * 1024))), // size_filter: max 100MB
    ///     3,                                     // min_file_count
    ///     20 * 1024 * 1024 * 1024,              // max_task_total_size: 20GB
    ///     GroupingStrategy::BinPack,             // grouping_strategy
    ///     64 * 1024 * 1024,                     // target_group_size: 64MB
    ///     100,                                   // max_files_per_group
    ///     32 * 1024 * 1024,                     // min_group_size: 32MB
    ///     1024 * 1024 * 1024,                   // max_group_size: 1GB
    ///     2,                                     // min_group_file_count
    ///     50,                                    // max_group_file_count
    /// );
    /// ```
    pub fn create_custom_strategy(
        exclude_delete_files: bool,
        size_filter: Option<(Option<u64>, Option<u64>)>, // (min_size, max_size)
        min_file_count: usize,
        max_task_total_size: u64,
        grouping_strategy: GroupingStrategy,
        target_group_size: u64,
        max_files_per_group: usize,
        min_group_size: u64,
        max_group_size: u64,
        min_group_file_count: usize,
        max_group_file_count: usize,
    ) -> ThreeLayerStrategy {
        // File filtering layer
        let mut file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![];

        if exclude_delete_files {
            file_filters.push(Box::new(NoDeleteFilesStrategy));
        }

        if let Some((min_size, max_size)) = size_filter {
            file_filters.push(Box::new(SizeFilterStrategy { min_size, max_size }));
        }

        if min_file_count > 0 {
            file_filters.push(Box::new(MinFileCountStrategy::new(min_file_count)));
        }

        if max_task_total_size < u64::MAX {
            file_filters.push(Box::new(TaskSizeLimitStrategy {
                max_total_size: max_task_total_size,
            }));
        }

        // Grouping layer
        let grouping = match grouping_strategy {
            GroupingStrategy::Noop => GroupingStrategyEnum::Noop(NoopGroupingStrategy),
            GroupingStrategy::BinPack => GroupingStrategyEnum::BinPack(BinPackGroupingStrategy {
                target_group_size,
                max_files_per_group,
            }),
        };

        // Group filtering layer
        let mut group_filters: Vec<Box<dyn GroupFilterStrategy>> = vec![];

        if min_group_size > 0 {
            group_filters.push(Box::new(MinGroupSizeStrategy { min_group_size }));
        }

        if max_group_size < u64::MAX {
            group_filters.push(Box::new(MaxGroupSizeStrategy { max_group_size }));
        }

        if min_group_file_count > 0 {
            group_filters.push(Box::new(MinGroupFileCountStrategy {
                min_file_count: min_group_file_count,
            }));
        }

        if max_group_file_count < usize::MAX {
            group_filters.push(Box::new(MaxGroupFileCountStrategy {
                max_file_count: max_group_file_count,
            }));
        }

        ThreeLayerStrategy::new(file_filters, grouping, group_filters)
    }

    /// Create a file strategy based on compaction type and configuration
    ///
    /// This is the main entry point for creating file strategies. It returns
    /// a `ThreeLayerStrategy` that uses the three-layer architecture
    /// with file filtering, grouping, and group filtering.
    ///
    /// # Arguments
    /// * `compaction_type` - The type of compaction to perform
    /// * `config` - The compaction configuration
    ///
    /// # Returns
    /// A `ThreeLayerStrategy` containing the appropriate strategy for the given compaction type
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use iceberg_compaction_core::file_selection::strategy::FileStrategyFactory;
    /// # use iceberg_compaction_core::compaction::CompactionType;
    /// # use iceberg_compaction_core::config::CompactionPlanningConfig;
    /// # let config = CompactionPlanningConfig::default();
    /// let strategy = FileStrategyFactory::create_files_strategy(
    ///     CompactionType::MergeSmallDataFiles,
    ///     &config
    /// );
    /// ```
    pub fn create_files_strategy(
        compaction_type: CompactionType,
        config: &CompactionPlanningConfig,
    ) -> ThreeLayerStrategy {
        match compaction_type {
            CompactionType::MergeSmallDataFiles => {
                // Use the small files strategy architecture
                Self::create_small_files_strategy(config)
            }
            CompactionType::Full => Self::create_noop_strategy(),
        }
    }

    /// Create a dynamic strategy variant based on compaction type and configuration
    ///
    /// This method returns a `ThreeLayerStrategy` for flexible composition based on config.
    ///
    /// # Arguments
    /// * `compaction_type` - The type of compaction to perform
    /// * `config` - The compaction configuration
    ///
    /// # Returns
    /// A `ThreeLayerStrategy` containing the appropriate strategy for the given parameters
    pub fn create_dynamic_strategy(
        compaction_type: CompactionType,
        config: &CompactionPlanningConfig,
    ) -> ThreeLayerStrategy {
        // Both strategies now use the same implementation
        Self::create_files_strategy(compaction_type, config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompactionPlanningConfigBuilder;

    // Lazy static schema to avoid rebuilding it for every test
    use std::sync::{Arc, OnceLock};
    static TEST_SCHEMA: OnceLock<Arc<iceberg::spec::Schema>> = OnceLock::new();

    fn get_test_schema() -> Arc<iceberg::spec::Schema> {
        TEST_SCHEMA
            .get_or_init(|| Arc::new(iceberg::spec::Schema::builder().build().unwrap()))
            .clone()
    }

    // Helper function to create test FileScanTask
    fn create_test_file_scan_task(file_path: &str, file_size: u64) -> FileScanTask {
        use iceberg::spec::{DataContentType, DataFileFormat};

        FileScanTask {
            start: 0,
            length: file_size,
            record_count: Some(100),
            data_file_path: file_path.to_owned(),
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![],
            sequence_number: 1,
            equality_ids: vec![],
            file_size_in_bytes: file_size,
        }
    }

    // Helper function to create test FileScanTask with delete files
    fn create_test_file_scan_task_with_deletes(
        file_path: &str,
        file_size: u64,
        has_deletes: bool,
    ) -> FileScanTask {
        use iceberg::spec::{DataContentType, DataFileFormat};
        use std::sync::Arc;

        let deletes = if has_deletes {
            // Create a dummy delete file task for testing
            vec![Arc::new(FileScanTask {
                start: 0,
                length: 1024,
                record_count: Some(10),
                data_file_path: format!("{}.delete", file_path),
                data_file_content: DataContentType::EqualityDeletes,
                data_file_format: DataFileFormat::Parquet,
                schema: get_test_schema(),
                project_field_ids: vec![1],
                predicate: None,
                deletes: vec![],
                sequence_number: 1,
                equality_ids: vec![],
                file_size_in_bytes: 1024,
            })]
        } else {
            vec![]
        };

        FileScanTask {
            start: 0,
            length: file_size,
            record_count: Some(100),
            data_file_path: file_path.to_owned(),
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: get_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes,
            sequence_number: 1,
            equality_ids: vec![],
            file_size_in_bytes: file_size,
        }
    }

    // Helper function to execute strategy and return flattened files for testing
    fn execute_strategy_flat(
        strategy: &ThreeLayerStrategy,
        data_files: Vec<FileScanTask>,
    ) -> Vec<FileScanTask> {
        // Create a default config for testing
        let config = crate::config::CompactionPlanningConfigBuilder::default()
            .build()
            .unwrap();

        strategy
            .execute(data_files, &config)
            .unwrap()
            .into_iter()
            .flat_map(|group| group.into_files())
            .collect()
    }

    // Helper function to create default config for testing
    fn create_test_config() -> crate::config::CompactionPlanningConfig {
        crate::config::CompactionPlanningConfigBuilder::default()
            .build()
            .unwrap()
    }

    #[test]
    fn test_noop_strategy() {
        let strategy = NoopStrategy;
        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
        ];

        let result_data: Vec<FileScanTask> = vec![strategy.filter(data_files.clone())]
            .into_iter()
            .flatten()
            .collect();

        assert_eq!(result_data.len(), data_files.len());
        assert_eq!(strategy.description(), "Noop");
    }

    #[test]
    fn test_size_filter_strategy() {
        let strategy = SizeFilterStrategy {
            min_size: Some(5 * 1024 * 1024),  // 5MB min
            max_size: Some(50 * 1024 * 1024), // 50MB max
        };

        // Test description
        assert_eq!(strategy.description(), "SizeFilter[5-50MB]");

        // Test filtering logic
        let data_files = vec![
            create_test_file_scan_task("small_file.parquet", 2 * 1024 * 1024), // 2MB - should be filtered out
            create_test_file_scan_task("medium_file1.parquet", 10 * 1024 * 1024), // 10MB - should pass
            create_test_file_scan_task("medium_file2.parquet", 30 * 1024 * 1024), // 30MB - should pass
            create_test_file_scan_task("large_file.parquet", 100 * 1024 * 1024), // 100MB - should be filtered out
        ];

        let result: Vec<FileScanTask> = strategy.filter(data_files);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "medium_file1.parquet");
        assert_eq!(result[1].data_file_path, "medium_file2.parquet");

        // Test edge cases
        let edge_case_strategy = SizeFilterStrategy {
            min_size: Some(1024 * 1024),
            max_size: Some(32 * 1024 * 1024),
        };
        assert_eq!(edge_case_strategy.description(), "SizeFilter[1-32MB]");
    }

    #[test]
    fn test_file_strategy_factory() {
        let config = CompactionPlanningConfigBuilder::default().build().unwrap();

        // Test individual strategy creation
        let noop_strategy = FileStrategyFactory::create_noop_strategy();
        assert!(noop_strategy.description().contains("Noop"));

        let small_files_strategy = FileStrategyFactory::create_small_files_strategy(&config);
        assert!(small_files_strategy.description().contains("NoDeleteFiles"));
        assert!(small_files_strategy.description().contains("SizeFilter"));
        assert!(small_files_strategy.description().contains("MinFileCount"));
        assert!(small_files_strategy.description().contains("TaskSizeLimit"));

        // Test unified strategy creation
        let small_files_unified = FileStrategyFactory::create_files_strategy(
            crate::compaction::CompactionType::MergeSmallDataFiles,
            &config,
        );
        assert!(small_files_unified.description().contains("NoDeleteFiles"));
        assert!(small_files_unified.description().contains("SizeFilter"));
        assert!(small_files_unified.description().contains("MinFileCount"));
        assert!(small_files_unified.description().contains("TaskSizeLimit"));

        let full_unified = FileStrategyFactory::create_files_strategy(
            crate::compaction::CompactionType::Full,
            &config,
        );
        // Full compaction now uses a permissive strategy with no filtering
        assert!(full_unified.description().contains("Noop"));
        assert!(full_unified.description().contains("NoopGrouping"));
        assert!(full_unified.description().contains("NoopGroupFilter"));
    }

    #[test]
    fn test_unified_strategy() {
        let config = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(10 * 1024 * 1024) // 10MB threshold
            .max_task_total_size(100 * 1024 * 1024) // 100MB task limit
            .min_group_size(0) // Allow any group size for testing
            .min_group_file_count(1) // Allow single-file groups for testing
            .build()
            .unwrap();

        // Test filtering functionality
        let strategy = FileStrategyFactory::create_files_strategy(
            crate::compaction::CompactionType::MergeSmallDataFiles,
            &config,
        );

        let data_files = vec![
            create_test_file_scan_task_with_deletes("small1.parquet", 5 * 1024 * 1024, false), // 5MB, no deletes - should pass
            create_test_file_scan_task_with_deletes("small2.parquet", 15 * 1024 * 1024, true), // 15MB, has deletes - should be filtered out
            create_test_file_scan_task_with_deletes("small3.parquet", 10 * 1024 * 1024, false), // 10MB, no deletes - should pass
            create_test_file_scan_task_with_deletes("large.parquet", 100 * 1024 * 1024, false), // 100MB - should be filtered out
        ];

        let result = strategy.execute(data_files, &create_test_config()).unwrap();

        // Result should contain groups, flatten to check individual files
        let flat_result: Vec<FileScanTask> = result
            .into_iter()
            .flat_map(|group| group.into_files())
            .collect();
        assert_eq!(flat_result.len(), 2);
        assert_eq!(flat_result[0].data_file_path, "small1.parquet");
        assert_eq!(flat_result[1].data_file_path, "small3.parquet");

        // Verify all selected files are under the threshold and have no deletes
        for file in &flat_result {
            assert!(file.length <= config.small_file_threshold);
            assert!(file.deletes.is_empty());
        }
    }

    #[test]
    fn test_no_delete_files_strategy() {
        let strategy = NoDeleteFilesStrategy;

        let data_files = vec![
            create_test_file_scan_task_with_deletes("file1.parquet", 10 * 1024 * 1024, false), // No deletes - should pass
            create_test_file_scan_task_with_deletes("file2.parquet", 20 * 1024 * 1024, true), // Has deletes - should be filtered out
            create_test_file_scan_task_with_deletes("file3.parquet", 15 * 1024 * 1024, false), // No deletes - should pass
        ];

        let result: Vec<FileScanTask> = strategy.filter(data_files);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "file1.parquet");
        assert_eq!(result[1].data_file_path, "file3.parquet");
        assert_eq!(strategy.description(), "NoDeleteFiles");
    }

    #[test]
    fn test_task_size_limit_strategy() {
        let strategy = TaskSizeLimitStrategy {
            max_total_size: 25 * 1024 * 1024, // 25MB total limit
        };

        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024), // 10MB - should pass (total: 10MB)
            create_test_file_scan_task("file2.parquet", 10 * 1024 * 1024), // 10MB - should pass (total: 20MB)
            create_test_file_scan_task("file3.parquet", 10 * 1024 * 1024), // 10MB - should be filtered out (would exceed 25MB)
            create_test_file_scan_task("file4.parquet", 5 * 1024 * 1024), // 5MB - should be filtered out
        ];

        let result: Vec<FileScanTask> = strategy.filter(data_files);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "file1.parquet");
        assert_eq!(result[1].data_file_path, "file2.parquet");
    }

    #[test]
    fn test_min_file_count_strategy() {
        let strategy = MinFileCountStrategy::new(3);

        // Test description
        assert_eq!(strategy.description(), "MinFileCount[3]");

        // Test with exactly minimum files
        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
            create_test_file_scan_task("file3.parquet", 15 * 1024 * 1024),
        ];

        let result: Vec<FileScanTask> = strategy.filter(data_files.clone());
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].data_file_path, "file1.parquet");
        assert_eq!(result[1].data_file_path, "file2.parquet");
        assert_eq!(result[2].data_file_path, "file3.parquet");

        // Test with more than minimum files
        let more_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
            create_test_file_scan_task("file3.parquet", 15 * 1024 * 1024),
            create_test_file_scan_task("file4.parquet", 25 * 1024 * 1024),
            create_test_file_scan_task("file5.parquet", 30 * 1024 * 1024),
        ];

        let result_more: Vec<FileScanTask> = strategy.filter(more_files);
        assert_eq!(result_more.len(), 5); // Returns all files when more than minimum

        // Test with fewer than minimum files
        let fewer_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
        ];

        let result_fewer: Vec<FileScanTask> = strategy.filter(fewer_files);
        assert_eq!(result_fewer.len(), 0); // Returns no files when not enough files

        // Test with minimum count of 0 (should pass all files)
        let zero_strategy = MinFileCountStrategy::new(0);
        let result_zero: Vec<FileScanTask> =
            zero_strategy.filter(vec![create_test_file_scan_task(
                "file1.parquet",
                10 * 1024 * 1024,
            )]);
        assert_eq!(result_zero.len(), 1);

        // Test with minimum count of 1
        let one_strategy = MinFileCountStrategy::new(1);
        let result_one: Vec<FileScanTask> = one_strategy.filter(vec![create_test_file_scan_task(
            "file1.parquet",
            10 * 1024 * 1024,
        )]);
        assert_eq!(result_one.len(), 1);
    }

    #[test]
    fn test_small_files_strategy_end_to_end() {
        let config = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(20 * 1024 * 1024) // 20MB threshold
            .max_task_total_size(50 * 1024 * 1024) // 50MB task limit
            .min_group_size(0) // Allow any group size for testing
            .min_group_file_count(1) // Allow single-file groups for testing
            .build()
            .unwrap();

        let strategy = FileStrategyFactory::create_small_files_strategy(&config);

        let data_files = vec![
            create_test_file_scan_task_with_deletes("small1.parquet", 5 * 1024 * 1024, false), // 5MB, no deletes - should pass
            create_test_file_scan_task_with_deletes("small2.parquet", 15 * 1024 * 1024, true), // 15MB, has deletes - should be filtered out
            create_test_file_scan_task_with_deletes("small3.parquet", 10 * 1024 * 1024, false), // 10MB, no deletes - should pass
            create_test_file_scan_task_with_deletes("small4.parquet", 25 * 1024 * 1024, false), // 25MB - should be filtered out (exceeds threshold)
            create_test_file_scan_task_with_deletes("large.parquet", 100 * 1024 * 1024, false), // 100MB - should be filtered out
        ];

        let result = execute_strategy_flat(&strategy, data_files);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "small1.parquet");
        assert_eq!(result[1].data_file_path, "small3.parquet");

        // Verify all selected files are under the threshold and have no deletes
        for file in &result {
            assert!(file.length <= config.small_file_threshold);
            assert!(file.deletes.is_empty());
        }
    }

    #[test]
    fn test_small_files_strategy_with_min_file_count() {
        // Test min_file_count behavior with different values and scenarios

        // Test case 1: min_file_count = 3 with BinPack grouping
        let config_binpack = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(20 * 1024 * 1024) // 20MB threshold
            .max_task_total_size(50 * 1024 * 1024) // 50MB task limit
            .min_file_count(3) // Require at least 3 files
            .grouping_strategy(crate::config::GroupingStrategy::BinPack)
            .min_group_size(0)
            .min_group_file_count(1)
            .build()
            .unwrap();

        let strategy_binpack = FileStrategyFactory::create_small_files_strategy(&config_binpack);

        // Test description
        let description = strategy_binpack.description();
        assert!(description.contains("MinFileCount[3]"));
        assert!(description.contains("NoDeleteFiles"));
        assert!(description.contains("SizeFilter"));
        assert!(description.contains("TaskSizeLimit"));

        // Test with exactly minimum files (3) - should pass
        let exact_min_files = vec![
            create_test_file_scan_task_with_deletes("small1.parquet", 5 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small2.parquet", 10 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small3.parquet", 15 * 1024 * 1024, false),
        ];

        let exact_result = execute_strategy_flat(&strategy_binpack, exact_min_files);
        assert_eq!(
            exact_result.len(),
            3,
            "Should return all 3 files when exactly meeting minimum"
        );

        // Test case 2: min_file_count = 3 with Noop grouping for faster tests
        let config_noop = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(20 * 1024 * 1024)
            .max_task_total_size(50 * 1024 * 1024)
            .min_file_count(3)
            .grouping_strategy(crate::config::GroupingStrategy::Noop)
            .min_group_size(0)
            .min_group_file_count(1)
            .build()
            .unwrap();

        let strategy_noop = FileStrategyFactory::create_small_files_strategy(&config_noop);

        // Test with fewer than minimum files (2) - should return none
        let insufficient_files = vec![
            create_test_file_scan_task_with_deletes("small1.parquet", 5 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small2.parquet", 10 * 1024 * 1024, false),
        ];

        let insufficient_result = execute_strategy_flat(&strategy_noop, insufficient_files);
        assert_eq!(
            insufficient_result.len(),
            0,
            "Should return no files when not meeting minimum count"
        );

        // Test with more than minimum files (5) - should return all
        let many_files = vec![
            create_test_file_scan_task_with_deletes("small1.parquet", 5 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small2.parquet", 10 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small3.parquet", 15 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small4.parquet", 8 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small5.parquet", 12 * 1024 * 1024, false),
        ];

        let many_result = execute_strategy_flat(&strategy_noop, many_files);
        assert_eq!(
            many_result.len(),
            5,
            "Should return all files when more than minimum"
        );

        // Verify all selected files are under the threshold and have no deletes
        for file in &many_result {
            assert!(file.length <= config_noop.small_file_threshold);
            assert!(file.deletes.is_empty());
        }

        // Test case 3: min_file_count = 1, provide 2 files
        let config_min_1 = CompactionPlanningConfigBuilder::default()
            .min_file_count(1)
            .small_file_threshold(20 * 1024 * 1024)
            .grouping_strategy(crate::config::GroupingStrategy::Noop)
            .min_group_size(0)
            .min_group_file_count(1)
            .build()
            .unwrap();

        let strategy_min_1 = FileStrategyFactory::create_small_files_strategy(&config_min_1);

        let two_files = vec![
            create_test_file_scan_task_with_deletes("small1.parquet", 5 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small2.parquet", 10 * 1024 * 1024, false),
        ];

        let result = strategy_min_1
            .execute(two_files, &create_test_config())
            .unwrap();
        assert_eq!(
            result.len(),
            1,
            "Should return 1 group containing all files when minimum is 1"
        );
        assert_eq!(result[0].data_file_count, 2);

        // Test case 4: min_file_count = 0, should always return files (bypass mode)
        let config_min_0 = CompactionPlanningConfigBuilder::default()
            .min_file_count(0)
            .small_file_threshold(20 * 1024 * 1024)
            .grouping_strategy(crate::config::GroupingStrategy::Noop)
            .min_group_size(0)
            .min_group_file_count(1)
            .build()
            .unwrap();

        let strategy_min_0 = FileStrategyFactory::create_small_files_strategy(&config_min_0);

        let single_file = vec![create_test_file_scan_task_with_deletes(
            "small1.parquet",
            5 * 1024 * 1024,
            false,
        )];

        let result = strategy_min_0
            .execute(single_file, &create_test_config())
            .unwrap();
        assert_eq!(
            result.len(),
            1,
            "Should return single group when minimum is 0"
        );
    }

    #[test]
    fn test_config_behavior() {
        // Test that the default configuration behaves correctly (min_file_count = 0 means unlimited)
        let default_config = CompactionPlanningConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::Noop) // Use noop to preserve order
            .min_group_size(0) // Allow any group size for testing
            .min_group_file_count(1) // Allow single-file groups for testing
            .build()
            .unwrap();

        // Verify that default min_file_count is 0 (unlimited)
        assert_eq!(
            default_config.min_file_count, 0,
            "Default min_file_count should be 0 (unlimited)"
        );

        let strategy = FileStrategyFactory::create_small_files_strategy(&default_config);

        // Test description should include MinFileCount[0] for unlimited behavior
        let description = strategy.description();
        assert!(
            description.contains("MinFileCount[0]"),
            "Default strategy should show MinFileCount[0] for unlimited behavior, got: {}",
            description
        );

        // Test with single file - should pass (no minimum requirement)
        let single_file = vec![create_test_file_scan_task_with_deletes(
            "single.parquet",
            5 * 1024 * 1024,
            false,
        )];

        let result = execute_strategy_flat(&strategy, single_file);
        assert_eq!(
            result.len(),
            1,
            "Default config should accept single file (unlimited)"
        );
        assert_eq!(result[0].data_file_path, "single.parquet");

        // Test with empty iterator - should return empty (no files available)
        let empty_files: Vec<FileScanTask> = vec![];
        let result = execute_strategy_flat(&strategy, empty_files);
        assert_eq!(
            result.len(),
            0,
            "Default config should return empty when no files available"
        );

        // Test with multiple files - should return all files (unlimited)
        let multiple_files = vec![
            create_test_file_scan_task_with_deletes("file1.parquet", 5 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("file2.parquet", 8 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("file3.parquet", 12 * 1024 * 1024, false),
        ];

        let result = execute_strategy_flat(&strategy, multiple_files);
        assert_eq!(
            result.len(),
            3,
            "Default config should return all files (unlimited)"
        );

        // Verify files are processed in order
        assert_eq!(result[0].data_file_path, "file1.parquet");
        assert_eq!(result[1].data_file_path, "file2.parquet");
        assert_eq!(result[2].data_file_path, "file3.parquet");

        // Verify all files meet the size and delete criteria
        for file in &result {
            assert!(
                file.length <= default_config.small_file_threshold,
                "File {} should be under threshold {} but was {}",
                file.data_file_path,
                default_config.small_file_threshold,
                file.length
            );
            assert!(
                file.deletes.is_empty(),
                "File {} should have no delete files",
                file.data_file_path
            );
        }

        // Test different min_file_count values
        let configs_and_expected = vec![
            (0, "MinFileCount[0]"),   // Should work for 0 files
            (1, "MinFileCount[1]"),   // Should work for 1 file
            (5, "MinFileCount[5]"),   // Should work for 5 files
            (10, "MinFileCount[10]"), // Should work for 10 files
        ];

        for (min_count, expected_desc) in configs_and_expected {
            let config = CompactionPlanningConfigBuilder::default()
                .min_file_count(min_count)
                .build()
                .unwrap();

            assert_eq!(config.min_file_count, min_count);

            let strategy = FileStrategyFactory::create_small_files_strategy(&config);
            let description = strategy.description();
            assert!(
                description.contains(expected_desc),
                "Expected '{}' to contain '{}' for min_count={}",
                description,
                expected_desc,
                min_count
            );
        }
    }

    #[test]
    fn test_group_filter_strategies_with_file_groups() {
        // Test that the new FileGroup-based group filter strategies work correctly

        // Create test file groups
        let small_group = FileGroup::new(vec![
            create_test_file_scan_task("small1.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("small2.parquet", 10 * 1024 * 1024), // 10MB
        ]);

        let large_group = FileGroup::new(vec![
            create_test_file_scan_task("large1.parquet", 50 * 1024 * 1024), // 50MB
            create_test_file_scan_task("large2.parquet", 100 * 1024 * 1024), // 100MB
            create_test_file_scan_task("large3.parquet", 75 * 1024 * 1024), // 75MB
        ]);

        let single_file_group = FileGroup::new(vec![
            create_test_file_scan_task("single.parquet", 20 * 1024 * 1024), // 20MB
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

        // Test MaxGroupSizeStrategy
        let max_size_strategy = MaxGroupSizeStrategy {
            max_group_size: 50 * 1024 * 1024,
        }; // 50MB max
        let filtered_by_max_size = max_size_strategy.filter_groups(groups.clone());
        assert_eq!(filtered_by_max_size.len(), 2); // small_group (15MB) and single_file_group (20MB) should pass

        // Test MinGroupFileCountStrategy
        let min_file_count_strategy = MinGroupFileCountStrategy { min_file_count: 2 };
        let filtered_by_min_count = min_file_count_strategy.filter_groups(groups.clone());
        assert_eq!(filtered_by_min_count.len(), 2); // small_group (2 files) and large_group (3 files) should pass

        // Test MaxGroupFileCountStrategy
        let max_file_count_strategy = MaxGroupFileCountStrategy { max_file_count: 2 };
        let filtered_by_max_count = max_file_count_strategy.filter_groups(groups.clone());
        assert_eq!(filtered_by_max_count.len(), 2); // small_group (2 files) and single_file_group (1 file) should pass

        // Test NoopGroupFilterStrategy
        let noop_strategy = NoopGroupFilterStrategy;
        let noop_result = noop_strategy.filter_groups(groups.clone());
        assert_eq!(noop_result.len(), 3); // All groups should pass through

        // Test that descriptions are formatted correctly
        assert_eq!(min_size_strategy.description(), "MinGroupSize[100MB]");
        assert_eq!(max_size_strategy.description(), "MaxGroupSize[50MB]");
        assert_eq!(
            min_file_count_strategy.description(),
            "MinGroupFileCount[2]"
        );
        assert_eq!(
            max_file_count_strategy.description(),
            "MaxGroupFileCount[2]"
        );
        assert_eq!(noop_strategy.description(), "NoopGroupFilter");
    }

    #[test]
    fn test_create_custom_strategy_comprehensive() {
        // Test create_custom_strategy with various parameter combinations

        // Test case 1: All filters enabled (using Noop grouping for faster tests)
        let strategy_all_enabled = FileStrategyFactory::create_custom_strategy(
            true,                                               // exclude_delete_files
            Some((Some(1024 * 1024), Some(100 * 1024 * 1024))), // size_filter: 1MB-100MB
            3,                                                  // min_file_count
            10 * 1024 * 1024 * 1024,                            // max_task_total_size: 10GB
            crate::config::GroupingStrategy::Noop, // grouping_strategy: Use Noop for faster tests
            0,                                     // target_group_size: not used for noop
            50,                                    // max_files_per_group: not used for noop
            0,                                     // min_group_size: not used for noop
            512 * 1024 * 1024,                     // max_group_size: not used for noop
            1,                                     // min_group_file_count: not used for noop
            25,                                    // max_group_file_count: not used for noop
        );

        let description = strategy_all_enabled.description();
        assert!(description.contains("NoDeleteFiles"));
        assert!(description.contains("SizeFilter[1-100MB]"));
        assert!(description.contains("MinFileCount[3]"));
        assert!(description.contains("TaskSizeLimit[10GB]"));
        assert!(description.contains("NoopGrouping"));

        // Test case 2: Minimal filters (most disabled)
        let strategy_minimal = FileStrategyFactory::create_custom_strategy(
            false,                                 // exclude_delete_files: disabled
            None,                                  // size_filter: disabled
            0,                                     // min_file_count: disabled
            u64::MAX,                              // max_task_total_size: disabled
            crate::config::GroupingStrategy::Noop, // grouping_strategy: noop
            0,                                     // target_group_size: not used for noop
            0,                                     // max_files_per_group: not used for noop
            0,                                     // min_group_size: disabled
            u64::MAX,                              // max_group_size: disabled
            0,                                     // min_group_file_count: disabled
            usize::MAX,                            // max_group_file_count: disabled
        );

        let minimal_description = strategy_minimal.description();
        assert!(minimal_description.contains("NoopGrouping"));
        // Should have minimal filters
        assert!(!minimal_description.contains("NoDeleteFiles"));
        assert!(!minimal_description.contains("SizeFilter"));
        assert!(!minimal_description.contains("MinFileCount"));
        assert!(!minimal_description.contains("TaskSizeLimit"));
        assert!(!minimal_description.contains("MinGroupSize"));
        assert!(!minimal_description.contains("MaxGroupSize"));
        assert!(!minimal_description.contains("MinGroupFileCount"));
        assert!(!minimal_description.contains("MaxGroupFileCount"));

        // Test case 3: Only size filter with min but no max
        let strategy_min_size_only = FileStrategyFactory::create_custom_strategy(
            false,                                 // exclude_delete_files
            Some((Some(10 * 1024 * 1024), None)),  // size_filter: min 10MB only
            0,                                     // min_file_count: disabled
            u64::MAX,                              // max_task_total_size: disabled
            crate::config::GroupingStrategy::Noop, // grouping_strategy
            0,                                     // target_group_size
            0,                                     // max_files_per_group
            0,                                     // min_group_size: disabled
            u64::MAX,                              // max_group_size: disabled
            0,                                     // min_group_file_count: disabled
            usize::MAX,                            // max_group_file_count: disabled
        );

        let min_size_description = strategy_min_size_only.description();
        assert!(min_size_description.contains("SizeFilter[>10MB]"));

        // Test case 4: Only size filter with max but no min
        let strategy_max_size_only = FileStrategyFactory::create_custom_strategy(
            false,                                 // exclude_delete_files
            Some((None, Some(50 * 1024 * 1024))),  // size_filter: max 50MB only
            0,                                     // min_file_count: disabled
            u64::MAX,                              // max_task_total_size: disabled
            crate::config::GroupingStrategy::Noop, // grouping_strategy
            0,                                     // target_group_size
            0,                                     // max_files_per_group
            0,                                     // min_group_size: disabled
            u64::MAX,                              // max_group_size: disabled
            0,                                     // min_group_file_count: disabled
            usize::MAX,                            // max_group_file_count: disabled
        );

        let max_size_description = strategy_max_size_only.description();
        assert!(max_size_description.contains("SizeFilter[<50MB]"));

        // Test functional behavior with test data
        let test_files = vec![
            create_test_file_scan_task_with_deletes("small.parquet", 5 * 1024 * 1024, false), // 5MB, no deletes
            create_test_file_scan_task_with_deletes("medium.parquet", 50 * 1024 * 1024, false), // 50MB, no deletes
            create_test_file_scan_task_with_deletes("large.parquet", 200 * 1024 * 1024, false), // 200MB, no deletes
            create_test_file_scan_task_with_deletes("with_deletes.parquet", 30 * 1024 * 1024, true), // 30MB, has deletes
        ];

        // Test strategy with all filters enabled
        let result_all_enabled = execute_strategy_flat(&strategy_all_enabled, test_files.clone());
        // Should filter out files with deletes and files outside 1MB-100MB range
        // Also need at least 3 files, but we only have 2 qualifying files (small.parquet 5MB, medium.parquet 50MB)
        // So result should be empty due to min_file_count requirement
        assert_eq!(result_all_enabled.len(), 0);

        // Test strategy with minimal filters
        let result_minimal = execute_strategy_flat(&strategy_minimal, test_files.clone());
        // Should pass through all files (no filtering)
        assert_eq!(result_minimal.len(), 4);
    }

    #[test]
    fn test_create_dynamic_strategy() {
        // Test create_dynamic_strategy explicitly
        let config = CompactionPlanningConfigBuilder::default().build().unwrap();

        // Test with MergeSmallDataFiles - should behave like create_files_strategy
        let dynamic_small_files = FileStrategyFactory::create_dynamic_strategy(
            crate::compaction::CompactionType::MergeSmallDataFiles,
            &config,
        );
        let regular_small_files = FileStrategyFactory::create_files_strategy(
            crate::compaction::CompactionType::MergeSmallDataFiles,
            &config,
        );

        // Both should have identical descriptions
        assert_eq!(
            dynamic_small_files.description(),
            regular_small_files.description()
        );

        // Test with Full compaction - should behave like create_files_strategy
        let dynamic_full = FileStrategyFactory::create_dynamic_strategy(
            crate::compaction::CompactionType::Full,
            &config,
        );
        let regular_full = FileStrategyFactory::create_files_strategy(
            crate::compaction::CompactionType::Full,
            &config,
        );

        // Both should have identical descriptions
        assert_eq!(dynamic_full.description(), regular_full.description());

        // Test functional equivalence
        let test_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024), // 10MB
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024), // 20MB
        ];

        let dynamic_result = execute_strategy_flat(&dynamic_small_files, test_files.clone());
        let regular_result = execute_strategy_flat(&regular_small_files, test_files.clone());

        assert_eq!(dynamic_result.len(), regular_result.len());
        for (dynamic_file, regular_file) in dynamic_result.iter().zip(regular_result.iter()) {
            assert_eq!(dynamic_file.data_file_path, regular_file.data_file_path);
            assert_eq!(dynamic_file.length, regular_file.length);
        }
    }

    #[test]
    fn test_create_custom_strategy_grouping_strategies() {
        // Test different grouping strategies in create_custom_strategy

        // Test BinPack grouping
        let bin_pack_strategy = FileStrategyFactory::create_custom_strategy(
            false,                                    // exclude_delete_files
            None,                                     // size_filter: disabled
            0,                                        // min_file_count: disabled
            u64::MAX,                                 // max_task_total_size: disabled
            crate::config::GroupingStrategy::BinPack, // grouping_strategy
            64 * 1024 * 1024,                         // target_group_size: 64MB
            100,                                      // max_files_per_group
            0,                                        // min_group_size: disabled
            u64::MAX,                                 // max_group_size: disabled
            0,                                        // min_group_file_count: disabled
            usize::MAX,                               // max_group_file_count: disabled
        );

        let bin_pack_description = bin_pack_strategy.description();
        assert!(bin_pack_description.contains("BinPackGrouping[target=64MB, max_files=100]"));

        // Test Noop grouping
        let noop_strategy = FileStrategyFactory::create_custom_strategy(
            false,                                 // exclude_delete_files
            None,                                  // size_filter: disabled
            0,                                     // min_file_count: disabled
            u64::MAX,                              // max_task_total_size: disabled
            crate::config::GroupingStrategy::Noop, // grouping_strategy
            0,                                     // target_group_size: not used
            0,                                     // max_files_per_group: not used
            0,                                     // min_group_size: disabled
            u64::MAX,                              // max_group_size: disabled
            0,                                     // min_group_file_count: disabled
            usize::MAX,                            // max_group_file_count: disabled
        );

        let noop_description = noop_strategy.description();
        assert!(noop_description.contains("NoopGrouping"));

        // Test functional behavior with multiple files for grouping
        let test_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024), // 10MB
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024), // 20MB
            create_test_file_scan_task("file3.parquet", 30 * 1024 * 1024), // 30MB
            create_test_file_scan_task("file4.parquet", 40 * 1024 * 1024), // 40MB
        ];

        let config = create_test_config();
        let bin_pack_groups = bin_pack_strategy
            .execute(test_files.clone(), &config)
            .unwrap();
        let noop_groups = noop_strategy.execute(test_files.clone(), &config).unwrap();

        // BinPack should create more or equal groups than noop (which creates one single group)
        // Since noop consolidates all files into 1 group, binpack will typically create >= 1 groups
        assert!(bin_pack_groups.len() >= noop_groups.len());

        // Verify BinPack behavior: should actually create multiple groups for size balancing
        // With files: 10MB + 20MB + 30MB + 40MB = 100MB total, and target 64MB,
        // BinPack should create at least 2 groups to balance sizes
        assert!(
            bin_pack_groups.len() >= 1,
            "BinPack should create at least 1 group"
        );

        // Verify all files are preserved across groups
        let total_binpack_files: usize = bin_pack_groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(
            total_binpack_files, 4,
            "BinPack should preserve all 4 files"
        );

        // Noop should create one single group containing all files
        assert_eq!(noop_groups.len(), 1);
        assert_eq!(noop_groups[0].data_file_count, 4);

        // Additional detailed BinPack behavior verification
        println!("BinPack created {} groups:", bin_pack_groups.len());
        for (i, group) in bin_pack_groups.iter().enumerate() {
            println!(
                "  Group {}: {} files, {} total bytes",
                i + 1,
                group.data_file_count,
                group.total_size
            );
        }

        // Verify BinPack behavior: For our test case (100MB total, 64MB target),
        // the algorithm calculates: 100/64 = 1.56 -> max(1, 1) = 1 group
        // So it should create exactly 1 group containing all files
        assert_eq!(
            bin_pack_groups.len(),
            1,
            "BinPack should create 1 group for this size distribution"
        );
        assert_eq!(
            bin_pack_groups[0].data_file_count, 4,
            "Single group should contain all 4 files"
        );
        assert_eq!(
            bin_pack_groups[0].total_size,
            100 * 1024 * 1024,
            "Total size should be 100MB"
        );

        // This demonstrates the key difference between Noop and BinPack:
        // - Noop: Always creates 1 group (simple consolidation)
        // - BinPack: Creates optimized number of groups based on size calculations
        // In this specific case, both create 1 group, but with different reasoning

        // Test with larger files to force BinPack to create multiple groups
        let large_test_files = vec![
            create_test_file_scan_task("large1.parquet", 50 * 1024 * 1024), // 50MB
            create_test_file_scan_task("large2.parquet", 50 * 1024 * 1024), // 50MB
            create_test_file_scan_task("large3.parquet", 50 * 1024 * 1024), // 50MB
            create_test_file_scan_task("large4.parquet", 50 * 1024 * 1024), // 50MB
        ]; // Total: 200MB

        let large_bin_pack_groups = bin_pack_strategy
            .execute(large_test_files.clone(), &config)
            .unwrap();
        let large_noop_groups = noop_strategy.execute(large_test_files, &config).unwrap();

        // With 200MB total and 64MB target: 200/64 = 3.125 -> max(1, 3) = 3 groups
        println!(
            "Large files - BinPack created {} groups",
            large_bin_pack_groups.len()
        );
        assert!(
            large_bin_pack_groups.len() >= 3,
            "BinPack should create multiple groups for 200MB total"
        );

        // Noop should still create only 1 group
        assert_eq!(
            large_noop_groups.len(),
            1,
            "Noop should always create 1 group"
        );
        assert_eq!(
            large_noop_groups[0].data_file_count, 4,
            "Noop group should contain all files"
        );

        // Now we can see the real difference between strategies
        assert!(
            large_bin_pack_groups.len() > large_noop_groups.len(),
            "BinPack should create more groups than Noop for large files"
        );
    }

    #[test]
    fn test_create_custom_strategy_boundary_values() {
        // Test edge cases and boundary values for create_custom_strategy

        // Test with zero and maximum values
        let boundary_strategy = FileStrategyFactory::create_custom_strategy(
            true,                                     // exclude_delete_files
            Some((Some(0), Some(u64::MAX))),          // size_filter: 0 to MAX
            1,                                        // min_file_count: 1 (minimal valid value)
            1,                                        // max_task_total_size: 1 byte (very small)
            crate::config::GroupingStrategy::BinPack, // grouping_strategy
            1,                                        // target_group_size: 1 byte
            1,                                        // max_files_per_group: 1 file max
            1,                                        // min_group_size: 1 byte
            1,                                        // max_group_size: 1 byte
            1,                                        // min_group_file_count: 1 file min
            1,                                        // max_group_file_count: 1 file max
        );

        let description = boundary_strategy.description();
        // Note: u64::MAX is very large, let's just check that SizeFilter is present with the expected range
        assert!(description.contains("SizeFilter[0-17592186044415MB]")); // This is u64::MAX in MB
        assert!(description.contains("MinFileCount[1]"));
        assert!(description.contains("TaskSizeLimit[0GB]")); // 1 byte rounds to 0 GB
        assert!(description.contains("MinGroupSize[0MB]")); // 1 byte rounds to 0 MB
        assert!(description.contains("MaxGroupSize[0MB]")); // 1 byte rounds to 0 MB
        assert!(description.contains("MinGroupFileCount[1]"));
        assert!(description.contains("MaxGroupFileCount[1]"));

        // Test functional behavior - very restrictive settings should filter out most files
        let test_files = vec![
            create_test_file_scan_task_with_deletes("tiny.parquet", 1, false), // 1 byte, no deletes
            create_test_file_scan_task_with_deletes("small.parquet", 1024, false), // 1KB, no deletes
        ];

        let result = execute_strategy_flat(&boundary_strategy, test_files);
        // Due to very restrictive max_task_total_size (1 byte), only the first file should pass
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data_file_path, "tiny.parquet");
    }

    #[test]
    fn test_binpack_grouping_with_zero_target_group_size() {
        // Regression test for BinPackGroupingStrategy performance issue
        // when target_group_size = 0
        let grouping_strategy = BinPackGroupingStrategy {
            target_group_size: 0, // This used to cause performance issues
            max_files_per_group: 100,
        };

        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("file2.parquet", 10 * 1024 * 1024), // 10MB
            create_test_file_scan_task("file3.parquet", 15 * 1024 * 1024), // 15MB
        ];

        // This should complete quickly (not create millions of groups)
        let start = std::time::Instant::now();
        let groups = grouping_strategy.group_files(data_files.into_iter());
        let duration = start.elapsed();

        // Should complete in reasonable time (< 1 second)
        assert!(
            duration.as_secs() < 1,
            "BinPack with target_group_size=0 should not take more than 1 second"
        );

        // When target_group_size=0, should use single group behavior
        assert_eq!(
            groups.len(),
            1,
            "Should create only one group when target_group_size=0"
        );
        assert_eq!(
            groups[0].data_file_count, 3,
            "Single group should contain all 3 files"
        );

        // Verify all files are included
        let file_names: Vec<&str> = groups[0]
            .data_files
            .iter()
            .map(|task| task.data_file_path.as_str())
            .collect();
        assert!(file_names.contains(&"file1.parquet"));
        assert!(file_names.contains(&"file2.parquet"));
        assert!(file_names.contains(&"file3.parquet"));
    }

    #[test]
    fn test_noop_grouping_strategy() {
        let grouping_strategy = NoopGroupingStrategy;

        // Test with multiple files
        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("file2.parquet", 10 * 1024 * 1024), // 10MB
            create_test_file_scan_task("file3.parquet", 15 * 1024 * 1024), // 15MB
        ];

        let groups = grouping_strategy.group_files(data_files.into_iter());

        // NoopGrouping should create one single group containing all files
        assert_eq!(
            groups.len(),
            1,
            "NoopGrouping should create one single group containing all files"
        );

        // The single group should contain all files in the same order
        let group = &groups[0];
        assert_eq!(
            group.data_file_count, 3,
            "The single group should contain all 3 files"
        );
        assert_eq!(group.data_files[0].data_file_path, "file1.parquet");
        assert_eq!(group.data_files[1].data_file_path, "file2.parquet");
        assert_eq!(group.data_files[2].data_file_path, "file3.parquet");

        // Test with single file
        let single_file = vec![
            create_test_file_scan_task("single.parquet", 20 * 1024 * 1024), // 20MB
        ];

        let single_groups = grouping_strategy.group_files(single_file.into_iter());
        assert_eq!(
            single_groups.len(),
            1,
            "NoopGrouping should create one group for single file"
        );
        assert_eq!(single_groups[0].data_file_count, 1);
        assert_eq!(
            single_groups[0].data_files[0].data_file_path,
            "single.parquet"
        );

        // Test with empty file list
        let empty_files: Vec<FileScanTask> = vec![];
        let empty_groups = grouping_strategy.group_files(empty_files.into_iter());
        assert_eq!(
            empty_groups.len(),
            0,
            "NoopGrouping should return empty list for empty input"
        );

        // Test description
        assert_eq!(grouping_strategy.description(), "NoopGrouping");
    }

    #[test]
    fn test_noop_grouping_vs_binpack_comparison() {
        // Test to ensure NoopGrouping behavior is distinctly different from BinPack
        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 1 * 1024 * 1024), // 1MB
            create_test_file_scan_task("file2.parquet", 1 * 1024 * 1024), // 1MB
            create_test_file_scan_task("file3.parquet", 1 * 1024 * 1024), // 1MB
            create_test_file_scan_task("file4.parquet", 1 * 1024 * 1024), // 1MB
        ];

        // NoopGrouping: All files in one group
        let noop_strategy = NoopGroupingStrategy;
        let noop_groups = noop_strategy.group_files(data_files.clone().into_iter());
        assert_eq!(noop_groups.len(), 1, "Noop should create exactly 1 group");
        assert_eq!(
            noop_groups[0].data_file_count, 4,
            "Noop group should contain all 4 files"
        );

        // BinPack with small target: Multiple groups
        let binpack_strategy = BinPackGroupingStrategy {
            target_group_size: 2 * 1024 * 1024, // 2MB target
            max_files_per_group: 100,
        };
        let binpack_groups = binpack_strategy.group_files(data_files.into_iter());

        // BinPack should create multiple groups due to size constraints
        // Total size: 4MB, target: 2MB -> should create ~2 groups
        assert!(
            binpack_groups.len() >= 2,
            "BinPack should create multiple groups for small target size"
        );

        // Verify total files are preserved
        let total_noop_files: usize = noop_groups.iter().map(|g| g.data_file_count).sum();
        let total_binpack_files: usize = binpack_groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_noop_files, 4);
        assert_eq!(total_binpack_files, 4);

        // Key difference: Noop consolidates, BinPack may distribute
        assert!(noop_groups.len() <= binpack_groups.len(),
                "Noop should create fewer or equal groups compared to BinPack with restrictive settings");
    }

    #[test]
    fn test_binpack_grouping_strategy_normal_cases() {
        // Test BinPack with normal target_group_size values
        let grouping_strategy = BinPackGroupingStrategy {
            target_group_size: 20 * 1024 * 1024, // 20MB target
            max_files_per_group: 100,
        };

        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("file2.parquet", 10 * 1024 * 1024), // 10MB
            create_test_file_scan_task("file3.parquet", 15 * 1024 * 1024), // 15MB
            create_test_file_scan_task("file4.parquet", 8 * 1024 * 1024), // 8MB
        ];

        let total_size = 5 + 10 + 15 + 8; // 38MB total
        let expected_groups = (total_size / 20).max(1); // Should be 1-2 groups

        let groups = grouping_strategy.group_files(data_files.into_iter());

        assert!(
            groups.len() <= expected_groups as usize + 1,
            "Should create reasonable number of groups"
        );
        assert!(groups.len() >= 1, "Should create at least one group");

        // Verify all files are included
        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 4, "All files should be included in groups");

        // Test description
        assert_eq!(
            grouping_strategy.description(),
            "BinPackGrouping[target=20MB, max_files=100]"
        );
    }

    #[test]
    fn test_binpack_grouping_with_max_files_constraint() {
        // Test BinPack with max_files_per_group constraint
        let grouping_strategy = BinPackGroupingStrategy {
            target_group_size: 1024 * 1024 * 1024, // 1GB target (very large)
            max_files_per_group: 2,                // Only 2 files per group
        };

        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 1 * 1024 * 1024), // 1MB
            create_test_file_scan_task("file2.parquet", 1 * 1024 * 1024), // 1MB
            create_test_file_scan_task("file3.parquet", 1 * 1024 * 1024), // 1MB
            create_test_file_scan_task("file4.parquet", 1 * 1024 * 1024), // 1MB
            create_test_file_scan_task("file5.parquet", 1 * 1024 * 1024), // 1MB
        ];

        let groups = grouping_strategy.group_files(data_files.into_iter());

        // With 5 files and max 2 files per group, should create at least 2 groups
        // (actual algorithm does files.len() / max_files_per_group = 5/2 = 2)
        assert!(
            groups.len() >= 2,
            "Should create at least 2 groups due to max_files_per_group constraint"
        );

        // Verify all files are included
        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 5, "All files should be included in groups");

        // Verify the algorithm behavior: with target_group_size very large and 5 files,
        // split_num should be max(1, 2) = 2 groups due to max_files_per_group constraint
        assert_eq!(
            groups.len(),
            2,
            "Should create exactly 2 groups based on algorithm logic"
        );

        // One group should have 3 files, another should have 2 files (5 files distributed across 2 groups)
        let mut group_sizes: Vec<usize> = groups.iter().map(|g| g.data_file_count).collect();
        group_sizes.sort();
        assert_eq!(
            group_sizes,
            vec![2, 3],
            "Groups should have 2 and 3 files respectively"
        );
    }

    #[test]
    fn test_grouping_strategy_enum() {
        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("file2.parquet", 10 * 1024 * 1024), // 10MB
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
        assert_eq!(noop_enum.description(), "NoopGrouping");

        // Test Noop variant with empty files
        let empty_files: Vec<FileScanTask> = vec![];
        let noop_empty_groups = noop_enum.group_files(empty_files.into_iter());
        assert_eq!(
            noop_empty_groups.len(),
            0,
            "Noop should return empty for empty input"
        );

        // Test BinPack variant
        let binpack_enum = GroupingStrategyEnum::BinPack(BinPackGroupingStrategy {
            target_group_size: 20 * 1024 * 1024,
            max_files_per_group: 100,
        });
        let binpack_groups = binpack_enum.group_files(data_files.into_iter());
        assert!(
            binpack_groups.len() >= 1,
            "BinPack should create at least one group"
        );
        assert_eq!(
            binpack_enum.description(),
            "BinPackGrouping[target=20MB, max_files=100]"
        );
    }

    #[test]
    fn test_strategy_edge_cases() {
        // Combined test for edge cases across different filter strategies

        // Size filter edge cases - Test min_size only
        let min_only_strategy = SizeFilterStrategy {
            min_size: Some(10 * 1024 * 1024), // 10MB min
            max_size: None,
        };
        assert_eq!(min_only_strategy.description(), "SizeFilter[>10MB]");

        let data_files = vec![
            create_test_file_scan_task("small.parquet", 5 * 1024 * 1024), // 5MB - should be filtered out
            create_test_file_scan_task("medium.parquet", 15 * 1024 * 1024), // 15MB - should pass
            create_test_file_scan_task("large.parquet", 100 * 1024 * 1024), // 100MB - should pass
        ];

        let result = min_only_strategy.filter(data_files);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "medium.parquet");
        assert_eq!(result[1].data_file_path, "large.parquet");

        // Test max_size only
        let max_only_strategy = SizeFilterStrategy {
            min_size: None,
            max_size: Some(50 * 1024 * 1024), // 50MB max
        };
        assert_eq!(max_only_strategy.description(), "SizeFilter[<50MB]");

        let data_files2 = vec![
            create_test_file_scan_task("small.parquet", 5 * 1024 * 1024), // 5MB - should pass
            create_test_file_scan_task("medium.parquet", 30 * 1024 * 1024), // 30MB - should pass
            create_test_file_scan_task("large.parquet", 100 * 1024 * 1024), // 100MB - should be filtered out
        ];

        let result2 = max_only_strategy.filter(data_files2);
        assert_eq!(result2.len(), 2);
        assert_eq!(result2[0].data_file_path, "small.parquet");
        assert_eq!(result2[1].data_file_path, "medium.parquet");

        // Test no constraints (both None)
        let no_filter_strategy = SizeFilterStrategy {
            min_size: None,
            max_size: None,
        };
        assert_eq!(no_filter_strategy.description(), "SizeFilter[Any]");

        let data_files3 = vec![
            create_test_file_scan_task("file1.parquet", 1 * 1024), // 1KB - should pass
            create_test_file_scan_task("file2.parquet", 1024 * 1024 * 1024), // 1GB - should pass
        ];

        let result3 = no_filter_strategy.filter(data_files3);
        assert_eq!(result3.len(), 2); // All files should pass

        // Task size limit edge cases - Test with exact limit
        let strategy = TaskSizeLimitStrategy {
            max_total_size: 20 * 1024 * 1024, // 20MB total limit
        };

        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024), // 10MB - should pass (total: 10MB)
            create_test_file_scan_task("file2.parquet", 10 * 1024 * 1024), // 10MB - should pass (total: 20MB exactly)
            create_test_file_scan_task("file3.parquet", 1), // 1 byte - should be filtered out (would exceed 20MB)
        ];

        let result = strategy.filter(data_files);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "file1.parquet");
        assert_eq!(result[1].data_file_path, "file2.parquet");

        // Test with very small limit
        let small_limit_strategy = TaskSizeLimitStrategy {
            max_total_size: 5 * 1024 * 1024, // 5MB total limit
        };

        let large_files = vec![
            create_test_file_scan_task("large1.parquet", 10 * 1024 * 1024), // 10MB - should be filtered out
            create_test_file_scan_task("large2.parquet", 10 * 1024 * 1024), // 10MB - should be filtered out
        ];

        let result_small = small_limit_strategy.filter(large_files);
        assert_eq!(result_small.len(), 0); // No files should pass

        // Test description formatting
        assert_eq!(strategy.description(), "TaskSizeLimit[0GB]"); // 20MB rounds down to 0GB

        let gb_strategy = TaskSizeLimitStrategy {
            max_total_size: 2 * 1024 * 1024 * 1024, // 2GB
        };
        assert_eq!(gb_strategy.description(), "TaskSizeLimit[2GB]");
    }
}
