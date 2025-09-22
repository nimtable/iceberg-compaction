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
        let total_size = data_files.iter().map(|task| task.length).sum();
        let data_file_count = data_files.len();

        // Extract delete files from data files (similar to build_input_file_scan_tasks logic)
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
                    _ => {
                        // Skip other types
                    }
                }
            }
        }

        // Convert maps to vectors
        let position_delete_files = position_delete_map.into_values().collect();
        let equality_delete_files = equality_delete_map.into_values().collect();

        Self {
            data_files,
            position_delete_files,
            equality_delete_files,
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

/// `BinPack` grouping strategy that optimizes file size distribution
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
            let max_possible_groups = files.len().div_ceil(self.max_files_per_group.max(1)); // Ceiling division
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

/// Configuration for creating custom file selection strategies
#[derive(Debug, Clone)]
pub struct CustomStrategyConfig {
    pub exclude_delete_files: bool,
    pub size_filter: Option<(Option<u64>, Option<u64>)>, // (min_size, max_size)
    pub min_file_count: usize,
    pub max_task_total_size: u64,
    pub grouping_strategy: GroupingStrategy,
    pub target_group_size: u64,
    pub max_files_per_group: usize,
    pub min_group_size: u64,
    pub max_group_size: u64,
    pub min_group_file_count: usize,
    pub max_group_file_count: usize,
}

impl Default for CustomStrategyConfig {
    fn default() -> Self {
        Self {
            exclude_delete_files: false,
            size_filter: None,
            min_file_count: 0,
            max_task_total_size: u64::MAX,
            grouping_strategy: GroupingStrategy::Noop,
            target_group_size: 64 * 1024 * 1024, // 64MB
            max_files_per_group: 100,
            min_group_size: 0,
            max_group_size: u64::MAX,
            min_group_file_count: 0,
            max_group_file_count: usize::MAX,
        }
    }
}

impl CustomStrategyConfig {
    pub fn builder() -> CustomStrategyConfigBuilder {
        CustomStrategyConfigBuilder::default()
    }
}

#[derive(Debug, Default)]
pub struct CustomStrategyConfigBuilder {
    config: CustomStrategyConfig,
}

impl CustomStrategyConfigBuilder {
    pub fn exclude_delete_files(mut self, exclude: bool) -> Self {
        self.config.exclude_delete_files = exclude;
        self
    }

    pub fn size_filter(mut self, min_size: Option<u64>, max_size: Option<u64>) -> Self {
        self.config.size_filter = Some((min_size, max_size));
        self
    }

    pub fn min_file_count(mut self, count: usize) -> Self {
        self.config.min_file_count = count;
        self
    }

    pub fn max_task_total_size(mut self, size: u64) -> Self {
        self.config.max_task_total_size = size;
        self
    }

    pub fn grouping_strategy(mut self, strategy: GroupingStrategy) -> Self {
        self.config.grouping_strategy = strategy;
        self
    }

    pub fn target_group_size(mut self, size: u64) -> Self {
        self.config.target_group_size = size;
        self
    }

    pub fn max_files_per_group(mut self, count: usize) -> Self {
        self.config.max_files_per_group = count;
        self
    }

    pub fn min_group_size(mut self, size: u64) -> Self {
        self.config.min_group_size = size;
        self
    }

    pub fn max_group_size(mut self, size: u64) -> Self {
        self.config.max_group_size = size;
        self
    }

    pub fn min_group_file_count(mut self, count: usize) -> Self {
        self.config.min_group_file_count = count;
        self
    }

    pub fn max_group_file_count(mut self, count: usize) -> Self {
        self.config.max_group_file_count = count;
        self
    }

    pub fn build(self) -> CustomStrategyConfig {
        self.config
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
            "NoFileFilters".to_owned()
        } else {
            self.file_filters
                .iter()
                .map(|f| f.description())
                .collect::<Vec<_>>()
                .join(" -> ")
        };

        let group_filter_desc = if self.group_filters.is_empty() {
            "NoGroupFilters".to_owned()
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
        let file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![
            Box::new(NoDeleteFilesStrategy),
            Box::new(SizeFilterStrategy {
                min_size: None,
                max_size: Some(config.small_file_threshold),
            }),
            Box::new(MinFileCountStrategy::new(config.min_file_count)),
            Box::new(TaskSizeLimitStrategy {
                max_total_size: config.max_task_total_size,
            }),
        ];

        // Grouping layer
        let grouping = match config.grouping_strategy {
            GroupingStrategy::Noop => GroupingStrategyEnum::Noop(NoopGroupingStrategy),
            GroupingStrategy::BinPack => GroupingStrategyEnum::BinPack(BinPackGroupingStrategy {
                target_group_size: config.max_group_size,
                max_files_per_group: config.max_group_file_count,
            }),
        };

        // Group filtering layer - for small files, use more permissive settings
        let group_filters: Vec<Box<dyn GroupFilterStrategy>> = vec![
            // For small files, allow very small groups (any size above 0)
            Box::new(MinGroupSizeStrategy { min_group_size: 0 }),
            Box::new(MaxGroupSizeStrategy {
                max_group_size: config.max_group_size,
            }),
            // For small files, allow single-file groups
            Box::new(MinGroupFileCountStrategy { min_file_count: 1 }),
            Box::new(MaxGroupFileCountStrategy {
                max_file_count: config.max_group_file_count,
            }),
        ];

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

    /// Create a custom strategy with structured configuration
    ///
    /// This method provides a more maintainable way to create custom strategies
    /// using a builder pattern for configuration.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use iceberg_compaction_core::file_selection::strategy::{FileStrategyFactory, CustomStrategyConfig};
    /// # use iceberg_compaction_core::config::GroupingStrategy;
    /// let config = CustomStrategyConfig::builder()
    ///     .exclude_delete_files(true)
    ///     .size_filter(None, Some(100 * 1024 * 1024)) // max 100MB
    ///     .min_file_count(3)
    ///     .grouping_strategy(GroupingStrategy::BinPack)
    ///     .target_group_size(64 * 1024 * 1024) // 64MB
    ///     .build();
    /// let strategy = FileStrategyFactory::create_custom_strategy_v2(config);
    /// ```
    pub fn create_custom_strategy_v2(config: CustomStrategyConfig) -> ThreeLayerStrategy {
        Self::create_custom_strategy(
            config.exclude_delete_files,
            config.size_filter,
            config.min_file_count,
            config.max_task_total_size,
            config.grouping_strategy,
            config.target_group_size,
            config.max_files_per_group,
            config.min_group_size,
            config.max_group_size,
            config.min_group_file_count,
            config.max_group_file_count,
        )
    }

    /// Create a custom strategy with flexible configuration
    ///
    /// This method provides fine-grained control over which filters to include.
    ///
    /// **Note**: Consider using `create_custom_strategy_v2` with `CustomStrategyConfig`
    /// for better maintainability with complex configurations.
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
    #[allow(clippy::too_many_arguments)]
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
            strategy: &ThreeLayerStrategy,
            data_files: Vec<FileScanTask>,
        ) -> Vec<FileScanTask> {
            let config = CompactionPlanningConfigBuilder::default().build().unwrap();

            strategy
                .execute(data_files, &config)
                .unwrap()
                .into_iter()
                .flat_map(|group| group.into_files())
                .collect()
        }

        /// Create test config with common defaults
        pub fn create_test_config() -> CompactionPlanningConfig {
            CompactionPlanningConfigBuilder::default().build().unwrap()
        }
    }

    // Helper function to create test FileScanTask
    fn create_test_file_scan_task(file_path: &str, file_size: u64) -> FileScanTask {
        TestFileBuilder::new(file_path).size(file_size).build()
    }

    // Helper function to create test FileScanTask with delete files
    fn create_test_file_scan_task_with_deletes(
        file_path: &str,
        file_size: u64,
        has_deletes: bool,
    ) -> FileScanTask {
        let mut builder = TestFileBuilder::new(file_path).size(file_size);
        if has_deletes {
            builder = builder.with_deletes();
        }
        builder.build()
    }

    // Helper function to execute strategy and return flattened files for testing
    fn execute_strategy_flat(
        strategy: &ThreeLayerStrategy,
        data_files: Vec<FileScanTask>,
    ) -> Vec<FileScanTask> {
        TestUtils::execute_strategy_flat(strategy, data_files)
    }

    // Helper function to create default config for testing
    fn create_test_config() -> crate::config::CompactionPlanningConfig {
        TestUtils::create_test_config()
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

        // Test filtering logic with comprehensive validation
        let test_files = vec![
            create_test_file_scan_task("too_small.parquet", 2 * 1024 * 1024), // 2MB - should be filtered
            create_test_file_scan_task("min_edge.parquet", 5 * 1024 * 1024), // 5MB - should pass (exactly min)
            create_test_file_scan_task("medium1.parquet", 10 * 1024 * 1024), // 10MB - should pass
            create_test_file_scan_task("medium2.parquet", 30 * 1024 * 1024), // 30MB - should pass
            create_test_file_scan_task("max_edge.parquet", 50 * 1024 * 1024), // 50MB - should pass (exactly max)
            create_test_file_scan_task("too_large.parquet", 100 * 1024 * 1024), // 100MB - should be filtered
        ];

        let result: Vec<FileScanTask> = strategy.filter(test_files);

        // Verify correct files passed filter
        assert_eq!(
            result.len(),
            4,
            "Should pass exactly 4 files (5MB to 50MB inclusive)"
        );

        let expected_files = [
            "min_edge.parquet",
            "medium1.parquet",
            "medium2.parquet",
            "max_edge.parquet",
        ];
        for (i, expected_name) in expected_files.iter().enumerate() {
            assert_eq!(
                result[i].data_file_path, *expected_name,
                "File {} should be {}",
                i, expected_name
            );
        }

        // Verify all passed files meet size criteria
        for file in &result {
            assert!(
                file.length >= 5 * 1024 * 1024,
                "File {} size {} should be >= 5MB",
                file.data_file_path,
                file.length
            );
            assert!(
                file.length <= 50 * 1024 * 1024,
                "File {} size {} should be <= 50MB",
                file.data_file_path,
                file.length
            );
        }

        // Test edge case: exactly matching min and max boundaries
        let boundary_files = vec![
            create_test_file_scan_task("exact_min.parquet", 5 * 1024 * 1024), // Exactly 5MB
            create_test_file_scan_task("exact_max.parquet", 50 * 1024 * 1024), // Exactly 50MB
        ];

        let boundary_result = strategy.filter(boundary_files);
        assert_eq!(
            boundary_result.len(),
            2,
            "Boundary values should be inclusive"
        );
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

        assert_eq!(strategy.description(), "TaskSizeLimit[0GB]"); // 25MB rounds down to 0GB

        // Test cumulative size limiting with precise validation
        let test_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024), // 10MB - should pass (total: 10MB)
            create_test_file_scan_task("file2.parquet", 10 * 1024 * 1024), // 10MB - should pass (total: 20MB)
            create_test_file_scan_task("file3.parquet", 5 * 1024 * 1024), // 5MB - should pass (total: 25MB exactly)
            create_test_file_scan_task("file4.parquet", 1), // 1 byte - should be filtered (would exceed)
            create_test_file_scan_task("file5.parquet", 5 * 1024 * 1024), // 5MB - should be filtered
        ];

        let result: Vec<FileScanTask> = strategy.filter(test_files);

        assert_eq!(
            result.len(),
            3,
            "Should pass first 3 files totaling exactly 25MB"
        );
        assert_eq!(result[0].data_file_path, "file1.parquet");
        assert_eq!(result[1].data_file_path, "file2.parquet");
        assert_eq!(result[2].data_file_path, "file3.parquet");

        // Verify cumulative size constraint
        let total_size: u64 = result.iter().map(|f| f.length).sum();
        assert_eq!(
            total_size,
            25 * 1024 * 1024,
            "Total size should be exactly 25MB"
        );
        assert!(
            total_size <= strategy.max_total_size,
            "Total should not exceed limit"
        );

        // Test with files that individually exceed limit
        let large_limit_strategy = TaskSizeLimitStrategy {
            max_total_size: 5 * 1024 * 1024, // 5MB limit
        };

        let large_files = vec![
            create_test_file_scan_task("huge1.parquet", 10 * 1024 * 1024), // 10MB - exceeds limit alone
            create_test_file_scan_task("huge2.parquet", 10 * 1024 * 1024), // 10MB - would also exceed
        ];

        let large_result: Vec<FileScanTask> = large_limit_strategy.filter(large_files);
        assert_eq!(
            large_result.len(),
            0,
            "Should filter all files when each exceeds limit"
        );

        // Test with exact boundary
        let boundary_files = vec![
            create_test_file_scan_task("exact.parquet", 5 * 1024 * 1024), // Exactly 5MB
        ];

        let boundary_result = large_limit_strategy.filter(boundary_files);
        assert_eq!(
            boundary_result.len(),
            1,
            "Should pass file that exactly matches limit"
        );
        assert_eq!(boundary_result[0].length, 5 * 1024 * 1024);
    }

    #[test]
    fn test_min_file_count_strategy() {
        let strategy = MinFileCountStrategy::new(3);

        // Test description
        assert_eq!(strategy.description(), "MinFileCount[3]");

        // Test with exactly minimum files (3) - should pass all
        let exact_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
            create_test_file_scan_task("file3.parquet", 15 * 1024 * 1024),
        ];

        let exact_result: Vec<FileScanTask> = strategy.filter(exact_files.clone());
        assert_eq!(
            exact_result.len(),
            3,
            "Should pass all files when exactly meeting minimum"
        );
        // Verify order preserved
        for (i, file) in exact_result.iter().enumerate() {
            assert_eq!(file.data_file_path, exact_files[i].data_file_path);
        }

        // Test with more than minimum files (5 > 3) - should pass all
        let more_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
            create_test_file_scan_task("file3.parquet", 15 * 1024 * 1024),
            create_test_file_scan_task("file4.parquet", 25 * 1024 * 1024),
            create_test_file_scan_task("file5.parquet", 30 * 1024 * 1024),
        ];

        let more_result: Vec<FileScanTask> = strategy.filter(more_files);
        assert_eq!(
            more_result.len(),
            5,
            "Should pass all files when exceeding minimum"
        );

        // Test with fewer than minimum files (2 < 3) - should pass none
        let fewer_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
        ];

        let fewer_result: Vec<FileScanTask> = strategy.filter(fewer_files);
        assert_eq!(
            fewer_result.len(),
            0,
            "Should pass no files when below minimum"
        );

        // Test edge cases: min_count = 0 (bypass - should always pass)
        let zero_strategy = MinFileCountStrategy::new(0);
        assert_eq!(zero_strategy.description(), "MinFileCount[0]");

        let single_file = vec![create_test_file_scan_task(
            "single.parquet",
            10 * 1024 * 1024,
        )];
        let zero_result: Vec<FileScanTask> = zero_strategy.filter(single_file.clone());
        assert_eq!(zero_result.len(), 1, "min_count=0 should always pass files");
        assert_eq!(zero_result[0].data_file_path, "single.parquet");

        // Test edge case: min_count = 1 (minimal threshold)
        let one_strategy = MinFileCountStrategy::new(1);
        let one_result: Vec<FileScanTask> = one_strategy.filter(single_file);
        assert_eq!(one_result.len(), 1, "min_count=1 should pass single file");
    }

    #[test]
    fn test_small_files_strategy_comprehensive() {
        // Comprehensive test for small files strategy with various configurations

        // Test basic small files strategy
        let config = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(20 * 1024 * 1024) // 20MB threshold
            .max_task_total_size(50 * 1024 * 1024) // 50MB task limit
            .min_group_size(0)
            .min_group_file_count(1)
            .build()
            .unwrap();

        let strategy = FileStrategyFactory::create_small_files_strategy(&config);

        let test_files = vec![
            create_test_file_scan_task_with_deletes("small1.parquet", 5 * 1024 * 1024, false), // 5MB, no deletes
            create_test_file_scan_task_with_deletes("small2.parquet", 15 * 1024 * 1024, true), // 15MB, has deletes - filtered
            create_test_file_scan_task_with_deletes("small3.parquet", 10 * 1024 * 1024, false), // 10MB, no deletes
            create_test_file_scan_task_with_deletes("large.parquet", 25 * 1024 * 1024, false), // 25MB - filtered (exceeds threshold)
        ];

        let result = execute_strategy_flat(&strategy, test_files);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "small1.parquet");
        assert_eq!(result[1].data_file_path, "small3.parquet");

        // Verify all selected files meet criteria
        for file in &result {
            assert!(file.length <= config.small_file_threshold);
            assert!(file.deletes.is_empty());
        }

        // Test min_file_count behavior with different scenarios
        let min_count_config = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(20 * 1024 * 1024)
            .min_file_count(3) // Require at least 3 files
            .grouping_strategy(crate::config::GroupingStrategy::Noop)
            .min_group_size(0)
            .min_group_file_count(1)
            .build()
            .unwrap();

        let min_count_strategy =
            FileStrategyFactory::create_small_files_strategy(&min_count_config);

        // Test description includes min_file_count
        assert!(min_count_strategy.description().contains("MinFileCount[3]"));

        // Test with insufficient files (2 < 3)
        let insufficient_files = vec![
            create_test_file_scan_task_with_deletes("small1.parquet", 5 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small2.parquet", 10 * 1024 * 1024, false),
        ];
        let insufficient_result = execute_strategy_flat(&min_count_strategy, insufficient_files);
        assert_eq!(
            insufficient_result.len(),
            0,
            "Should return no files when not meeting minimum count"
        );

        // Test with sufficient files (3 >= 3)
        let sufficient_files = vec![
            create_test_file_scan_task_with_deletes("small1.parquet", 5 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small2.parquet", 10 * 1024 * 1024, false),
            create_test_file_scan_task_with_deletes("small3.parquet", 15 * 1024 * 1024, false),
        ];
        let sufficient_result = execute_strategy_flat(&min_count_strategy, sufficient_files);
        assert_eq!(
            sufficient_result.len(),
            3,
            "Should return all files when meeting minimum count"
        );

        // Test with min_file_count = 0 (bypass mode)
        let bypass_config = CompactionPlanningConfigBuilder::default()
            .min_file_count(0)
            .small_file_threshold(20 * 1024 * 1024)
            .grouping_strategy(crate::config::GroupingStrategy::Noop)
            .min_group_size(0)
            .min_group_file_count(1)
            .build()
            .unwrap();

        let bypass_strategy = FileStrategyFactory::create_small_files_strategy(&bypass_config);
        assert!(bypass_strategy.description().contains("MinFileCount[0]"));

        let single_file = vec![create_test_file_scan_task_with_deletes(
            "single.parquet",
            5 * 1024 * 1024,
            false,
        )];
        let bypass_result = execute_strategy_flat(&bypass_strategy, single_file);
        assert_eq!(
            bypass_result.len(),
            1,
            "Should accept single file with min_file_count=0"
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
    fn test_grouping_strategies() {
        // Test different grouping strategies comprehensively

        // Test BinPack with 64MB target
        let bin_pack_strategy = FileStrategyFactory::create_custom_strategy(
            false,
            None,
            0,
            u64::MAX,
            crate::config::GroupingStrategy::BinPack,
            64 * 1024 * 1024,
            100, // 64MB target, 100 max files per group
            0,
            u64::MAX,
            0,
            usize::MAX,
        );

        assert!(bin_pack_strategy
            .description()
            .contains("BinPackGrouping[target=64MB, max_files=100]"));

        // Test with files that fit in one group (60MB < 64MB target)
        let small_files = vec![
            create_test_file_scan_task("file1.parquet", 20 * 1024 * 1024), // 20MB
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024), // 20MB
            create_test_file_scan_task("file3.parquet", 20 * 1024 * 1024), // 20MB
        ]; // Total: 60MB

        let config = create_test_config();
        let small_groups = bin_pack_strategy.execute(small_files, &config).unwrap();

        assert_eq!(
            small_groups.len(),
            1,
            "Should create 1 group for 60MB with 64MB target"
        );
        assert_eq!(
            small_groups[0].data_file_count, 3,
            "Group should contain all 3 files"
        );
        assert_eq!(
            small_groups[0].total_size,
            60 * 1024 * 1024,
            "Total size should be 60MB"
        );

        // Test with files that exceed target (200MB > 64MB)
        let large_files = vec![
            create_test_file_scan_task("large1.parquet", 50 * 1024 * 1024), // 50MB
            create_test_file_scan_task("large2.parquet", 50 * 1024 * 1024), // 50MB
            create_test_file_scan_task("large3.parquet", 50 * 1024 * 1024), // 50MB
            create_test_file_scan_task("large4.parquet", 50 * 1024 * 1024), // 50MB
        ]; // Total: 200MB

        let large_groups = bin_pack_strategy.execute(large_files, &config).unwrap();

        // BinPack algorithm: 200MB / 64MB = 3.125 -> floor = 3 groups
        assert!(
            large_groups.len() >= 3,
            "Should create at least 3 groups for 200MB with 64MB target, got: {}",
            large_groups.len()
        );

        // Verify all files preserved
        let total_files: usize = large_groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 4, "All 4 files should be preserved");

        // Test max_files_per_group constraint
        let max_files_strategy = FileStrategyFactory::create_custom_strategy(
            false,
            None,
            0,
            u64::MAX,
            crate::config::GroupingStrategy::BinPack,
            1024 * 1024 * 1024,
            2, // 1GB target (very large), 2 max files per group
            0,
            u64::MAX,
            0,
            usize::MAX,
        );

        let many_files = vec![
            create_test_file_scan_task("f1.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("f2.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("f3.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("f4.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("f5.parquet", 5 * 1024 * 1024), // 5MB
        ]; // Total: 25MB << 1GB

        let max_files_groups = max_files_strategy.execute(many_files, &config).unwrap();

        // With ceiling division: ceil(5/2) = 3 groups to respect max_files_per_group=2
        assert_eq!(
            max_files_groups.len(),
            3,
            "Should create 3 groups with max_files_per_group=2"
        );

        // Verify constraint respected
        for (i, group) in max_files_groups.iter().enumerate() {
            assert!(
                group.data_file_count <= 2,
                "Group {} has {} files, should be <= 2",
                i + 1,
                group.data_file_count
            );
        }

        // Test Noop grouping (always creates single group)
        let noop_strategy = FileStrategyFactory::create_custom_strategy(
            false,
            None,
            0,
            u64::MAX,
            crate::config::GroupingStrategy::Noop,
            0,
            0,
            0,
            u64::MAX,
            0,
            usize::MAX,
        );

        assert!(noop_strategy.description().contains("NoopGrouping"));

        let noop_files = vec![
            create_test_file_scan_task("n1.parquet", 100 * 1024 * 1024), // 100MB
            create_test_file_scan_task("n2.parquet", 100 * 1024 * 1024), // 100MB
        ];

        let noop_groups = noop_strategy.execute(noop_files, &config).unwrap();
        assert_eq!(noop_groups.len(), 1, "Noop should always create 1 group");
        assert_eq!(
            noop_groups[0].data_file_count, 2,
            "Noop group should contain all files"
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
    fn test_binpack_grouping_strategy_normal_cases() {
        // Test BinPack with normal target_group_size values and strict validation
        let grouping_strategy = BinPackGroupingStrategy {
            target_group_size: 20 * 1024 * 1024, // 20MB target
            max_files_per_group: 100,
        };

        // Test Case 1: Small files that should fit in one group (38MB < 2*20MB)
        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("file2.parquet", 10 * 1024 * 1024), // 10MB
            create_test_file_scan_task("file3.parquet", 15 * 1024 * 1024), // 15MB
            create_test_file_scan_task("file4.parquet", 8 * 1024 * 1024), // 8MB
        ];

        let total_size = 38 * 1024 * 1024; // 38MB total
        let groups = grouping_strategy.group_files(data_files.into_iter());

        // Algorithm calculates split_num = max(1, total_size / target_size) = max(1, 38/20) = max(1, 1) = 1
        // But also considers max_files_per_group: max_possible_groups = ceil(4/100) = 1
        // Final split_num = max(1, 1) = 1 group
        assert_eq!(
            groups.len(),
            1,
            "Should create exactly 1 group: total_size(38MB) / target_size(20MB) = 1.9 -> floor = 1"
        );

        // Verify the single group contains all files
        assert_eq!(
            groups[0].data_file_count, 4,
            "Single group should contain all 4 files"
        );

        // Verify total size is preserved
        assert_eq!(
            groups[0].total_size, total_size,
            "Total size should be preserved: 38MB"
        );

        // Verify max_files_per_group constraint is respected
        assert!(
            groups[0].data_file_count <= 100,
            "Group has {} files, should be <= 100",
            groups[0].data_file_count
        );

        // Test Case 2: Large files that should trigger multiple groups (75MB > 3*20MB)
        let large_files = vec![
            create_test_file_scan_task("large1.parquet", 25 * 1024 * 1024), // 25MB
            create_test_file_scan_task("large2.parquet", 25 * 1024 * 1024), // 25MB
            create_test_file_scan_task("large3.parquet", 25 * 1024 * 1024), // 25MB
        ]; // Total: 75MB

        let large_groups = grouping_strategy.group_files(large_files.into_iter());

        // Algorithm: split_num = max(1, 75MB / 20MB) = max(1, 3) = 3 groups
        // max_possible_groups = ceil(3/100) = 1, final = max(3, 1) = 3
        assert_eq!(
            large_groups.len(),
            3,
            "Should create exactly 3 groups: 75MB / 20MB = 3.75 -> floor = 3"
        );

        // Verify all files preserved in large test
        let total_large_files: usize = large_groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(
            total_large_files, 3,
            "All 3 large files should be preserved"
        );

        // Verify each group in large test respects constraints
        for (i, group) in large_groups.iter().enumerate() {
            assert!(
                group.data_file_count <= 100,
                "Group {} has {} files, should be <= 100",
                i + 1,
                group.data_file_count
            );
            assert!(
                group.data_file_count > 0,
                "Group {} should have at least 1 file",
                i + 1
            );

            // Verify BinPack distribution: with 3 files and 3 groups, should be [1,1,1]
            assert_eq!(
                group.data_file_count, 1,
                "Each of the 3 groups should contain exactly 1 file (3 files / 3 groups)"
            );
        }

        // Test Case 3: Boundary case - exactly 2x target size (40MB = 2*20MB)
        let boundary_files = vec![
            create_test_file_scan_task("boundary1.parquet", 20 * 1024 * 1024), // 20MB
            create_test_file_scan_task("boundary2.parquet", 20 * 1024 * 1024), // 20MB
        ]; // Total: 40MB

        let boundary_groups = grouping_strategy.group_files(boundary_files.into_iter());

        // Algorithm: 40MB / 20MB = 2.0 -> floor = 2 groups
        assert_eq!(
            boundary_groups.len(),
            2,
            "Should create exactly 2 groups: 40MB / 20MB = 2.0 -> floor = 2"
        );

        // Verify boundary case file distribution
        let boundary_file_counts: Vec<usize> =
            boundary_groups.iter().map(|g| g.data_file_count).collect();
        let total_boundary_files: usize = boundary_file_counts.iter().sum();
        assert_eq!(
            total_boundary_files, 2,
            "All 2 boundary files should be preserved"
        );

        // With 2 files and 2 groups, BinPack should distribute as [1,1]
        for (i, group) in boundary_groups.iter().enumerate() {
            assert_eq!(
                group.data_file_count,
                1,
                "Each boundary group {} should contain exactly 1 file",
                i + 1
            );
        }

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
            create_test_file_scan_task("file1.parquet", 1024 * 1024), // 1MB
            create_test_file_scan_task("file2.parquet", 1024 * 1024), // 1MB
            create_test_file_scan_task("file3.parquet", 1024 * 1024), // 1MB
            create_test_file_scan_task("file4.parquet", 1024 * 1024), // 1MB
            create_test_file_scan_task("file5.parquet", 1024 * 1024), // 1MB
        ];

        let groups = grouping_strategy.group_files(data_files.into_iter());

        // With 5 files and max 2 files per group, should create ceil(5/2) = 3 groups
        // to ensure max_files_per_group constraint is respected
        assert!(
            groups.len() >= 3,
            "Should create at least 3 groups due to max_files_per_group constraint"
        );

        // Verify all files are included
        let total_files: usize = groups.iter().map(|g| g.data_file_count).sum();
        assert_eq!(total_files, 5, "All files should be included in groups");

        // Verify the algorithm behavior: with target_group_size very large and 5 files,
        // split_num should be max(1, ceil(5/2)) = 3 groups due to max_files_per_group constraint
        assert_eq!(
            groups.len(),
            3,
            "Should create exactly 3 groups based on ceiling division logic"
        );

        // Groups should have [2, 2, 1] files to respect max_files_per_group=2 constraint
        let mut group_sizes: Vec<usize> = groups.iter().map(|g| g.data_file_count).collect();
        group_sizes.sort();
        assert_eq!(
            group_sizes,
            vec![1, 2, 2],
            "Groups should have [1, 2, 2] files to respect max_files_per_group constraint"
        );

        // Verify no group exceeds max_files_per_group constraint
        for (i, group) in groups.iter().enumerate() {
            assert!(
                group.data_file_count <= 2,
                "Group {} has {} files, should be <= 2",
                i,
                group.data_file_count
            );
        }
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
            !binpack_groups.is_empty(),
            "BinPack should create at least one group"
        );
        assert_eq!(
            binpack_enum.description(),
            "BinPackGrouping[target=20MB, max_files=100]"
        );
    }

    #[test]
    fn test_strategy_descriptions_and_edge_cases() {
        // Combined test for filter strategies descriptions and edge cases

        // Size filter edge cases
        let min_only = SizeFilterStrategy {
            min_size: Some(10 * 1024 * 1024), // 10MB min
            max_size: None,
        };
        assert_eq!(min_only.description(), "SizeFilter[>10MB]");

        let max_only = SizeFilterStrategy {
            min_size: None,
            max_size: Some(50 * 1024 * 1024), // 50MB max
        };
        assert_eq!(max_only.description(), "SizeFilter[<50MB]");

        let no_filter = SizeFilterStrategy {
            min_size: None,
            max_size: None,
        };
        assert_eq!(no_filter.description(), "SizeFilter[Any]");

        // Task size limit description formatting
        let small_limit = TaskSizeLimitStrategy {
            max_total_size: 20 * 1024 * 1024,
        }; // 20MB
        assert_eq!(small_limit.description(), "TaskSizeLimit[0GB]"); // Rounds down to 0GB

        let large_limit = TaskSizeLimitStrategy {
            max_total_size: 2 * 1024 * 1024 * 1024,
        }; // 2GB
        assert_eq!(large_limit.description(), "TaskSizeLimit[2GB]");

        // Test functional behavior for edge cases
        let test_files = vec![
            create_test_file_scan_task("small.parquet", 5 * 1024 * 1024), // 5MB
            create_test_file_scan_task("medium.parquet", 15 * 1024 * 1024), // 15MB
            create_test_file_scan_task("large.parquet", 100 * 1024 * 1024), // 100MB
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

        // Test task size limit edge case
        let limit_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024), // 10MB
            create_test_file_scan_task("file2.parquet", 10 * 1024 * 1024), // 10MB
            create_test_file_scan_task("file3.parquet", 1), // 1 byte - should be filtered
        ];

        let limit_result = small_limit.filter(limit_files);
        assert_eq!(limit_result.len(), 2); // Exactly 20MB limit
        assert_eq!(limit_result[0].data_file_path, "file1.parquet");
        assert_eq!(limit_result[1].data_file_path, "file2.parquet");
    }

    #[test]
    fn test_file_group_parallelism_calculation() {
        // Test FileGroup::calculate_parallelism functionality
        let config = CompactionPlanningConfigBuilder::default()
            .min_size_per_partition(10 * 1024 * 1024) // 10MB per partition
            .max_file_count_per_partition(5) // 5 files per partition
            .max_parallelism(8) // Max 8 parallel tasks
            .enable_heuristic_output_parallelism(true)
            .build()
            .unwrap();

        // Test normal case
        let files = vec![
            create_test_file_scan_task("file1.parquet", 15 * 1024 * 1024), // 15MB
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024), // 20MB
            create_test_file_scan_task("file3.parquet", 25 * 1024 * 1024), // 25MB
        ];

        let group = FileGroup::new(files);
        let (executor_parallelism, output_parallelism) =
            FileGroup::calculate_parallelism(&group, &config).unwrap();

        assert!(executor_parallelism >= 1);
        assert!(output_parallelism >= 1);
        assert!(output_parallelism <= executor_parallelism);
        assert!(executor_parallelism <= config.max_parallelism);

        // Test error case - empty group
        let empty_group = FileGroup::empty();
        let result = FileGroup::calculate_parallelism(&empty_group, &config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No files to calculate_task_parallelism"));
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
}
