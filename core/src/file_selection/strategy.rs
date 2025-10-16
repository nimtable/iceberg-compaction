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
    /// Total size of data files in this group (in bytes)
    ///
    /// Note: This only accounts for data file sizes. For sizing that includes
    /// delete files as well, use [`FileGroup::input_total_bytes`].
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
        // Note: de-duplicate delete files by path to avoid double-counting when
        // multiple data files reference the same delete file.
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
                "No files to calculate task parallelism".to_owned(),
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

        // Apply small-output heuristic to encourage larger outputs when input data is tiny.
        output_parallelism =
            Self::apply_output_parallelism_heuristic(files_to_compact, config, output_parallelism);

        Ok((input_parallelism, output_parallelism))
    }

    /// Heuristic: If the total data size is smaller than the target file size,
    /// favor merging into a single output to avoid tiny files.
    fn apply_output_parallelism_heuristic(
        files_to_compact: &FileGroup,
        config: &CompactionPlanningConfig,
        current_output_parallelism: usize,
    ) -> usize {
        if !config.enable_heuristic_output_parallelism || current_output_parallelism <= 1 {
            return current_output_parallelism;
        }

        let total_data_file_size = files_to_compact
            .data_files
            .iter()
            .map(|f| f.file_size_in_bytes)
            .sum::<u64>();

        if total_data_file_size > 0 && total_data_file_size < config.base.target_file_size {
            1
        } else {
            current_output_parallelism
        }
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

    /// Iterator over all input file paths (data + delete files)
    /// This provides a clean way to access all file paths without repetitive chaining
    pub fn iter_all_input_paths(&self) -> impl Iterator<Item = &str> {
        self.data_files
            .iter()
            .chain(&self.position_delete_files)
            .chain(&self.equality_delete_files)
            .map(|task| task.data_file_path.as_str())
    }
}

/// Object-safe trait for file filtering strategies
///
/// This trait enables dynamic dispatch for composable file filter chains.
pub trait FileFilterStrategy: std::fmt::Debug + Sync + Send {
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
pub trait GroupFilterStrategy: std::fmt::Debug + Sync + Send {
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

/// No-op grouping strategy that places all files into a single group
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

/// Pure bin-packing algorithm - similar to Java's BinPacking.ListPacker
///
/// This struct implements the First-Fit Decreasing algorithm for packing items into bins.
/// It is completely independent of business logic and only cares about the weight constraint.
#[derive(Debug, Clone)]
pub struct ListPacker {
    /// Target weight (size) for each bin
    pub target_weight: u64,
    /// Number of bins to look back when finding a suitable bin for an item
    /// Higher values may produce better packing but take more time
    pub lookback: usize,
}

impl ListPacker {
    /// Create a new `ListPacker` with the given target weight
    pub fn new(target_weight: u64) -> Self {
        Self {
            target_weight,
            lookback: 1, // Default lookback, matches Java's usage
        }
    }

    /// Pack items into bins using the First-Fit Decreasing algorithm
    ///
    /// # Arguments
    /// * `items` - Items to pack
    /// * `weight_func` - Function to extract weight from each item
    ///
    /// # Returns
    /// A vector of bins, where each bin is a vector of items
    pub fn pack<T, F>(&self, mut items: Vec<T>, weight_func: F) -> Vec<Vec<T>>
    where
        F: Fn(&T) -> u64,
    {
        if items.is_empty() {
            return vec![];
        }

        // Sort by weight descending (First-Fit Decreasing)
        items.sort_by_key(|b| std::cmp::Reverse(weight_func(b)));

        let mut bins: Vec<Bin<T>> = vec![];

        for item in items {
            let weight = weight_func(&item);

            // Try to find a bin within the lookback window that can fit this item
            let bin_to_use = bins
                .iter_mut()
                .rev()
                .take(self.lookback)
                .find(|bin| bin.can_add(weight));

            if let Some(bin) = bin_to_use {
                bin.add(item, weight);
            } else {
                // Create a new bin
                let mut new_bin = Bin::new(self.target_weight);
                new_bin.add(item, weight);
                bins.push(new_bin);
            }
        }

        // Extract items from bins
        bins.into_iter().map(|bin| bin.items).collect()
    }
}

/// Internal bin structure for the packing algorithm
#[derive(Debug)]
struct Bin<T> {
    target_weight: u64,
    items: Vec<T>,
    current_weight: u64,
}

impl<T> Bin<T> {
    fn new(target_weight: u64) -> Self {
        Self {
            target_weight,
            items: Vec::new(),
            current_weight: 0,
        }
    }

    fn can_add(&self, weight: u64) -> bool {
        // Special case: if target_weight is 0, always allow adding to existing bin
        // This ensures all items go into a single bin when target is 0
        if self.target_weight == 0 {
            return true;
        }
        self.current_weight + weight <= self.target_weight
    }

    fn add(&mut self, item: T, weight: u64) {
        self.current_weight += weight;
        self.items.push(item);
    }
}

/// `BinPack` grouping strategy that optimizes file size distribution
///
/// This strategy uses a pure bin-packing algorithm (First-Fit Decreasing) to group files
/// based solely on the target group size. It does not enforce any file count limitations.
#[derive(Debug)]
pub struct BinPackGroupingStrategy {
    pub target_group_size: u64,
}

impl BinPackGroupingStrategy {
    /// Create a new `BinPackGroupingStrategy`
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

        // Use the pure ListPacker algorithm
        // This only considers target_group_size, not max_files_per_group
        let packer = ListPacker::new(self.target_group_size);
        let groups = packer.pack(files, |task| task.length);

        // Convert Vec<Vec<FileScanTask>> to Vec<FileGroup>
        groups
            .into_iter()
            .map(FileGroup::new)
            .filter(|group| !group.is_empty())
            .collect()
    }

    pub fn description(&self) -> String {
        format!(
            "BinPackGrouping[target={}MB]",
            self.target_group_size / 1024 / 1024
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
        let gb = self.max_total_size as f64 / (1024.0 * 1024.0 * 1024.0);
        if gb < 1.0 {
            let mb = self.max_total_size / (1024 * 1024);
            format!("TaskSizeLimit[{}MB]", mb)
        } else {
            format!("TaskSizeLimit[{:.1}GB]", gb)
        }
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

/// Configuration for creating custom file selection strategies
#[derive(Debug, Clone)]
pub struct CustomStrategyConfig {
    pub exclude_delete_files: bool,
    pub size_filter: Option<(Option<u64>, Option<u64>)>, // (min_size, max_size)
    pub min_file_count: usize,
    pub max_task_total_size: u64,
    pub grouping_strategy: GroupingStrategy,
}

impl Default for CustomStrategyConfig {
    fn default() -> Self {
        Self {
            exclude_delete_files: false,
            size_filter: None,
            min_file_count: 0,
            max_task_total_size: u64::MAX,
            grouping_strategy: GroupingStrategy::Noop,
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

    pub fn build(self) -> CustomStrategyConfig {
        self.config
    }
}

/// Factory for creating file strategies based on compaction type and configuration
pub struct FileStrategyFactory;

/// Compaction strategy that combines file filtering, grouping, and group filtering
///
/// This strategy provides a flexible composition architecture where:
/// - File filters are composable using Vec<Box<dyn FileFilterStrategy>>
/// - Grouping is handled by a single enum-based strategy
/// - Group filters are composable using Vec<Box<dyn GroupFilterStrategy>>
#[derive(Debug)]
pub struct CompactionStrategy {
    file_filters: Vec<Box<dyn FileFilterStrategy>>,
    grouping: GroupingStrategyEnum,
    group_filters: Vec<Box<dyn GroupFilterStrategy>>,
}

impl CompactionStrategy {
    /// Create a new compaction strategy
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

    /// Apply the compaction strategy to filter and group files
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

        // Step 3: Apply group filtering before computing parallelism
        for filter in &self.group_filters {
            file_groups = filter.filter_groups(file_groups);
        }

        // Step 4: Calculate parallelism for each remaining group
        for group in &mut file_groups {
            let (executor_parallelism, output_parallelism) =
                FileGroup::calculate_parallelism(group, config)?;
            group.executor_parallelism = executor_parallelism;
            group.output_parallelism = output_parallelism;
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
    /// Create strategy for small files compaction with delete file filtering and size limits.
    pub fn create_small_files_strategy(config: &CompactionPlanningConfig) -> CompactionStrategy {
        // Use the grouping strategy from config
        Self::create_custom_strategy(
            /* exclude_delete_files */ true,
            /* size_filter */ Some((None, Some(config.small_file_threshold))),
            /* min_file_count */ config.min_file_count,
            /* max_task_total_size */ config.max_task_total_size,
            /* grouping_strategy */ &config.grouping_strategy,
        )
    }

    /// Create a no-op strategy that passes all files through
    pub fn create_noop_strategy() -> CompactionStrategy {
        CompactionStrategy::new(
            vec![Box::new(NoopStrategy)],
            GroupingStrategyEnum::Noop(NoopGroupingStrategy),
            vec![Box::new(NoopGroupFilterStrategy)],
        )
    }

    /// Create custom strategy with fine-grained parameter control.
    pub fn create_custom_strategy(
        exclude_delete_files: bool,
        size_filter: Option<(Option<u64>, Option<u64>)>, // (min_size, max_size)
        min_file_count: usize,
        max_task_total_size: u64,
        grouping_strategy: &GroupingStrategy,
    ) -> CompactionStrategy {
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
            GroupingStrategy::BinPack(config) => GroupingStrategyEnum::BinPack(
                BinPackGroupingStrategy::new(config.target_group_size),
            ),
        };

        // Group filtering layer (from BinPackConfig)
        let mut group_filters: Vec<Box<dyn GroupFilterStrategy>> = vec![];

        if let GroupingStrategy::BinPack(config) = grouping_strategy {
            if let Some(min_size) = config.min_group_size {
                if min_size > 0 {
                    group_filters.push(Box::new(MinGroupSizeStrategy {
                        min_group_size: min_size,
                    }));
                }
            }

            if let Some(min_count) = config.min_group_file_count {
                if min_count > 0 {
                    group_filters.push(Box::new(MinGroupFileCountStrategy {
                        min_file_count: min_count,
                    }));
                }
            }
        }

        CompactionStrategy::new(file_filters, grouping, group_filters)
    }

    /// Create file strategy based on compaction type and configuration.
    pub fn create_files_strategy(
        compaction_type: CompactionType,
        config: &CompactionPlanningConfig,
    ) -> CompactionStrategy {
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
    /// This method returns a `CompactionStrategy` for flexible composition based on config.
    ///
    /// # Arguments
    /// * `compaction_type` - The type of compaction to perform
    /// * `config` - The compaction configuration
    ///
    /// # Returns
    /// A `CompactionStrategy` containing the appropriate strategy for the given parameters
    pub fn create_dynamic_strategy(
        compaction_type: CompactionType,
        config: &CompactionPlanningConfig,
    ) -> CompactionStrategy {
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
            strategy: &CompactionStrategy,
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
            TestFileBuilder::new("too_small.parquet")
                .size(2 * 1024 * 1024)
                .build(), // 2MB - should be filtered
            TestFileBuilder::new("min_edge.parquet")
                .size(5 * 1024 * 1024)
                .build(), // 5MB - should pass (exactly min)
            TestFileBuilder::new("medium1.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB - should pass
            TestFileBuilder::new("medium2.parquet")
                .size(30 * 1024 * 1024)
                .build(), // 30MB - should pass
            TestFileBuilder::new("max_edge.parquet")
                .size(50 * 1024 * 1024)
                .build(), // 50MB - should pass (exactly max)
            TestFileBuilder::new("too_large.parquet")
                .size(100 * 1024 * 1024)
                .build(), // 100MB - should be filtered
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
        TestUtils::assert_paths_eq(&expected_files, &result);

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
            TestFileBuilder::new("exact_min.parquet")
                .size(5 * 1024 * 1024)
                .build(), // Exactly 5MB
            TestFileBuilder::new("exact_max.parquet")
                .size(50 * 1024 * 1024)
                .build(), // Exactly 50MB
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
        // Keep this test minimal: verify factory outputs non-empty description and basic routing
        let config = CompactionPlanningConfigBuilder::default().build().unwrap();

        // Noop factory
        let noop_strategy = FileStrategyFactory::create_noop_strategy();
        assert!(noop_strategy.description().contains("Noop"));

        // Small-files strategy should mention core filters
        let small_files_strategy = FileStrategyFactory::create_small_files_strategy(&config);
        let small_files_desc = small_files_strategy.description();
        assert!(small_files_desc.contains("NoDeleteFiles"));
        assert!(small_files_desc.contains("SizeFilter"));

        // Compaction type routing
        let routed_small = FileStrategyFactory::create_files_strategy(
            crate::compaction::CompactionType::MergeSmallDataFiles,
            &config,
        );
        assert!(!routed_small.description().is_empty());

        let routed_full = FileStrategyFactory::create_files_strategy(
            crate::compaction::CompactionType::Full,
            &config,
        );
        assert!(routed_full.description().contains("Noop"));
    }

    #[test]
    fn test_no_delete_files_strategy() {
        let strategy = NoDeleteFilesStrategy;

        let data_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(), // No deletes - should pass
            TestFileBuilder::new("file2.parquet")
                .size(20 * 1024 * 1024)
                .with_deletes()
                .build(), // Has deletes - should be filtered out
            TestFileBuilder::new("file3.parquet")
                .size(15 * 1024 * 1024)
                .build(), // No deletes - should pass
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

        assert_eq!(strategy.description(), "TaskSizeLimit[25MB]");

        // Test cumulative size limiting with precise validation
        let test_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB - should pass (total: 10MB)
            TestFileBuilder::new("file2.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB - should pass (total: 20MB)
            TestFileBuilder::new("file3.parquet")
                .size(5 * 1024 * 1024)
                .build(), // 5MB - should pass (total: 25MB exactly)
            TestFileBuilder::new("file4.parquet").size(1).build(), // 1 byte - should be filtered (would exceed)
            TestFileBuilder::new("file5.parquet")
                .size(5 * 1024 * 1024)
                .build(), // 5MB - should be filtered
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
            TestFileBuilder::new("huge1.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB - exceeds limit alone
            TestFileBuilder::new("huge2.parquet")
                .size(10 * 1024 * 1024)
                .build(), // 10MB - would also exceed
        ];

        let large_result: Vec<FileScanTask> = large_limit_strategy.filter(large_files);
        assert_eq!(
            large_result.len(),
            0,
            "Should filter all files when each exceeds limit"
        );

        // Test with exact boundary
        let boundary_files = vec![
            TestFileBuilder::new("exact.parquet")
                .size(5 * 1024 * 1024)
                .build(), // Exactly 5MB
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
        assert_eq!(
            more_result.len(),
            5,
            "Should pass all files when exceeding minimum"
        );

        // Test with fewer than minimum files (2 < 3) - should pass none
        let fewer_files = vec![
            TestFileBuilder::new("file1.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestFileBuilder::new("file2.parquet")
                .size(20 * 1024 * 1024)
                .build(),
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
        let config = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(20 * 1024 * 1024) // 20MB threshold
            .max_task_total_size(50 * 1024 * 1024) // 50MB task limit
            .build()
            .unwrap();

        let strategy = FileStrategyFactory::create_small_files_strategy(&config);

        // Description should reflect core filters
        let desc = strategy.description();
        assert!(desc.contains("NoDeleteFiles") && desc.contains("SizeFilter"));

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

        assert_eq!(result.len(), 2);
        let mut paths: Vec<_> = result.iter().map(|f| f.data_file_path.as_str()).collect();
        paths.sort();
        assert_eq!(paths, vec!["small1.parquet", "small3.parquet"]);

        // Verify all selected files meet criteria
        for file in &result {
            assert!(file.length <= config.small_file_threshold);
            assert!(file.deletes.is_empty());
        }

        // Test min_file_count behavior
        let min_count_config = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(20 * 1024 * 1024)
            .min_file_count(3)
            .build()
            .unwrap();

        let min_count_strategy =
            FileStrategyFactory::create_small_files_strategy(&min_count_config);

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
        let default_config = CompactionPlanningConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::Noop)
            .build()
            .unwrap();

        assert_eq!(default_config.min_file_count, 0);

        let default_strategy = FileStrategyFactory::create_small_files_strategy(&default_config);

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
        for file in &multi_result {
            assert!(file.length <= default_config.small_file_threshold);
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
        assert_eq!(min_size_strategy.description(), "MinGroupSize[100MB]");
        assert_eq!(
            min_file_count_strategy.description(),
            "MinGroupFileCount[2]"
        );
        assert_eq!(noop_strategy.description(), "NoopGroupFilter");
    }

    #[test]
    fn test_create_custom_strategy_comprehensive() {
        use crate::config::GroupingStrategy;

        // Test create_custom_strategy with basic parameter combinations

        // Test case 1: All filters enabled
        let strategy_all_enabled = FileStrategyFactory::create_custom_strategy(
            true,                                               // exclude_delete_files
            Some((Some(1024 * 1024), Some(100 * 1024 * 1024))), // size_filter: 1MB-100MB
            0,                       // min_file_count: disabled for simpler test
            10 * 1024 * 1024 * 1024, // max_task_total_size: 10GB
            &GroupingStrategy::Noop,
        );

        let description = strategy_all_enabled.description();
        assert!(description.contains("NoDeleteFiles") && description.contains("SizeFilter"));

        // Test case 2: Minimal filters
        let strategy_minimal = FileStrategyFactory::create_custom_strategy(
            false,
            None,
            0,
            u64::MAX,
            &GroupingStrategy::Noop,
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

        // Strategy with filters should exclude files with deletes and large files
        let result_filtered =
            TestUtils::execute_strategy_flat(&strategy_all_enabled, test_files.clone());
        assert_eq!(result_filtered.len(), 2); // small.parquet and medium.parquet

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

        assert_eq!(
            normal_strategy.description(),
            "BinPackGrouping[target=20MB]"
        );

        // Test Case 4: Large total size relative to target produces multiple groups (via factory)
        // Mirrors prior factory-based test but keeps coverage centralized here.
        use crate::config::BinPackConfig;
        let bin_pack_strategy = FileStrategyFactory::create_custom_strategy(
            false,
            None,
            0,
            u64::MAX,
            &GroupingStrategy::BinPack(BinPackConfig::new(64 * 1024 * 1024)), // 64MB target, no filters
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
        assert_eq!(noop_enum.description(), "NoopGrouping");

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
        assert_eq!(binpack_enum.description(), "BinPackGrouping[target=20MB]");
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
        let config = CompactionPlanningConfigBuilder::default()
            .min_size_per_partition(10 * 1024 * 1024) // 10MB per partition
            .max_file_count_per_partition(5) // 5 files per partition
            .max_parallelism(8) // Max 8 parallel tasks
            .enable_heuristic_output_parallelism(true)
            .build()
            .unwrap();

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
        assert!(executor_parallelism <= config.max_parallelism);

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
        let config = CompactionPlanningConfigBuilder::default()
            .min_size_per_partition(1) // allow partitioning to be driven by counts
            .max_file_count_per_partition(1)
            .max_parallelism(8)
            .enable_heuristic_output_parallelism(true)
            .build()
            .unwrap();

        let (exec_p, out_p) = FileGroup::calculate_parallelism(&group, &config).unwrap();
        assert!(exec_p >= 1);
        assert_eq!(
            out_p, 1,
            "Heuristic should force single output when data is tiny"
        );
    }
}
