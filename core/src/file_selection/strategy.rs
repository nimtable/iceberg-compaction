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
//! 1. File filters: Select files with composable per-file predicates
//! 2. Grouping: Choose a file-group scope, then combine files using Single
//!    (all-in-one) or `BinPack` (First-Fit Decreasing)
//! 3. Group filters: Remove groups below size/count thresholds
//!
//! Parallelism is calculated when selected groups become compaction plans.

use std::collections::HashMap;

use iceberg::scan::FileScanTask;

use super::packer::ListPacker;
use crate::config::{
    CompactionPlanningConfig, CompactionStrategy, FileGroupScope, GroupFilters, GroupingStrategy,
};

/// Complete result of file selection and grouping, before plan assembly.
///
/// Delete files are deduplicated by physical file/blob identity during construction.
/// Position deletes use Iceberg's fixed delete schema; equality deletes use their
/// descriptor's `equality_ids`.
///
/// `total_size` is the sum of `data_files[*].length` and excludes delete file
/// sizes. Execution parallelism belongs to the compaction plan assembled from
/// this selected group.
#[derive(Debug, Clone)]
pub struct SelectedFileGroup {
    pub data_files: Vec<FileScanTask>,
    pub position_delete_files: Vec<FileScanTask>,
    pub equality_delete_files: Vec<FileScanTask>,
    /// Sum of data file sizes only. Use [`input_total_bytes`](Self::input_total_bytes) for all files.
    pub total_size: u64,
    pub data_file_count: usize,
}

impl SelectedFileGroup {
    /// Constructs a selected group from data files.
    ///
    /// Deduplicates delete files by path plus optional Puffin byte range. Position
    /// delete files use Iceberg's fixed delete projection; equality delete files
    /// copy descriptor `equality_ids` to `project_field_ids`.
    pub fn new(data_files: Vec<FileScanTask>) -> Self {
        let total_size = data_files.iter().map(|task| task.length).sum();
        let data_file_count = data_files.len();

        // A Puffin file can contain multiple deletion-vector blobs, so path alone
        // is not a sufficient identity.
        let mut position_delete_map = HashMap::new();
        let mut equality_delete_map = HashMap::new();

        for task in &data_files {
            for delete_task in &task.deletes {
                let identity = (
                    delete_task.file_path.clone(),
                    delete_task.content_offset,
                    delete_task.content_size_in_bytes,
                );
                let mut standalone_task = delete_task.to_file_scan_task(task);
                match delete_task.file_type {
                    iceberg::spec::DataContentType::PositionDeletes => {
                        position_delete_map
                            .entry(identity)
                            .or_insert(standalone_task);
                    }
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        standalone_task.project_field_ids =
                            delete_task.equality_ids.clone().unwrap_or_default();
                        equality_delete_map
                            .entry(identity)
                            .or_insert(standalone_task);
                    }
                    _ => {}
                }
            }
        }

        let position_delete_files = position_delete_map
            .into_values()
            .collect::<Vec<FileScanTask>>();

        let equality_delete_files = equality_delete_map
            .into_values()
            .collect::<Vec<FileScanTask>>();

        Self {
            data_files,
            position_delete_files,
            equality_delete_files,
            total_size,
            data_file_count,
        }
    }

    /// Returns an empty selected group.
    pub fn empty() -> Self {
        Self {
            data_files: Vec::new(),
            position_delete_files: Vec::new(),
            equality_delete_files: Vec::new(),
            total_size: 0,
            data_file_count: 0,
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

/// Per-file predicate applied before grouping.
///
/// Concrete filters can be composed with [`and`](Self::and) and [`or`](Self::or)
/// before being type-erased for [`PlanStrategy`]. Filters in [`PlanStrategy`] are
/// still applied sequentially, so the top-level list has implicit AND semantics.
pub trait FileFilterStrategy: std::fmt::Debug + std::fmt::Display + Sync + Send {
    /// Returns whether a data file belongs to the candidate set.
    fn matches(&self, data_file: &FileScanTask) -> bool;

    /// Returns the matching data files in their original order.
    fn filter(&self, data_files: Vec<FileScanTask>) -> Vec<FileScanTask> {
        data_files
            .into_iter()
            .filter(|data_file| self.matches(data_file))
            .collect()
    }

    /// Combines two concrete filters with short-circuiting AND semantics.
    #[must_use]
    fn and<R>(self, right: R) -> AndFileFilter<Self, R>
    where
        Self: Sized,
        R: FileFilterStrategy,
    {
        AndFileFilter { left: self, right }
    }

    /// Combines two concrete filters with short-circuiting OR semantics.
    #[must_use]
    fn or<R>(self, right: R) -> OrFileFilter<Self, R>
    where
        Self: Sized,
        R: FileFilterStrategy,
    {
        OrFileFilter { left: self, right }
    }
}

/// Two concrete file filters combined with AND semantics.
#[derive(Debug)]
pub struct AndFileFilter<L, R> {
    left: L,
    right: R,
}

impl<L, R> FileFilterStrategy for AndFileFilter<L, R>
where
    L: FileFilterStrategy,
    R: FileFilterStrategy,
{
    fn matches(&self, data_file: &FileScanTask) -> bool {
        self.left.matches(data_file) && self.right.matches(data_file)
    }
}

impl<L, R> std::fmt::Display for AndFileFilter<L, R>
where
    L: std::fmt::Display,
    R: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} AND {})", self.left, self.right)
    }
}

/// Two concrete file filters combined with OR semantics.
#[derive(Debug)]
pub struct OrFileFilter<L, R> {
    left: L,
    right: R,
}

impl<L, R> FileFilterStrategy for OrFileFilter<L, R>
where
    L: FileFilterStrategy,
    R: FileFilterStrategy,
{
    fn matches(&self, data_file: &FileScanTask) -> bool {
        self.left.matches(data_file) || self.right.matches(data_file)
    }
}

impl<L, R> std::fmt::Display for OrFileFilter<L, R>
where
    L: std::fmt::Display,
    R: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} OR {})", self.left, self.right)
    }
}

/// Enum dispatching to grouping strategy implementations.
#[derive(Debug)]
pub enum GroupingStrategyEnum {
    Single(SingleGroupingStrategy),
    BinPack(BinPackGroupingStrategy),
}

impl GroupingStrategyEnum {
    /// Groups files independently within each Iceberg partition.
    ///
    /// This preserves the historical public API behavior. Use [`PlanStrategy`]
    /// with [`FileGroupScope::Table`] when the grouping strategy should see
    /// all selected files together.
    pub fn group_files<I>(&self, data_files: I) -> Vec<SelectedFileGroup>
    where I: IntoIterator<Item = FileScanTask> {
        group_files_by_partition(data_files.into_iter())
            .into_values()
            .flat_map(|files| self.group_files_without_partitioning(files))
            .collect()
    }

    fn group_files_without_partitioning<I>(&self, data_files: I) -> Vec<SelectedFileGroup>
    where I: IntoIterator<Item = FileScanTask> {
        let data_files = data_files.into_iter();
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
    fn filter_groups(&self, groups: Vec<SelectedFileGroup>) -> Vec<SelectedFileGroup>;
}

/// Single grouping strategy. Groups all files into one selected group.
///
/// Returns empty vec if input is empty.
#[derive(Debug)]
pub struct SingleGroupingStrategy;

impl SingleGroupingStrategy {
    pub fn group_files<I>(&self, data_files: I) -> Vec<SelectedFileGroup>
    where I: Iterator<Item = FileScanTask> {
        let files: Vec<FileScanTask> = data_files.collect();
        if files.is_empty() {
            vec![]
        } else {
            vec![SelectedFileGroup::new(files)]
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

    pub fn group_files<I>(&self, data_files: I) -> Vec<SelectedFileGroup>
    where I: Iterator<Item = FileScanTask> {
        let files: Vec<FileScanTask> = data_files.collect();

        if files.is_empty() {
            return vec![];
        }

        let packer = ListPacker::new(self.target_group_size);
        let groups = packer.pack(files, |task| task.file_size_in_bytes);

        groups
            .into_iter()
            .map(SelectedFileGroup::new)
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
/// `min_size` is inclusive and `max_size` is exclusive. If both are `None`,
/// passes all files.
#[derive(Debug)]
pub struct SizeFilterStrategy {
    pub min_size: Option<u64>,
    pub max_size: Option<u64>,
}

impl FileFilterStrategy for SizeFilterStrategy {
    fn matches(&self, data_file: &FileScanTask) -> bool {
        let file_size = data_file.length;
        match (self.min_size, self.max_size) {
            (Some(min), Some(max)) => file_size >= min && file_size < max,
            (Some(min), None) => file_size >= min,
            (None, Some(max)) => file_size < max,
            (None, None) => true,
        }
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
    fn matches(&self, data_file: &FileScanTask) -> bool {
        data_file.deletes.len() >= self.min_delete_file_count
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
    fn filter_groups(&self, groups: Vec<SelectedFileGroup>) -> Vec<SelectedFileGroup> {
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
    fn filter_groups(&self, groups: Vec<SelectedFileGroup>) -> Vec<SelectedFileGroup> {
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

/// Options for constructing a [`PlanStrategy`].
///
/// The default file group scope is [`FileGroupScope::Partition`], matching the
/// historical behavior of [`PlanStrategy::new`].
#[derive(Debug)]
pub struct PlanStrategyOptions {
    file_filters: Vec<Box<dyn FileFilterStrategy>>,
    grouping: GroupingStrategyEnum,
    file_group_scope: FileGroupScope,
    group_filters: Vec<Box<dyn GroupFilterStrategy>>,
}

impl PlanStrategyOptions {
    /// Creates options from the explicit file-selection pipeline stages.
    ///
    /// The file group scope defaults to [`FileGroupScope::Partition`]. Use
    /// [`with_file_group_scope`](Self::with_file_group_scope) to opt into
    /// table-scoped grouping.
    #[must_use]
    pub fn new(
        file_filters: Vec<Box<dyn FileFilterStrategy>>,
        grouping: GroupingStrategyEnum,
        group_filters: Vec<Box<dyn GroupFilterStrategy>>,
    ) -> Self {
        Self {
            file_filters,
            grouping,
            file_group_scope: FileGroupScope::Partition,
            group_filters,
        }
    }

    /// Sets the scope used before the grouping strategy runs.
    #[must_use]
    pub fn with_file_group_scope(mut self, file_group_scope: FileGroupScope) -> Self {
        self.file_group_scope = file_group_scope;
        self
    }
}

/// Three-stage pipeline: file filters → grouping → group filters.
///
/// Filters and group filters are applied sequentially. See [`select`](Self::select) for details.
#[derive(Debug)]
pub struct PlanStrategy {
    file_filters: Vec<Box<dyn FileFilterStrategy>>,
    grouping: GroupingStrategyEnum,
    file_group_scope: FileGroupScope,
    group_filters: Vec<Box<dyn GroupFilterStrategy>>,
}

impl PlanStrategy {
    /// Creates a plan strategy with partition-scoped grouping.
    ///
    /// This preserves the historical public constructor behavior. Use
    /// [`new_with_options`](Self::new_with_options) when non-default strategy
    /// options are needed.
    #[must_use]
    pub fn new(
        file_filters: Vec<Box<dyn FileFilterStrategy>>,
        grouping: GroupingStrategyEnum,
        group_filters: Vec<Box<dyn GroupFilterStrategy>>,
    ) -> Self {
        Self::new_with_options(PlanStrategyOptions::new(
            file_filters,
            grouping,
            group_filters,
        ))
    }

    /// Creates a plan strategy from explicit construction options.
    #[must_use]
    pub fn new_with_options(options: PlanStrategyOptions) -> Self {
        Self {
            file_filters: options.file_filters,
            grouping: options.grouping,
            file_group_scope: options.file_group_scope,
            group_filters: options.group_filters,
        }
    }

    /// Selects and groups files:
    /// 1. Apply each file filter sequentially
    /// 2. Group files using the configured file-group scope and grouping strategy
    /// 3. Apply each group filter sequentially
    pub fn select(&self, data_files: Vec<FileScanTask>) -> Vec<SelectedFileGroup> {
        let mut filtered_files = data_files;
        for filter in &self.file_filters {
            filtered_files = filter.filter(filtered_files);
        }

        let file_groups = self.group_files(filtered_files);

        let mut file_groups = file_groups;
        for filter in &self.group_filters {
            file_groups = filter.filter_groups(file_groups);
        }

        file_groups
    }

    fn group_files(&self, data_files: Vec<FileScanTask>) -> Vec<SelectedFileGroup> {
        match self.file_group_scope {
            FileGroupScope::Table => self.grouping.group_files_without_partitioning(data_files),
            FileGroupScope::Partition => self.grouping.group_files(data_files),
        }
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
        // Cross-cutting planning-scope filters must be inserted before the
        // strategy-specific candidate filter so the top-level AND semantics
        // apply uniformly to Full and selective strategies.
        let mut file_filters: Vec<Box<dyn FileFilterStrategy>> = vec![];

        let group_filter_config: Option<&GroupFilters> = match &config.strategy {
            CompactionStrategy::Full => None,
            CompactionStrategy::SmallFiles(strategy) => {
                file_filters.push(Box::new(SizeFilterStrategy {
                    min_size: None,
                    max_size: Some(strategy.small_file_threshold_bytes),
                }));
                strategy.group_filters.as_ref()
            }
            CompactionStrategy::FilesWithDeletes(strategy) => {
                if strategy.min_delete_file_count_threshold > 0 {
                    file_filters.push(Box::new(DeleteFileCountFilterStrategy::new(
                        strategy.min_delete_file_count_threshold,
                    )));
                }
                strategy.group_filters.as_ref()
            }
            CompactionStrategy::Auto(strategy) => {
                let size_filter = SizeFilterStrategy {
                    min_size: None,
                    max_size: Some(strategy.small_file_threshold_bytes),
                };
                let delete_filter =
                    DeleteFileCountFilterStrategy::new(strategy.min_delete_file_count_threshold);
                let candidate_filter: Box<dyn FileFilterStrategy> = match (
                    strategy.small_file_threshold_bytes > 0,
                    strategy.min_delete_file_count_threshold > 0,
                ) {
                    (true, true) => Box::new(size_filter.or(delete_filter)),
                    (true, false) => Box::new(size_filter),
                    (false, true) => Box::new(delete_filter),
                    // A size upper bound of zero matches no files.
                    (false, false) => Box::new(size_filter),
                };
                file_filters.push(candidate_filter);
                strategy.group_filters.as_ref()
            }
        };

        let (grouping, group_filters) =
            Self::build_grouping_and_filters(&config.grouping_strategy, group_filter_config);

        Self::new_with_options(
            PlanStrategyOptions::new(file_filters, grouping, group_filters)
                .with_file_group_scope(config.file_group_scope),
        )
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

        match self.file_group_scope {
            FileGroupScope::Partition => {
                write!(
                    f,
                    "{} -> {} -> {}",
                    file_filter_desc, self.grouping, group_filter_desc
                )
            }
            FileGroupScope::Table => write!(
                f,
                "{} -> {:?} -> {} -> {}",
                file_filter_desc, self.file_group_scope, self.grouping, group_filter_desc
            ),
        }
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

    use iceberg::scan::FileScanTaskDeleteFile;

    use super::*;
    use crate::compaction::CompactionPlanner;
    use crate::config::{
        AutoCompactionConfigBuilder, CompactionPlanningConfig, CompactionPlanningConfigBuilder,
        CompactionStrategy, FileGroupScope, FilesWithDeletesConfigBuilder, SmallFilesConfigBuilder,
    };
    use crate::{CompactionError, Result};
    static TEST_SCHEMA: OnceLock<Arc<iceberg::spec::Schema>> = OnceLock::new();

    fn plan_parallelism(
        selected_file_group: &SelectedFileGroup,
        config: &CompactionPlanningConfig,
    ) -> Result<(usize, usize)> {
        let plan = CompactionPlanner::new(config.clone()).create_plan(
            selected_file_group.clone(),
            iceberg::spec::MAIN_BRANCH,
            1,
        )?;
        Ok((
            plan.recommended_executor_parallelism(),
            plan.recommended_output_parallelism(),
        ))
    }

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
        data_sequence_number: Option<i64>,
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
                data_sequence_number: None,
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

        pub fn with_data_sequence_number(mut self, sequence_number: i64) -> Self {
            self.data_sequence_number = Some(sequence_number);
            self
        }

        pub fn build(self) -> FileScanTask {
            use iceberg::spec::{DataContentType, DataFileFormat};

            let deletes = if self.has_deletes {
                self.delete_types
                    .into_iter()
                    .enumerate()
                    .map(|(i, delete_type)| {
                        FileScanTaskDeleteFile::builder()
                            .with_file_path(format!(
                                "{}_{}_delete.parquet",
                                self.path.replace(".parquet", ""),
                                i
                            ))
                            .with_file_size_in_bytes(1024)
                            .with_file_type(delete_type)
                            .with_partition_spec_id(0)
                            .with_equality_ids(if delete_type == DataContentType::EqualityDeletes {
                                Some(vec![1, 2])
                            } else {
                                None
                            })
                            .with_file_format(DataFileFormat::Parquet)
                            .with_record_count(Some(10))
                            .with_sequence_number(1)
                            .build()
                    })
                    .collect()
            } else {
                vec![]
            };

            FileScanTask {
                start: 0,
                length: self.size,
                record_count: Some(100),
                first_row_id: None,
                data_sequence_number: self.data_sequence_number,
                data_file_path: self.path,
                data_file_format: DataFileFormat::Parquet,
                schema: self.schema.unwrap_or(get_test_schema()),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes,
                sequence_number: 1,
                file_size_in_bytes: self.size,
                partition: self.partition,
                partition_spec: None,
                name_mapping: None,
                unified_partition_type: None,
                case_sensitive: true,
                key_metadata: None,
            }
        }
    }

    #[test]
    fn test_delete_cleanup_min_data_sequence_number() {
        let tasks = vec![
            TestFileBuilder::new("old-clean.parquet")
                .with_data_sequence_number(1)
                .build(),
            TestFileBuilder::new("newer-affected.parquet")
                .with_data_sequence_number(10)
                .with_deletes()
                .build(),
            TestFileBuilder::new("oldest-affected.parquet")
                .with_data_sequence_number(8)
                .with_deletes()
                .build(),
        ];
        assert_eq!(
            crate::file_selection::FileSelector::delete_cleanup_min_data_sequence_number(&tasks),
            Some(8)
        );

        let missing_sequence = vec![
            TestFileBuilder::new("unknown.parquet")
                .with_deletes()
                .build(),
        ];
        assert_eq!(
            crate::file_selection::FileSelector::delete_cleanup_min_data_sequence_number(
                &missing_sequence
            ),
            None
        );
    }

    /// Helper functions for common test scenarios
    pub struct TestUtils;

    impl TestUtils {
        /// Execute strategy and return flattened files for testing
        pub fn execute_strategy_flat(
            strategy: &PlanStrategy,
            data_files: Vec<FileScanTask>,
        ) -> Vec<FileScanTask> {
            strategy
                .select(data_files)
                .into_iter()
                .flat_map(|group| group.into_files())
                .collect()
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
        ) -> FileScanTaskDeleteFile {
            use iceberg::spec::DataFileFormat;
            FileScanTaskDeleteFile::builder()
                .with_file_path(path)
                .with_file_size_in_bytes(1024)
                .with_file_type(content_type)
                .with_partition_spec_id(0)
                .with_equality_ids(
                    (content_type == iceberg::spec::DataContentType::EqualityDeletes)
                        .then_some(vec![1, 2]),
                )
                .with_file_format(DataFileFormat::Parquet)
                .with_record_count(Some(10))
                .with_sequence_number(1)
                .build()
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

    fn composite_filter_test_files() -> Vec<FileScanTask> {
        vec![
            TestFileBuilder::new("small-clean.parquet")
                .size(10 * 1024 * 1024)
                .build(),
            TestUtils::add_delete_files(
                TestFileBuilder::new("large-delete-heavy.parquet")
                    .size(64 * 1024 * 1024)
                    .build(),
                2,
            ),
            TestUtils::add_delete_files(
                TestFileBuilder::new("small-delete-heavy.parquet")
                    .size(10 * 1024 * 1024)
                    .build(),
                2,
            ),
            TestFileBuilder::new("large-clean.parquet")
                .size(64 * 1024 * 1024)
                .build(),
        ]
    }

    fn small_file_filter() -> SizeFilterStrategy {
        SizeFilterStrategy {
            min_size: None,
            max_size: Some(32 * 1024 * 1024),
        }
    }

    fn delete_heavy_filter() -> DeleteFileCountFilterStrategy {
        DeleteFileCountFilterStrategy::new(2)
    }

    #[test]
    fn test_and_file_filter_requires_both_predicates() {
        let filter = small_file_filter().and(delete_heavy_filter());

        let result = filter.filter(composite_filter_test_files());

        TestUtils::assert_paths_eq(&["small-delete-heavy.parquet"], &result);
        assert_eq!(
            filter.to_string(),
            "(SizeFilter[<32MB] AND DeleteFileCountFilter[>=2 deletes])"
        );
    }

    #[test]
    fn test_or_file_filter_accepts_either_predicate_without_duplicates() {
        let filter = small_file_filter().or(delete_heavy_filter());

        let result = filter.filter(composite_filter_test_files());

        TestUtils::assert_paths_eq(
            &[
                "small-clean.parquet",
                "large-delete-heavy.parquet",
                "small-delete-heavy.parquet",
            ],
            &result,
        );
        assert_eq!(
            filter.to_string(),
            "(SizeFilter[<32MB] OR DeleteFileCountFilter[>=2 deletes])"
        );
    }

    #[test]
    fn test_file_filter_combinators_can_be_nested() {
        let candidate_filter = small_file_filter().or(delete_heavy_filter());
        let large_file_filter = SizeFilterStrategy {
            min_size: Some(32 * 1024 * 1024),
            max_size: None,
        };
        let filter = candidate_filter.and(large_file_filter);

        let result = filter.filter(composite_filter_test_files());

        TestUtils::assert_paths_eq(&["large-delete-heavy.parquet"], &result);
        assert_eq!(
            filter.to_string(),
            "((SizeFilter[<32MB] OR DeleteFileCountFilter[>=2 deletes]) AND SizeFilter[>32MB])"
        );
    }

    #[test]
    fn test_auto_strategy_groups_the_union_of_candidate_files() {
        let auto_config = AutoCompactionConfigBuilder::default()
            .small_file_threshold_bytes(32 * 1024 * 1024_u64)
            .min_delete_file_count_threshold(2_usize)
            .build()
            .unwrap();
        let planning_config = CompactionPlanningConfig::new(CompactionStrategy::Auto(auto_config));
        let strategy = PlanStrategy::from(&planning_config);

        let groups = strategy.select(composite_filter_test_files());

        assert_eq!(groups.len(), 1);
        TestUtils::assert_paths_eq(
            &[
                "small-clean.parquet",
                "large-delete-heavy.parquet",
                "small-delete-heavy.parquet",
            ],
            &groups[0].data_files,
        );
        assert_eq!(
            strategy.to_string(),
            "(SizeFilter[<32MB] OR DeleteFileCountFilter[>=2 deletes]) -> SingleGrouping -> NoGroupFilters"
        );
    }

    #[test]
    fn test_auto_strategy_zero_threshold_disables_its_predicate() {
        let cases: [(u64, usize, Vec<&str>); 3] = [
            (0, 2, vec![
                "large-delete-heavy.parquet",
                "small-delete-heavy.parquet",
            ]),
            (32 * 1024 * 1024, 0, vec![
                "small-clean.parquet",
                "small-delete-heavy.parquet",
            ]),
            (0, 0, vec![]),
        ];

        for (small_threshold, delete_threshold, expected_paths) in cases {
            let auto_config = AutoCompactionConfigBuilder::default()
                .small_file_threshold_bytes(small_threshold)
                .min_delete_file_count_threshold(delete_threshold)
                .build()
                .unwrap();
            let planning_config =
                CompactionPlanningConfig::new(CompactionStrategy::Auto(auto_config));
            let strategy = PlanStrategy::from(&planning_config);
            let files = strategy
                .select(composite_filter_test_files())
                .into_iter()
                .flat_map(SelectedFileGroup::into_files)
                .collect::<Vec<_>>();

            TestUtils::assert_paths_eq(&expected_paths, &files);
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

        // Test normal range filtering [5MB, 50MB)
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

        // Test min = max (empty range because max is exclusive)
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
        let small_files_config = CompactionPlanningConfig::new(CompactionStrategy::SmallFiles(
            crate::config::SmallFilesConfig::default(),
        ));

        let small_files_strategy = PlanStrategy::from(&small_files_config);
        let small_files_desc = small_files_strategy.to_string();
        assert!(small_files_desc.contains("SizeFilter"));
        assert!(
            small_files_desc.contains("SingleGrouping")
                || small_files_desc.contains("BinPackGrouping")
        );

        let full_config = CompactionPlanningConfig::default();
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
        let deletes_config = CompactionPlanningConfig::new(CompactionStrategy::FilesWithDeletes(
            crate::config::FilesWithDeletesConfig::default(),
        ));
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

        let groups = strategy.select(test_files);

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

        let large_groups = strategy.select(large_files);
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
                .group_filters(crate::config::GroupFilters {
                    min_group_file_count: Some(min_count),
                    min_group_size_bytes: None,
                })
                .build()
                .unwrap();
            let config =
                CompactionPlanningConfig::new(CompactionStrategy::SmallFiles(small_files_config));
            let strategy = PlanStrategy::from(&config);

            let files: Vec<FileScanTask> = (0..input_count)
                .map(|i| {
                    TestFileBuilder::new(&format!("file{}.parquet", i))
                        .size((10 + i as u64 * 5) * 1024 * 1024)
                        .build()
                })
                .collect();

            let result = strategy.select(files);
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
        let config =
            CompactionPlanningConfig::new(CompactionStrategy::SmallFiles(small_files_config));

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

        let small_file_threshold = match &config.strategy {
            CompactionStrategy::SmallFiles(sf_config) => sf_config.small_file_threshold_bytes,
            _ => panic!("Expected small files config"),
        };
        for file in &result {
            assert!(file.length <= small_file_threshold);
        }

        let min_count_small_files_config = SmallFilesConfigBuilder::default()
            .small_file_threshold_bytes(20 * 1024 * 1024_u64)
            .group_filters(crate::config::GroupFilters {
                min_group_file_count: Some(3),
                min_group_size_bytes: None,
            })
            .build()
            .unwrap();
        let min_count_config = CompactionPlanningConfig::new(CompactionStrategy::SmallFiles(
            min_count_small_files_config,
        ));
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

        let default_small_files_config = SmallFilesConfigBuilder::default().build().unwrap();
        let default_config = CompactionPlanningConfig::new(CompactionStrategy::SmallFiles(
            default_small_files_config,
        ));

        match &default_config.strategy {
            CompactionStrategy::SmallFiles(config) => {
                // Default config should have Single grouping strategy with no group filters
                assert!(matches!(
                    default_config.grouping_strategy,
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
        let small_file_threshold_default = match &default_config.strategy {
            CompactionStrategy::SmallFiles(config) => config.small_file_threshold_bytes,
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
            SelectedFileGroup::new(vec![
                TestFileBuilder::new("small1.parquet")
                    .size(5 * 1024 * 1024)
                    .build(),
                TestFileBuilder::new("small2.parquet")
                    .size(10 * 1024 * 1024)
                    .build(),
            ]), // 15MB, 2 files
            SelectedFileGroup::new(vec![
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
            SelectedFileGroup::new(vec![
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
        let result_minimal =
            TestUtils::execute_strategy_flat(&strategy_minimal, test_files.clone());
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

        let large_groups = bin_pack_strategy.select(large_files);
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
        let groups = single_enum.group_files(data_files.clone());
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].data_file_count, 2);
        assert_eq!(single_enum.to_string(), "SingleGrouping");

        // Single-file case for Single
        let single = vec![
            TestFileBuilder::new("single.parquet")
                .size(20 * 1024 * 1024)
                .build(),
        ];
        let single_file_groups = single_enum.group_files(single);
        assert_eq!(single_file_groups.len(), 1);
        assert_eq!(single_file_groups[0].data_file_count, 1);
        TestUtils::assert_paths_eq(&["single.parquet"], &single_file_groups[0].data_files);

        // Empty input
        let empty_files: Vec<FileScanTask> = vec![];
        let single_empty_groups = single_enum.group_files(empty_files);
        assert_eq!(single_empty_groups.len(), 0);

        // Test BinPack variant
        let binpack_enum =
            GroupingStrategyEnum::BinPack(BinPackGroupingStrategy::new(20 * 1024 * 1024));
        let binpack_groups = binpack_enum.group_files(data_files);
        assert!(!binpack_groups.is_empty());
        assert_eq!(binpack_enum.to_string(), "BinPackGrouping[target=20MB]");
    }

    #[test]
    fn test_file_group_parallelism_calculation() {
        // Test plan assembly's parallelism calculation.
        let config = CompactionPlanningConfigBuilder::default()
            .strategy(CompactionStrategy::SmallFiles(
                crate::config::SmallFilesConfig::default(),
            ))
            .max_file_count_per_partition(5_usize) // 5 files per partition
            .max_input_parallelism(8_usize) // Max 8 parallel input tasks
            .max_output_parallelism(8_usize) // Max 8 parallel output tasks
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

        let group = SelectedFileGroup::new(files);
        let (executor_parallelism, output_parallelism) = plan_parallelism(&group, &config).unwrap();

        assert!(executor_parallelism >= 1);
        assert!(output_parallelism >= 1);
        assert!(output_parallelism <= executor_parallelism);
        assert!(executor_parallelism <= config.max_input_parallelism);

        // Test error case - empty group
        let empty_group = SelectedFileGroup::empty();
        let result = plan_parallelism(&empty_group, &config);
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
    fn test_file_group_delete_files_extraction() {
        // Test that SelectedFileGroup correctly extracts and organizes delete files
        use iceberg::spec::DataContentType;

        // Create a data file with both position and equality delete files
        let position_delete = TestUtils::create_delete_file(
            "pos_delete.parquet".to_owned(),
            DataContentType::PositionDeletes,
        );
        let mut equality_delete = TestUtils::create_delete_file(
            "eq_delete.parquet".to_owned(),
            DataContentType::EqualityDeletes,
        );
        equality_delete.file_size_in_bytes = 2048;
        equality_delete.record_count = Some(20);

        let mut data_file = TestFileBuilder::new("data.parquet")
            .size(10 * 1024 * 1024)
            .build();
        data_file.record_count = Some(1000);
        data_file.deletes = vec![position_delete, equality_delete];

        let group = SelectedFileGroup::new(vec![data_file]);

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
        use iceberg::spec::DataContentType;

        let mut shared_pos_delete = TestUtils::create_delete_file(
            "shared_pos_delete.parquet".to_owned(),
            DataContentType::PositionDeletes,
        );
        shared_pos_delete.file_size_in_bytes = 512;

        let mut f1 = TestFileBuilder::new("d1.parquet")
            .size(4 * 1024 * 1024)
            .build();
        f1.deletes = vec![shared_pos_delete.clone()];

        let mut f2 = TestFileBuilder::new("d2.parquet")
            .size(4 * 1024 * 1024)
            .build();
        f2.deletes = vec![shared_pos_delete];

        let group = SelectedFileGroup::new(vec![f1, f2]);
        // Dedup should keep one position delete
        assert_eq!(group.position_delete_files.len(), 1);
        assert_eq!(
            group.position_delete_files[0].data_file_path,
            "shared_pos_delete.parquet"
        );

        // Heuristic output parallelism: data total is 8MB, below default 1GB target, so 1 output
        let config = CompactionPlanningConfigBuilder::default()
            .strategy(CompactionStrategy::SmallFiles(
                crate::config::SmallFilesConfig::default(),
            ))
            .max_file_count_per_partition(1_usize)
            .max_input_parallelism(8_usize)
            .max_output_parallelism(8_usize)
            .enable_heuristic_output_parallelism(true)
            .build()
            .unwrap();

        let (exec_p, out_p) = plan_parallelism(&group, &config).unwrap();
        assert!(exec_p >= 1);
        assert_eq!(
            out_p, 1,
            "Heuristic should force single output when data is tiny"
        );
    }

    #[test]
    fn test_file_group_keeps_distinct_puffin_blobs() {
        use iceberg::spec::{DataContentType, DataFileFormat};

        let mut first = TestUtils::create_delete_file(
            "shared-dv.puffin".to_owned(),
            DataContentType::PositionDeletes,
        );
        first.file_format = DataFileFormat::Puffin;
        first.content_offset = Some(10);
        first.content_size_in_bytes = Some(20);

        let mut second = first.clone();
        second.content_offset = Some(40);
        second.content_size_in_bytes = Some(30);

        let mut data_file = TestFileBuilder::new("data.parquet").build();
        data_file.deletes = vec![first, second];

        let mut ranges = SelectedFileGroup::new(vec![data_file])
            .position_delete_files
            .into_iter()
            .map(|task| (task.start, task.length))
            .collect::<Vec<_>>();
        ranges.sort_unstable();

        assert_eq!(ranges, vec![(10, 20), (40, 30)]);
    }

    #[test]
    fn test_file_group_delete_files_dedup_mixed_types() {
        // Test deduplication of equality deletes and mixed delete types
        use iceberg::spec::DataContentType;

        let mut shared_eq_delete = TestUtils::create_delete_file(
            "shared_eq_delete.parquet".to_owned(),
            DataContentType::EqualityDeletes,
        );
        shared_eq_delete.record_count = Some(5);
        let mut pos_delete = TestUtils::create_delete_file(
            "pos_delete.parquet".to_owned(),
            DataContentType::PositionDeletes,
        );
        pos_delete.file_size_in_bytes = 512;
        pos_delete.record_count = Some(3);

        let mut f1 = TestFileBuilder::new("d1.parquet")
            .size(5 * 1024 * 1024)
            .build();
        f1.deletes = vec![shared_eq_delete.clone(), pos_delete.clone()];

        let mut f2 = TestFileBuilder::new("d2.parquet")
            .size(5 * 1024 * 1024)
            .build();
        f2.deletes = vec![shared_eq_delete, pos_delete];

        let group = SelectedFileGroup::new(vec![f1, f2]);

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
        let config = CompactionPlanningConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::BinPack(binpack_config))
            .max_file_count_per_partition(1_usize)
            .max_input_parallelism(8_usize)
            .max_output_parallelism(8_usize)
            .enable_heuristic_output_parallelism(true)
            .build()
            .unwrap();

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

        let groups = strategy.select(test_files.clone());

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
        let config = CompactionPlanningConfig::new(CompactionStrategy::FilesWithDeletes(
            files_with_deletes_config,
        ));
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
        let config = CompactionPlanningConfigBuilder::default()
            .grouping_strategy(crate::config::GroupingStrategy::Single)
            .build()
            .unwrap();

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

        let result = strategy.select(test_files.clone());

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
        let config = CompactionPlanningConfigBuilder::default()
            .strategy(CompactionStrategy::SmallFiles(
                crate::config::SmallFilesConfig::default(),
            ))
            .max_file_count_per_partition(1_usize)
            .max_input_parallelism(1000_usize)
            .max_output_parallelism(1000_usize)
            .build()
            .unwrap();

        // Create a group with very large file size (near u64::MAX / 2)
        let huge_file = TestFileBuilder::new("huge.parquet")
            .size(u64::MAX / 2)
            .build();

        let group = SelectedFileGroup::new(vec![huge_file]);
        let result = plan_parallelism(&group, &config);

        // Should not panic or overflow, should calculate valid parallelism
        assert!(result.is_ok());
        let (exec_p, out_p) = result.unwrap();
        assert!(exec_p >= 1);
        assert!(out_p >= 1);
        assert!(exec_p <= config.max_input_parallelism);
    }

    // Test 1: Output parallelism limiting small files
    #[test]
    fn test_output_parallelism_prevents_small_files() {
        let planning_config = CompactionPlanningConfigBuilder::default()
            .strategy(CompactionStrategy::SmallFiles(
                crate::config::SmallFilesConfig::default(),
            ))
            .target_file_size_bytes(1024 * 1024 * 1024_u64) // 1GB target
            .max_input_parallelism(32_usize)
            .max_output_parallelism(4_usize) // Limit to 4 output files
            .enable_heuristic_output_parallelism(false)
            .build()
            .unwrap();

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

        let tiny_group = SelectedFileGroup::new(tiny_files);
        let (_, tiny_output_p) = plan_parallelism(&tiny_group, &planning_config).unwrap();

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
            tiny_total_size < planning_config.target_file_size_bytes,
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

        let group = SelectedFileGroup::new(files);
        let (_, output_parallelism) = plan_parallelism(&group, &planning_config).unwrap();

        assert_eq!(
            output_parallelism, 4,
            "Output parallelism should be capped to prevent creating ~10 files"
        );

        // Verify each output file would be >= target size
        let avg_output_size = group.input_total_bytes() / output_parallelism as u64;
        assert!(
            avg_output_size >= planning_config.target_file_size_bytes,
            "Each output file should be >= target ({}GB >= 1GB)",
            avg_output_size / 1024 / 1024 / 1024
        );

        // Case 2: Very small input should produce only 1 output file
        let small_files = vec![
            TestFileBuilder::new("small.parquet")
                .size(100 * 1024 * 1024) // 100MB (< 1GB target)
                .build(),
        ];

        let small_group = SelectedFileGroup::new(small_files);
        let (_, small_output_p) = plan_parallelism(&small_group, &planning_config).unwrap();

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

        let exact_group = SelectedFileGroup::new(exact_files);
        let (_, exact_output_p) = plan_parallelism(&exact_group, &planning_config).unwrap();

        assert_eq!(
            exact_output_p, 4,
            "4GB input with 1GB target and max=4 should produce 4 files"
        );

        // Case 4: Small remainder that should be absorbed
        // 3.1GB total (3GB + 100MB small remainder)
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

        let small_remainder_group = SelectedFileGroup::new(files_small_remainder);
        let (_, small_remainder_output_p) =
            plan_parallelism(&small_remainder_group, &planning_config).unwrap();

        // Should still be capped at 4, and the small remainder should be absorbed
        assert_eq!(
            small_remainder_output_p, 3,
            "10.1GB input with tiny remainder should still produce 4 files, absorbing the small remainder"
        );

        let total_with_small_remainder = small_remainder_group.input_total_bytes();
        let avg_with_small_remainder = total_with_small_remainder / small_remainder_output_p as u64;

        // Each file should be > 1GB (since we're distributing 10.1GB across 4 files)
        assert!(
            avg_with_small_remainder > planning_config.target_file_size_bytes,
            "Small remainder should be absorbed, making each file > target size"
        );
    }

    /// Test 2: Input parallelism is constrained by multiple factors
    #[test]
    fn test_input_parallelism_multiple_constraints() {
        let planning_config = CompactionPlanningConfigBuilder::default()
            .strategy(CompactionStrategy::SmallFiles(
                crate::config::SmallFilesConfig::default(),
            ))
            .target_file_size_bytes(1024 * 1024 * 1024_u64) // 1GB target
            .max_file_count_per_partition(10_usize) // Max 10 files per partition
            .max_input_parallelism(8_usize) // Cap at 8
            .max_output_parallelism(4_usize)
            .enable_heuristic_output_parallelism(false)
            .build()
            .unwrap();

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

        let group1 = SelectedFileGroup::new(many_small_files);
        let (input_p1, _) = plan_parallelism(&group1, &planning_config).unwrap();

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

        let group2 = SelectedFileGroup::new(medium_count_files);
        let (input_p2, _) = plan_parallelism(&group2, &planning_config).unwrap();

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

        let group3 = SelectedFileGroup::new(few_files);
        let (input_p3, _) = plan_parallelism(&group3, &planning_config).unwrap();

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

        let group4 = SelectedFileGroup::new(large_files);
        let (input_p4, _) = plan_parallelism(&group4, &planning_config).unwrap();

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

    fn partitioned_file(name: &str, partition_num: i32) -> FileScanTask {
        TestFileBuilder::new(name)
            .size(1024 * 1024)
            .with_partition(create_partition_value(partition_num))
            .with_schema(get_test_schema_with_partition())
            .build()
    }

    fn delete_heavy_partitioned_file(name: &str, partition_num: i32) -> FileScanTask {
        TestFileBuilder::new(name)
            .size(1024 * 1024)
            .with_deletes()
            .with_partition(create_partition_value(partition_num))
            .with_schema(get_test_schema_with_partition())
            .build()
    }

    fn min_file_count_filter(min_file_count: usize) -> crate::config::GroupFilters {
        crate::config::GroupFilters {
            min_group_file_count: Some(min_file_count),
            min_group_size_bytes: None,
        }
    }

    #[test]
    fn test_grouping_strategy_enum_group_files_keeps_partition_scope() {
        let files = vec![
            partitioned_file("p0_file1.parquet", 0),
            partitioned_file("p0_file2.parquet", 0),
            partitioned_file("p1_file1.parquet", 1),
            partitioned_file("p2_file1.parquet", 2),
            partitioned_file("p2_file2.parquet", 2),
        ];

        let strategy = GroupingStrategyEnum::Single(SingleGroupingStrategy);
        let groups = strategy.group_files(files);
        let mut file_counts = groups
            .iter()
            .map(|group| group.data_file_count)
            .collect::<Vec<_>>();
        file_counts.sort_unstable();

        assert_eq!(file_counts, vec![1, 2, 2]);
    }

    #[test]
    fn test_table_scope_single_grouping_does_not_split_partitions() {
        let files = vec![
            partitioned_file("p0_file1.parquet", 0),
            partitioned_file("p0_file2.parquet", 0),
            partitioned_file("p1_file1.parquet", 1),
            partitioned_file("p2_file1.parquet", 2),
            partitioned_file("p2_file2.parquet", 2),
        ];

        let strategy = PlanStrategy::new_with_options(
            PlanStrategyOptions::new(
                vec![],
                GroupingStrategyEnum::Single(SingleGroupingStrategy),
                vec![],
            )
            .with_file_group_scope(FileGroupScope::Table),
        );
        let groups = strategy.group_files(files);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].data_file_count, 5);
    }

    #[test]
    fn test_small_files_table_scope_applies_group_filter_across_partitions() {
        let files = vec![
            partitioned_file("p0_file1.parquet", 0),
            partitioned_file("p0_file2.parquet", 0),
            partitioned_file("p1_file1.parquet", 1),
            partitioned_file("p1_file2.parquet", 1),
            partitioned_file("p2_file1.parquet", 2),
        ];
        let group_filters = min_file_count_filter(5);
        let table_scope_config = CompactionPlanningConfigBuilder::default()
            .strategy(CompactionStrategy::SmallFiles(
                SmallFilesConfigBuilder::default()
                    .group_filters(group_filters.clone())
                    .build()
                    .unwrap(),
            ))
            .grouping_strategy(GroupingStrategy::Single)
            .file_group_scope(FileGroupScope::Table)
            .build()
            .unwrap();
        let partition_scope_config = CompactionPlanningConfigBuilder::default()
            .strategy(CompactionStrategy::SmallFiles(
                SmallFilesConfigBuilder::default()
                    .group_filters(group_filters)
                    .build()
                    .unwrap(),
            ))
            .grouping_strategy(GroupingStrategy::Single)
            .build()
            .unwrap();

        let table_scope_groups = PlanStrategy::from(&table_scope_config).select(files.clone());
        let partition_scope_groups = PlanStrategy::from(&partition_scope_config).select(files);

        assert_eq!(table_scope_groups.len(), 1);
        assert_eq!(table_scope_groups[0].data_file_count, 5);
        assert_eq!(partition_scope_groups.len(), 0);
    }

    #[test]
    fn test_files_with_deletes_table_scope_applies_group_filter_across_partitions() {
        let files = vec![
            delete_heavy_partitioned_file("p0_file1.parquet", 0),
            delete_heavy_partitioned_file("p1_file1.parquet", 1),
            delete_heavy_partitioned_file("p2_file1.parquet", 2),
        ];
        let group_filters = min_file_count_filter(3);
        let table_scope_config = CompactionPlanningConfigBuilder::default()
            .strategy(CompactionStrategy::FilesWithDeletes(
                FilesWithDeletesConfigBuilder::default()
                    .min_delete_file_count_threshold(1_usize)
                    .group_filters(group_filters.clone())
                    .build()
                    .unwrap(),
            ))
            .grouping_strategy(GroupingStrategy::Single)
            .file_group_scope(FileGroupScope::Table)
            .build()
            .unwrap();
        let partition_scope_config = CompactionPlanningConfigBuilder::default()
            .strategy(CompactionStrategy::FilesWithDeletes(
                FilesWithDeletesConfigBuilder::default()
                    .min_delete_file_count_threshold(1_usize)
                    .group_filters(group_filters)
                    .build()
                    .unwrap(),
            ))
            .grouping_strategy(GroupingStrategy::Single)
            .build()
            .unwrap();

        let table_scope_groups = PlanStrategy::from(&table_scope_config).select(files.clone());
        let partition_scope_groups = PlanStrategy::from(&partition_scope_config).select(files);

        assert_eq!(table_scope_groups.len(), 1);
        assert_eq!(table_scope_groups[0].data_file_count, 3);
        assert_eq!(partition_scope_groups.len(), 0);
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
        // 3. Execute strategy
        let groups = strategy.select(files);

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
            .group_filters(crate::config::GroupFilters {
                min_group_file_count: Some(2),
                min_group_size_bytes: None,
            })
            .build()
            .unwrap();
        let config =
            CompactionPlanningConfig::new(CompactionStrategy::SmallFiles(small_files_config));
        let strategy = PlanStrategy::from(&config);

        // 3. Execute strategy
        let groups = strategy.select(files);

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
        let groups = strategy.select(files);

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
        let groups = strategy.select(files);

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
        let groups = strategy.select(files);

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
        let groups = strategy.select(files);

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

        let groups = strategy.select(files);

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

        let groups = strategy.select(files);

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

    /// `target_file_size = 0` must not panic during plan assembly.
    #[test]
    fn test_target_file_size_zero_does_not_panic() {
        let config = CompactionPlanningConfigBuilder::default()
            .target_file_size_bytes(0_u64)
            .build()
            .unwrap();
        let selected_file_group = SelectedFileGroup::new(vec![
            TestFileBuilder::new("small.parquet").size(1024).build(),
        ]);

        assert_eq!(
            plan_parallelism(&selected_file_group, &config).unwrap(),
            (1, 1)
        );
    }
}
