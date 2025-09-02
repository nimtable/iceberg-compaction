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
use crate::config::CompactionPlanningConfig;
use iceberg::scan::FileScanTask;

/// Strategy trait for filtering files during compaction
///
/// This trait is designed for zero-cost abstractions with compile-time optimization.
/// All implementations use static dispatch for maximum performance.
pub trait StaticFileStrategy {
    /// Filter the input data files as an iterator for memory efficiency
    ///
    /// This processes files in a streaming fashion without loading all files into memory at once.
    fn filter_iter<I>(&self, data_files: I) -> impl Iterator<Item = FileScanTask>
    where
        I: Iterator<Item = FileScanTask>;

    /// Get a description of this strategy for logging/debugging
    fn description(&self) -> String;
}

/// Static composition of two strategies using nested types
#[derive(Debug)]
pub struct Compose<A, B> {
    first: A,
    second: B,
}

impl<A, B> Compose<A, B> {
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

impl<A: StaticFileStrategy, B: StaticFileStrategy> StaticFileStrategy for Compose<A, B> {
    fn filter_iter<I>(&self, data_files: I) -> impl Iterator<Item = FileScanTask>
    where
        I: Iterator<Item = FileScanTask>,
    {
        // Chain the two strategies: first applies to input, second applies to first's output
        self.second.filter_iter(self.first.filter_iter(data_files))
    }

    fn description(&self) -> String {
        format!(
            "{} -> {}",
            self.first.description(),
            self.second.description()
        )
    }
}

/// No-op strategy that passes through all files unchanged
#[derive(Debug)]
pub struct NoopStrategy;

impl StaticFileStrategy for NoopStrategy {
    fn filter_iter<I>(&self, data_files: I) -> impl Iterator<Item = FileScanTask>
    where
        I: Iterator<Item = FileScanTask>,
    {
        // No-op: just pass through the iterator
        data_files
    }

    fn description(&self) -> String {
        "Noop".to_owned()
    }
}

/// Strategy for filtering out files that have associated delete files
#[derive(Debug)]
pub struct NoDeleteFilesStrategy;

impl StaticFileStrategy for NoDeleteFilesStrategy {
    fn filter_iter<I>(&self, data_files: I) -> impl Iterator<Item = FileScanTask>
    where
        I: Iterator<Item = FileScanTask>,
    {
        // Stream processing: filter out files with delete files
        data_files.filter(|task| task.deletes.is_empty())
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

impl StaticFileStrategy for SizeFilterStrategy {
    fn filter_iter<I>(&self, data_files: I) -> impl Iterator<Item = FileScanTask>
    where
        I: Iterator<Item = FileScanTask>,
    {
        // Stream processing: filter by size without collecting
        let min_size = self.min_size;
        let max_size = self.max_size;

        data_files.filter(move |task| {
            let file_size = task.length;
            match (min_size, max_size) {
                (Some(min), Some(max)) => file_size >= min && file_size <= max,
                (Some(min), None) => file_size >= min,
                (None, Some(max)) => file_size <= max,
                (None, None) => true,
            }
        })
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

/// Iterator that tracks total size and stops when limit is exceeded
pub struct SizeLimitIterator<I> {
    inner: I,
    max_total_size: u64,
    current_total: u64,
}

impl<I: Iterator<Item = FileScanTask>> Iterator for SizeLimitIterator<I> {
    type Item = FileScanTask;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(task) = self.inner.next() {
            let file_size = task.length;
            if self.current_total + file_size <= self.max_total_size {
                self.current_total += file_size;
                return Some(task);
            } else {
                // Size limit exceeded, stop iteration
                return None;
            }
        }
        None
    }
}

impl StaticFileStrategy for TaskSizeLimitStrategy {
    fn filter_iter<I>(&self, data_files: I) -> impl Iterator<Item = FileScanTask>
    where
        I: Iterator<Item = FileScanTask>,
    {
        // Stream processing with stateful size tracking
        SizeLimitIterator {
            inner: data_files,
            max_total_size: self.max_total_size,
            current_total: 0,
        }
    }

    fn description(&self) -> String {
        format!(
            "TaskSizeLimit[{}GB]",
            self.max_total_size / 1024 / 1024 / 1024
        )
    }
}

/// Strategy for ensuring minimum file count
#[derive(Debug)]
pub struct MinFileCountStrategy {
    pub min_file_count: usize,
}

impl MinFileCountStrategy {
    /// Create a new MinFileCountStrategy with minimum file count requirement
    pub fn new(min_file_count: usize) -> Self {
        Self { min_file_count }
    }
}

/// Iterator that ensures minimum file count is met
pub struct MinFileCountIterator<I> {
    inner: I,
    min_file_count: usize,
    file_count: usize,
    buffered_files: Vec<FileScanTask>,
    bypass_mode: bool, // Once we have enough files, we bypass the check
}

impl<I: Iterator<Item = FileScanTask>> Iterator for MinFileCountIterator<I> {
    type Item = FileScanTask;

    fn next(&mut self) -> Option<Self::Item> {
        // If we're in bypass mode, just pass through the iterator
        if self.bypass_mode {
            return self.inner.next();
        }

        // Continue collecting files until we reach min_file_count
        while self.file_count < self.min_file_count {
            if let Some(file) = self.inner.next() {
                self.file_count += 1;
                self.buffered_files.push(file);
            } else {
                // No more files available, and we don't have enough
                // Return None to indicate no files should be processed
                return None;
            }
        }

        // We now have at least min_file_count files
        if !self.buffered_files.is_empty() {
            // Return the first buffered file
            let file = self.buffered_files.remove(0);

            // If buffer is empty and we have enough files, switch to bypass mode
            if self.buffered_files.is_empty() {
                self.bypass_mode = true;
            }

            Some(file)
        } else {
            // Buffer is empty, we're in bypass mode, pass through
            self.bypass_mode = true;
            self.inner.next()
        }
    }
}

impl StaticFileStrategy for MinFileCountStrategy {
    fn filter_iter<I>(&self, data_files: I) -> impl Iterator<Item = FileScanTask>
    where
        I: Iterator<Item = FileScanTask>,
    {
        MinFileCountIterator {
            inner: data_files,
            min_file_count: self.min_file_count,
            file_count: 0,
            buffered_files: Vec::new(),
            bypass_mode: false,
        }
    }

    fn description(&self) -> String {
        format!("MinFileCount[{}]", self.min_file_count)
    }
}

/// Factory for creating file strategies based on compaction type and configuration
pub struct FileStrategyFactory;

// Type aliases for common strategy combinations
pub type SmallFilesStrategy =
    Compose<Compose<NoDeleteFilesStrategy, SizeFilterStrategy>, TaskSizeLimitStrategy>;

/// Unified strategy enum that can hold different static strategy types
///
/// This enum provides a type-erased interface for different file strategies
/// with zero-cost static dispatch. All variants use compile-time known types
/// for maximum performance.
#[derive(Debug)]
pub enum UnifiedStrategy {
    /// No-operation strategy that passes all files through
    Noop(NoopStrategy),
    /// Strategy optimized for small files compaction
    SmallFiles(SmallFilesStrategy),
}

impl UnifiedStrategy {
    /// Filter files using the appropriate strategy
    ///
    /// This method returns a Vec to provide a unified interface while maintaining
    /// static dispatch for optimal performance. For streaming processing of very
    /// large datasets, consider using the individual strategy types directly.
    pub fn filter_iter<I>(&self, data_files: I) -> Vec<FileScanTask>
    where
        I: Iterator<Item = FileScanTask>,
    {
        match self {
            UnifiedStrategy::Noop(strategy) => {
                StaticFileStrategy::filter_iter(strategy, data_files).collect()
            }
            UnifiedStrategy::SmallFiles(strategy) => {
                StaticFileStrategy::filter_iter(strategy, data_files).collect()
            }
        }
    }

    /// Get a description of this strategy for logging/debugging
    pub fn description(&self) -> String {
        match self {
            UnifiedStrategy::Noop(strategy) => StaticFileStrategy::description(strategy),
            UnifiedStrategy::SmallFiles(strategy) => StaticFileStrategy::description(strategy),
        }
    }

    /// Create a `UnifiedStrategy` from any `StaticFileStrategy`
    ///
    /// This is a convenience method that allows you to wrap any static strategy
    /// in the `UnifiedStrategy` enum for use with the unified interface.
    pub fn from_static<T: Into<UnifiedStrategy>>(strategy: T) -> Self {
        strategy.into()
    }
}

// Implement From trait for easy conversion from static strategies
impl From<NoopStrategy> for UnifiedStrategy {
    fn from(strategy: NoopStrategy) -> Self {
        UnifiedStrategy::Noop(strategy)
    }
}

impl From<SmallFilesStrategy> for UnifiedStrategy {
    fn from(strategy: SmallFilesStrategy) -> Self {
        UnifiedStrategy::SmallFiles(strategy)
    }
}

impl FileStrategyFactory {
    /// Create strategy for small files compaction
    ///
    /// Returns a statically typed strategy that filters out delete files,
    /// applies size filtering, and limits total task size.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use iceberg_compaction_core::file_selection::strategy::FileStrategyFactory;
    /// # use iceberg_compaction_core::config::CompactionPlanningConfig;
    /// # let config = CompactionPlanningConfig::default();
    /// let strategy = FileStrategyFactory::create_small_files_strategy(&config);
    /// ```
    pub fn create_small_files_strategy(config: &CompactionPlanningConfig) -> SmallFilesStrategy {
        // Build the strategy: NoDeleteFiles -> SizeFilter -> TaskSizeLimit
        Compose::new(
            Compose::new(
                NoDeleteFilesStrategy,
                SizeFilterStrategy {
                    min_size: None,
                    max_size: Some(config.small_file_threshold),
                },
            ),
            TaskSizeLimitStrategy {
                max_total_size: config.max_task_total_size,
            },
        )
    }

    /// Create a no-op strategy that passes all files through
    pub fn create_noop_strategy() -> NoopStrategy {
        NoopStrategy
    }

    /// Create a custom strategy builder for advanced use cases
    ///
    /// This method is for **advanced users** who need custom strategy combinations
    /// that aren't covered by the predefined strategies.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use iceberg_compaction_core::file_selection::strategy::FileStrategyFactory;
    /// // Custom strategy for specific requirements
    /// let strategy = FileStrategyFactory::builder()
    ///     .exclude_delete_files()                                 // Skip files with delete files
    ///     .filter_by_size(Some(1024*1024), Some(100*1024*1024))  // 1MB-100MB files only
    ///     .require_min_files(3)                                   // Need at least 3 files to proceed
    ///     .limit_task_size(20 * 1024*1024*1024)                  // 20GB task limit
    ///     .build();
    /// ```
    pub fn builder() -> StrategyBuilder<NoopStrategy> {
        StrategyBuilder::new()
    }

    /// Create a file strategy based on compaction type and configuration
    ///
    /// This is the main entry point for creating file strategies. It returns
    /// a `UnifiedStrategy` that can handle different types of compaction strategies
    /// in a type-safe manner with zero-cost static dispatch.
    ///
    /// # Arguments
    /// * `compaction_type` - The type of compaction to perform
    /// * `config` - The compaction configuration
    ///
    /// # Returns
    /// A `UnifiedStrategy` containing the appropriate file strategy for the given compaction type
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
    ) -> UnifiedStrategy {
        match compaction_type {
            CompactionType::MergeSmallDataFiles => Self::create_small_files_strategy(config).into(),
            CompactionType::Full => Self::create_noop_strategy().into(),
        }
    }
}

/// Builder for creating custom file strategies using static composition
pub struct StrategyBuilder<T> {
    strategy: T,
}

impl StrategyBuilder<NoopStrategy> {
    pub fn new() -> Self {
        Self {
            strategy: NoopStrategy,
        }
    }
}

impl Default for StrategyBuilder<NoopStrategy> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> StrategyBuilder<T> {
    /// Add a size filter to the strategy chain
    pub fn filter_by_size(
        self,
        min_size: Option<u64>,
        max_size: Option<u64>,
    ) -> StrategyBuilder<Compose<T, SizeFilterStrategy>> {
        StrategyBuilder {
            strategy: Compose::new(self.strategy, SizeFilterStrategy { min_size, max_size }),
        }
    }

    /// Add delete files exclusion to the strategy chain
    pub fn exclude_delete_files(self) -> StrategyBuilder<Compose<T, NoDeleteFilesStrategy>> {
        StrategyBuilder {
            strategy: Compose::new(self.strategy, NoDeleteFilesStrategy),
        }
    }

    /// Add task size limit to the strategy chain
    pub fn limit_task_size(
        self,
        max_total_size: u64,
    ) -> StrategyBuilder<Compose<T, TaskSizeLimitStrategy>> {
        StrategyBuilder {
            strategy: Compose::new(self.strategy, TaskSizeLimitStrategy { max_total_size }),
        }
    }

    /// Add minimum file count requirement to the strategy chain
    /// If fewer than min_file_count files are available, no files will be returned
    /// Once min_file_count is reached, all subsequent files are passed through
    pub fn require_min_files(
        self,
        min_file_count: usize,
    ) -> StrategyBuilder<Compose<T, MinFileCountStrategy>> {
        StrategyBuilder {
            strategy: Compose::new(self.strategy, MinFileCountStrategy::new(min_file_count)),
        }
    }

    /// Add a custom strategy to the chain
    pub fn then<U>(self, next_strategy: U) -> StrategyBuilder<Compose<T, U>> {
        StrategyBuilder {
            strategy: Compose::new(self.strategy, next_strategy),
        }
    }

    /// Build the final strategy
    pub fn build(self) -> T {
        self.strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompactionPlanningConfigBuilder;

    // Helper function to create test FileScanTask
    fn create_test_file_scan_task(file_path: &str, file_size: u64) -> FileScanTask {
        use iceberg::spec::{DataContentType, DataFileFormat};
        use std::sync::Arc;

        FileScanTask {
            start: 0,
            length: file_size,
            record_count: Some(100),
            data_file_path: file_path.to_owned(),
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: Arc::new(iceberg::spec::Schema::builder().build().unwrap()),
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
                schema: Arc::new(iceberg::spec::Schema::builder().build().unwrap()),
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
            schema: Arc::new(iceberg::spec::Schema::builder().build().unwrap()),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes,
            sequence_number: 1,
            equality_ids: vec![],
            file_size_in_bytes: file_size,
        }
    }

    #[test]
    fn test_noop_strategy() {
        let strategy = NoopStrategy;
        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
        ];

        let result_data: Vec<FileScanTask> = strategy
            .filter_iter(data_files.clone().into_iter())
            .collect();

        assert_eq!(result_data.len(), data_files.len());
        assert_eq!(StaticFileStrategy::description(&strategy), "Noop");
    }

    #[test]
    fn test_size_filter_strategy() {
        let strategy = SizeFilterStrategy {
            min_size: Some(5 * 1024 * 1024),  // 5MB min
            max_size: Some(50 * 1024 * 1024), // 50MB max
        };

        // Test description
        assert_eq!(
            StaticFileStrategy::description(&strategy),
            "SizeFilter[5-50MB]"
        );

        // Test filtering logic
        let data_files = vec![
            create_test_file_scan_task("small_file.parquet", 2 * 1024 * 1024), // 2MB - should be filtered out
            create_test_file_scan_task("medium_file1.parquet", 10 * 1024 * 1024), // 10MB - should pass
            create_test_file_scan_task("medium_file2.parquet", 30 * 1024 * 1024), // 30MB - should pass
            create_test_file_scan_task("large_file.parquet", 100 * 1024 * 1024), // 100MB - should be filtered out
        ];

        let result: Vec<FileScanTask> = strategy.filter_iter(data_files.into_iter()).collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "medium_file1.parquet");
        assert_eq!(result[1].data_file_path, "medium_file2.parquet");

        // Test edge cases
        let edge_case_strategy = SizeFilterStrategy {
            min_size: Some(1024 * 1024),
            max_size: Some(32 * 1024 * 1024),
        };
        assert_eq!(
            StaticFileStrategy::description(&edge_case_strategy),
            "SizeFilter[1-32MB]"
        );
    }

    #[test]
    fn test_compose_strategy() {
        let strategy = Compose::new(
            SizeFilterStrategy {
                min_size: None,
                max_size: Some(20 * 1024 * 1024), // Only files <= 20MB
            },
            TaskSizeLimitStrategy {
                max_total_size: 25 * 1024 * 1024, // Total limit 25MB
            },
        );

        // Test description
        assert_eq!(
            StaticFileStrategy::description(&strategy),
            "SizeFilter[<20MB] -> TaskSizeLimit[0GB]"
        );

        // Test filtering logic
        let data_files = vec![
            create_test_file_scan_task("small1.parquet", 5 * 1024 * 1024), // 5MB - should pass (total: 5MB)
            create_test_file_scan_task("small2.parquet", 10 * 1024 * 1024), // 10MB - should pass (total: 15MB)
            create_test_file_scan_task("small3.parquet", 15 * 1024 * 1024), // 15MB - should be filtered out by size limit (would exceed 25MB)
            create_test_file_scan_task("large.parquet", 30 * 1024 * 1024), // 30MB - should be filtered out by size filter
        ];

        let result: Vec<FileScanTask> = strategy.filter_iter(data_files.into_iter()).collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "small1.parquet");
        assert_eq!(result[1].data_file_path, "small2.parquet");
    }

    #[test]
    fn test_strategy_builder() {
        let strategy = FileStrategyFactory::builder()
            .filter_by_size(None, Some(32 * 1024 * 1024))
            .limit_task_size(10 * 1024 * 1024 * 1024)
            .build();

        let description = StaticFileStrategy::description(&strategy);
        assert!(description.contains("SizeFilter"));
        assert!(description.contains("TaskSizeLimit"));

        // Test builder with multiple filters
        let complex_strategy = FileStrategyFactory::builder()
            .exclude_delete_files()
            .filter_by_size(Some(1024 * 1024), Some(100 * 1024 * 1024))
            .limit_task_size(5 * 1024 * 1024 * 1024)
            .build();

        let complex_desc = StaticFileStrategy::description(&complex_strategy);
        assert!(complex_desc.contains("NoDeleteFiles"));
        assert!(complex_desc.contains("SizeFilter"));
        assert!(complex_desc.contains("TaskSizeLimit"));
    }

    #[test]
    fn test_file_strategy_factory() {
        let config = CompactionPlanningConfigBuilder::default().build().unwrap();

        // Test individual strategy creation
        let noop_strategy = FileStrategyFactory::create_noop_strategy();
        assert_eq!(StaticFileStrategy::description(&noop_strategy), "Noop");

        let small_files_strategy = FileStrategyFactory::create_small_files_strategy(&config);
        assert!(StaticFileStrategy::description(&small_files_strategy).contains("NoDeleteFiles"));
        assert!(StaticFileStrategy::description(&small_files_strategy).contains("SizeFilter"));
        assert!(StaticFileStrategy::description(&small_files_strategy).contains("TaskSizeLimit"));

        // Test unified strategy creation
        let small_files_unified = FileStrategyFactory::create_files_strategy(
            crate::compaction::CompactionType::MergeSmallDataFiles,
            &config,
        );
        assert!(small_files_unified.description().contains("NoDeleteFiles"));
        assert!(small_files_unified.description().contains("SizeFilter"));
        assert!(small_files_unified.description().contains("TaskSizeLimit"));

        let full_unified = FileStrategyFactory::create_files_strategy(
            crate::compaction::CompactionType::Full,
            &config,
        );
        assert_eq!(full_unified.description(), "Noop");
    }

    #[test]
    fn test_unified_strategy() {
        let config = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(10 * 1024 * 1024) // 10MB threshold
            .max_task_total_size(100 * 1024 * 1024) // 100MB task limit
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

        let result = strategy.filter_iter(data_files.into_iter());

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "small1.parquet");
        assert_eq!(result[1].data_file_path, "small3.parquet");

        // Verify all selected files are under the threshold and have no deletes
        for file in &result {
            assert!(file.length <= config.small_file_threshold);
            assert!(file.deletes.is_empty());
        }

        // Test From trait conversions
        let noop_unified: UnifiedStrategy = NoopStrategy.into();
        assert_eq!(noop_unified.description(), "Noop");

        let small_files_strategy = FileStrategyFactory::create_small_files_strategy(&config);
        let small_files_unified: UnifiedStrategy = small_files_strategy.into();
        assert!(small_files_unified.description().contains("NoDeleteFiles"));

        // Test from_static method
        let noop_unified_2 = UnifiedStrategy::from_static(NoopStrategy);
        assert_eq!(noop_unified_2.description(), "Noop");
    }

    #[test]
    fn test_no_delete_files_strategy() {
        let strategy = NoDeleteFilesStrategy;

        let data_files = vec![
            create_test_file_scan_task_with_deletes("file1.parquet", 10 * 1024 * 1024, false), // No deletes - should pass
            create_test_file_scan_task_with_deletes("file2.parquet", 20 * 1024 * 1024, true), // Has deletes - should be filtered out
            create_test_file_scan_task_with_deletes("file3.parquet", 15 * 1024 * 1024, false), // No deletes - should pass
        ];

        let result: Vec<FileScanTask> = strategy.filter_iter(data_files.into_iter()).collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "file1.parquet");
        assert_eq!(result[1].data_file_path, "file3.parquet");
        assert_eq!(StaticFileStrategy::description(&strategy), "NoDeleteFiles");
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

        let result: Vec<FileScanTask> = strategy.filter_iter(data_files.into_iter()).collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "file1.parquet");
        assert_eq!(result[1].data_file_path, "file2.parquet");
    }

    #[test]
    fn test_min_file_count_strategy() {
        let strategy = MinFileCountStrategy::new(3);

        // Test description
        assert_eq!(
            StaticFileStrategy::description(&strategy),
            "MinFileCount[3]"
        );

        // Test with exactly minimum files
        let data_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
            create_test_file_scan_task("file3.parquet", 15 * 1024 * 1024),
        ];

        let result: Vec<FileScanTask> = strategy
            .filter_iter(data_files.clone().into_iter())
            .collect();
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

        let result_more: Vec<FileScanTask> = strategy.filter_iter(more_files.into_iter()).collect();
        assert_eq!(result_more.len(), 5); // Returns all files when more than minimum

        // Test with fewer than minimum files
        let fewer_files = vec![
            create_test_file_scan_task("file1.parquet", 10 * 1024 * 1024),
            create_test_file_scan_task("file2.parquet", 20 * 1024 * 1024),
        ];

        let result_fewer: Vec<FileScanTask> =
            strategy.filter_iter(fewer_files.into_iter()).collect();
        assert_eq!(result_fewer.len(), 0); // Returns no files when not enough files

        // Test with minimum count of 0 (should pass all files)
        let zero_strategy = MinFileCountStrategy::new(0);
        let result_zero: Vec<FileScanTask> = zero_strategy
            .filter_iter(
                vec![create_test_file_scan_task(
                    "file1.parquet",
                    10 * 1024 * 1024,
                )]
                .into_iter(),
            )
            .collect();
        assert_eq!(result_zero.len(), 1);

        // Test with minimum count of 1
        let one_strategy = MinFileCountStrategy::new(1);
        let result_one: Vec<FileScanTask> = one_strategy
            .filter_iter(
                vec![create_test_file_scan_task(
                    "file1.parquet",
                    10 * 1024 * 1024,
                )]
                .into_iter(),
            )
            .collect();
        assert_eq!(result_one.len(), 1);
    }

    #[test]
    fn test_strategy_builder_with_min_files() {
        let strategy = FileStrategyFactory::builder()
            .filter_by_size(None, Some(32 * 1024 * 1024))
            .require_min_files(2)
            .limit_task_size(10 * 1024 * 1024 * 1024)
            .build();

        let description = StaticFileStrategy::description(&strategy);
        assert!(description.contains("SizeFilter"));
        assert!(description.contains("MinFileCount[2]"));
        assert!(description.contains("TaskSizeLimit"));

        // Test functionality with the composed strategy - enough files after size filter
        let data_files = vec![
            create_test_file_scan_task("small1.parquet", 5 * 1024 * 1024), // 5MB - should pass size filter
            create_test_file_scan_task("small2.parquet", 10 * 1024 * 1024), // 10MB - should pass size filter
            create_test_file_scan_task("large.parquet", 50 * 1024 * 1024), // 50MB - should be filtered out by size
        ];

        let result: Vec<FileScanTask> = strategy.filter_iter(data_files.into_iter()).collect();
        assert_eq!(result.len(), 2); // Two files pass size filter and meet minimum count

        // Test with insufficient files after size filtering
        let insufficient_files = vec![
            create_test_file_scan_task("small1.parquet", 5 * 1024 * 1024), // 5MB - should pass size filter
            create_test_file_scan_task("large1.parquet", 50 * 1024 * 1024), // 50MB - filtered out by size
            create_test_file_scan_task("large2.parquet", 60 * 1024 * 1024), // 60MB - filtered out by size
        ];

        let insufficient_result: Vec<FileScanTask> = strategy
            .filter_iter(insufficient_files.into_iter())
            .collect();
        assert_eq!(insufficient_result.len(), 0); // Only 1 file passes size filter, but need at least 2
    }

    #[test]
    fn test_small_files_strategy_end_to_end() {
        let config = CompactionPlanningConfigBuilder::default()
            .small_file_threshold(20 * 1024 * 1024) // 20MB threshold
            .max_task_total_size(50 * 1024 * 1024) // 50MB task limit
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

        let result: Vec<FileScanTask> = strategy.filter_iter(data_files.into_iter()).collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "small1.parquet");
        assert_eq!(result[1].data_file_path, "small3.parquet");

        // Verify all selected files are under the threshold and have no deletes
        for file in &result {
            assert!(file.length <= config.small_file_threshold);
            assert!(file.deletes.is_empty());
        }
    }
}
