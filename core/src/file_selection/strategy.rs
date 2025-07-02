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

use crate::CompactionConfig;
use crate::Result;
use iceberg::scan::FileScanTask;

use crate::compaction::CompactionType;

/// Strategy trait for filtering files during compaction
pub trait FileStrategy: Send + Sync {
    /// Filter the input data files
    /// Delete files are already contained in each FileScanTask.deletes field
    fn filter(&self, data_files: Vec<FileScanTask>) -> Result<Vec<FileScanTask>>;

    /// Get a description of this strategy for logging/debugging
    fn description(&self) -> String;
}

/// A composable strategy that applies multiple strategies in sequence
#[derive(Default)]
pub struct CompositeStrategy {
    strategies: Vec<Box<dyn FileStrategy>>,
}

impl CompositeStrategy {
    pub fn add_strategy(mut self, strategy: Box<dyn FileStrategy>) -> Self {
        self.strategies.push(strategy);
        self
    }
}

impl FileStrategy for CompositeStrategy {
    fn filter(&self, mut data_files: Vec<FileScanTask>) -> Result<Vec<FileScanTask>> {
        for strategy in &self.strategies {
            data_files = strategy.filter(data_files)?;
        }
        Ok(data_files)
    }

    fn description(&self) -> String {
        let descriptions: Vec<String> = self.strategies.iter().map(|s| s.description()).collect();
        format!("Composite[{}]", descriptions.join(" -> "))
    }
}

/// No-op strategy that passes through all files unchanged
pub struct NoopStrategy;

impl FileStrategy for NoopStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Result<Vec<FileScanTask>> {
        Ok(data_files)
    }

    fn description(&self) -> String {
        "Noop".to_string()
    }
}

/// Strategy for filtering files by size threshold
pub struct SizeFilterStrategy {
    pub min_size: Option<u64>,
    pub max_size: Option<u64>,
}

impl FileStrategy for SizeFilterStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Result<Vec<FileScanTask>> {
        let filtered_data_files: Vec<FileScanTask> = data_files
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
            .collect();

        Ok(filtered_data_files)
    }

    fn description(&self) -> String {
        match (self.min_size, self.max_size) {
            (Some(min), Some(max)) => {
                format!("SizeFilter[{}-{}MB]", min / 1024 / 1024, max / 1024 / 1024)
            }
            (Some(min), None) => format!("SizeFilter[>{}MB]", min / 1024 / 1024),
            (None, Some(max)) => format!("SizeFilter[<{}MB]", max / 1024 / 1024),
            (None, None) => "SizeFilter[Any]".to_string(),
        }
    }
}

/// Strategy for limiting total task size
pub struct TaskSizeLimitStrategy {
    pub max_total_size: u64,
}

impl FileStrategy for TaskSizeLimitStrategy {
    fn filter(&self, data_files: Vec<FileScanTask>) -> Result<Vec<FileScanTask>> {
        let mut total_size = 0u64;
        let filtered_data_files: Vec<FileScanTask> = data_files
            .into_iter()
            .take_while(|task| {
                let file_size = task.length;
                if total_size + file_size <= self.max_total_size {
                    total_size += file_size;
                    true
                } else {
                    false
                }
            })
            .collect();

        Ok(filtered_data_files)
    }

    fn description(&self) -> String {
        format!(
            "TaskSizeLimit[{}GB]",
            self.max_total_size / 1024 / 1024 / 1024
        )
    }
}

/// Factory for creating file strategies based on compaction type and configuration
pub struct FileStrategyFactory;

impl FileStrategyFactory {
    /// Create a strategy based on compaction type and config
    ///
    /// This is the **recommended** method for most users. It creates predefined,
    /// well-tested strategies for standard compaction scenarios.
    ///
    /// Use this method unless you need custom strategy combinations.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use iceberg_compaction_core::compaction::{FileStrategyFactory, CompactionType};
    /// # use iceberg_compaction_core::config::CompactionConfig;
    /// # let config = CompactionConfig::default();
    /// let strategy = FileStrategyFactory::create_strategy(&CompactionType::SmallFiles, &config);
    /// ```
    pub fn create_strategy(
        compaction_type: &CompactionType,
        config: &CompactionConfig,
    ) -> Box<dyn FileStrategy> {
        match compaction_type {
            CompactionType::Full => Box::new(NoopStrategy),
            CompactionType::SmallFiles => Self::create_small_files_strategy(config),
        }
    }

    /// Create strategy for small files compaction
    fn create_small_files_strategy(config: &CompactionConfig) -> Box<dyn FileStrategy> {
        // Use the builder pattern for consistency and readability
        Self::builder()
            .filter_by_size(None, Some(config.small_file_threshold))
            .limit_task_size(config.max_task_total_size)
            .build()
    }

    /// Create a custom strategy builder for advanced use cases
    ///
    /// This method is for **advanced users** who need custom strategy combinations
    /// that aren't covered by the predefined compaction types.
    ///
    /// For most use cases, prefer `create_strategy()` instead.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use iceberg_compaction_core::compaction::FileStrategyFactory;
    /// // Custom strategy for specific requirements
    /// let strategy = FileStrategyFactory::builder()
    ///     .filter_by_size(Some(1024*1024), Some(100*1024*1024))  // 1MB-100MB files only
    ///     .limit_task_size(20 * 1024*1024*1024)  // 20GB task limit
    ///     .build();
    /// ```
    pub fn builder() -> StrategyBuilder {
        StrategyBuilder::default()
    }
}

/// Builder for creating custom file strategies
#[derive(Default)]
pub struct StrategyBuilder {
    composite: CompositeStrategy,
}

impl StrategyBuilder {
    pub fn filter_by_size(self, min_size: Option<u64>, max_size: Option<u64>) -> Self {
        Self {
            composite: self
                .composite
                .add_strategy(Box::new(SizeFilterStrategy { min_size, max_size })),
        }
    }

    pub fn limit_task_size(self, max_total_size: u64) -> Self {
        Self {
            composite: self
                .composite
                .add_strategy(Box::new(TaskSizeLimitStrategy { max_total_size })),
        }
    }

    pub fn add_custom_strategy(self, strategy: Box<dyn FileStrategy>) -> Self {
        Self {
            composite: self.composite.add_strategy(strategy),
        }
    }

    pub fn build(self) -> Box<dyn FileStrategy> {
        Box::new(self.composite)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompactionConfigBuilder;

    // Helper function to create test FileScanTask
    fn create_test_file_scan_task(file_path: &str, file_size: u64) -> FileScanTask {
        use iceberg::spec::{DataContentType, DataFileFormat};
        use std::sync::Arc;

        FileScanTask {
            start: 0,
            length: file_size,
            record_count: Some(100),
            data_file_path: file_path.to_string(),
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

    #[test]
    fn test_noop_strategy() {
        let strategy = NoopStrategy;
        let data_files = vec![];

        let result_data = strategy.filter(data_files.clone()).unwrap();

        assert_eq!(result_data.len(), data_files.len());
        assert_eq!(strategy.description(), "Noop");
    }

    #[test]
    fn test_size_filter_strategy_description() {
        let strategy = SizeFilterStrategy {
            min_size: Some(1024 * 1024),
            max_size: Some(32 * 1024 * 1024),
        };
        assert_eq!(strategy.description(), "SizeFilter[1-32MB]");
    }

    #[test]
    fn test_composite_strategy_description() {
        let composite = CompositeStrategy::default()
            .add_strategy(Box::new(SizeFilterStrategy {
                min_size: None,
                max_size: Some(32 * 1024 * 1024),
            }))
            .add_strategy(Box::new(TaskSizeLimitStrategy {
                max_total_size: 10 * 1024 * 1024 * 1024,
            }));

        assert_eq!(
            composite.description(),
            "Composite[SizeFilter[<32MB] -> TaskSizeLimit[10GB]]"
        );
    }

    #[test]
    fn test_strategy_builder() {
        let strategy = FileStrategyFactory::builder()
            .filter_by_size(None, Some(32 * 1024 * 1024))
            .limit_task_size(10 * 1024 * 1024 * 1024)
            .build();

        assert!(strategy.description().contains("SizeFilter"));
        assert!(strategy.description().contains("TaskSizeLimit"));
    }

    #[test]
    fn test_strategy_builder_with_custom_strategy() {
        let custom_strategy = Box::new(SizeFilterStrategy {
            min_size: Some(1024 * 1024), // 1MB minimum
            max_size: None,
        });

        let strategy = FileStrategyFactory::builder()
            .filter_by_size(None, Some(32 * 1024 * 1024))
            .add_custom_strategy(custom_strategy)
            .limit_task_size(10 * 1024 * 1024 * 1024)
            .build();

        let description = strategy.description();
        assert!(description.contains("SizeFilter"));
        assert!(description.contains("TaskSizeLimit"));
        // Should contain multiple SizeFilter strategies
        assert_eq!(description.matches("SizeFilter").count(), 2);
    }

    #[test]
    fn test_factory_create_strategy() {
        let config = CompactionConfigBuilder::default().build().unwrap();

        let full_strategy = FileStrategyFactory::create_strategy(&CompactionType::Full, &config);
        assert_eq!(full_strategy.description(), "Noop");

        let small_files_strategy =
            FileStrategyFactory::create_strategy(&CompactionType::SmallFiles, &config);
        assert!(small_files_strategy.description().contains("Composite"));
    }

    #[test]
    fn test_size_filter_strategy_logic() {
        let strategy = SizeFilterStrategy {
            min_size: Some(5 * 1024 * 1024),  // 5MB min
            max_size: Some(50 * 1024 * 1024), // 50MB max
        };

        let data_files = vec![
            create_test_file_scan_task("small_file.parquet", 2 * 1024 * 1024), // 2MB - should be filtered out
            create_test_file_scan_task("medium_file1.parquet", 10 * 1024 * 1024), // 10MB - should pass
            create_test_file_scan_task("medium_file2.parquet", 30 * 1024 * 1024), // 30MB - should pass
            create_test_file_scan_task("large_file.parquet", 100 * 1024 * 1024), // 100MB - should be filtered out
        ];

        let result = strategy.filter(data_files).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "medium_file1.parquet");
        assert_eq!(result[1].data_file_path, "medium_file2.parquet");
    }

    #[test]
    fn test_size_filter_strategy_min_only() {
        let strategy = SizeFilterStrategy {
            min_size: Some(10 * 1024 * 1024), // 10MB min, no max
            max_size: None,
        };

        let data_files = vec![
            create_test_file_scan_task("small_file.parquet", 5 * 1024 * 1024), // 5MB - should be filtered out
            create_test_file_scan_task("medium_file.parquet", 15 * 1024 * 1024), // 15MB - should pass
            create_test_file_scan_task("large_file.parquet", 100 * 1024 * 1024), // 100MB - should pass
        ];

        let result = strategy.filter(data_files).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "medium_file.parquet");
        assert_eq!(result[1].data_file_path, "large_file.parquet");
    }

    #[test]
    fn test_size_filter_strategy_max_only() {
        let strategy = SizeFilterStrategy {
            min_size: None,
            max_size: Some(20 * 1024 * 1024), // 20MB max, no min
        };

        let data_files = vec![
            create_test_file_scan_task("small_file1.parquet", 5 * 1024 * 1024), // 5MB - should pass
            create_test_file_scan_task("small_file2.parquet", 15 * 1024 * 1024), // 15MB - should pass
            create_test_file_scan_task("large_file.parquet", 50 * 1024 * 1024), // 50MB - should be filtered out
        ];

        let result = strategy.filter(data_files).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "small_file1.parquet");
        assert_eq!(result[1].data_file_path, "small_file2.parquet");
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

        let result = strategy.filter(data_files).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "file1.parquet");
        assert_eq!(result[1].data_file_path, "file2.parquet");
    }

    #[test]
    fn test_composite_strategy_filter_logic() {
        let composite = CompositeStrategy::default()
            .add_strategy(Box::new(SizeFilterStrategy {
                min_size: None,
                max_size: Some(20 * 1024 * 1024), // Only files <= 20MB
            }))
            .add_strategy(Box::new(TaskSizeLimitStrategy {
                max_total_size: 25 * 1024 * 1024, // Total limit 25MB
            }));

        let data_files = vec![
            create_test_file_scan_task("small1.parquet", 5 * 1024 * 1024), // 5MB - should pass (total: 5MB)
            create_test_file_scan_task("small2.parquet", 10 * 1024 * 1024), // 10MB - should pass (total: 15MB)
            create_test_file_scan_task("small3.parquet", 15 * 1024 * 1024), // 15MB - should be filtered out by size limit (would exceed 25MB)
            create_test_file_scan_task("large.parquet", 30 * 1024 * 1024), // 30MB - should be filtered out by size filter
        ];

        let result = composite.filter(data_files).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_file_path, "small1.parquet");
        assert_eq!(result[1].data_file_path, "small2.parquet");
    }

    #[test]
    fn test_small_files_strategy_end_to_end() {
        let config = CompactionConfigBuilder::default()
            .small_file_threshold(20 * 1024 * 1024) // 20MB threshold
            .max_task_total_size(50 * 1024 * 1024) // 50MB task limit
            .build()
            .unwrap();

        let strategy = FileStrategyFactory::create_strategy(&CompactionType::SmallFiles, &config);

        let data_files = vec![
            create_test_file_scan_task("small1.parquet", 5 * 1024 * 1024), // 5MB - should pass
            create_test_file_scan_task("small2.parquet", 15 * 1024 * 1024), // 15MB - should pass
            create_test_file_scan_task("small3.parquet", 10 * 1024 * 1024), // 10MB - should pass
            create_test_file_scan_task("small4.parquet", 25 * 1024 * 1024), // 25MB - should be filtered out (exceeds threshold)
            create_test_file_scan_task("large.parquet", 100 * 1024 * 1024), // 100MB - should be filtered out
        ];

        let result = strategy.filter(data_files).unwrap();

        // Only the first 3 small files should pass (5+15+10=30MB, next 10MB would exceed 50MB limit)
        // Actually, let's recalculate: 5+15+10 = 30MB, all should pass since under 50MB limit
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].data_file_path, "small1.parquet");
        assert_eq!(result[1].data_file_path, "small2.parquet");
        assert_eq!(result[2].data_file_path, "small3.parquet");

        // Verify all selected files are under the threshold
        for file in &result {
            assert!(file.length <= config.small_file_threshold);
        }
    }
}
