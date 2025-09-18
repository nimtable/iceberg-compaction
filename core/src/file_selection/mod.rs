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

use crate::Result;
use futures::stream::TryStreamExt;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;

pub mod strategy;

// Re-export commonly used types for convenience
pub use strategy::{FileGroup, FileStrategyFactory, ThreeLayerStrategy};

/// File selection service responsible for selecting files for various operations
pub struct FileSelector;

impl FileSelector {
    /// Get scan tasks from table with specific snapshot ID and apply filtering strategy
    /// Returns grouped files maintaining the group structure from the strategy
    pub async fn get_grouped_scan_tasks_with_strategy(
        table: &Table,
        snapshot_id: i64,
        strategy: ThreeLayerStrategy,
    ) -> Result<Vec<FileGroup>> {
        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .with_delete_file_processing_enabled(true)
            .build()?;

        let file_scan_stream = scan.plan_files().await?;

        let data_files: Vec<FileScanTask> = file_scan_stream
            .try_filter_map(|task| {
                futures::future::ready(Ok(
                    if matches!(task.data_file_content, iceberg::spec::DataContentType::Data) {
                        Some(task)
                    } else {
                        None
                    },
                ))
            })
            .try_collect()
            .await?;

        // Apply the three-layer strategy to get groups
        let file_groups: Vec<FileGroup> = strategy.execute(data_files);

        Ok(file_groups)
    }

    /// Get scan tasks from table with specific snapshot ID and apply filtering strategy
    /// Returns a single flattened FileGroup (for backward compatibility)
    pub async fn get_scan_tasks_with_strategy(
        table: &Table,
        snapshot_id: i64,
        strategy: ThreeLayerStrategy,
    ) -> Result<FileGroup> {
        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .with_delete_file_processing_enabled(true)
            .build()?;

        let file_scan_stream = scan.plan_files().await?;

        let data_files: Vec<FileScanTask> = file_scan_stream
            .try_filter_map(|task| {
                futures::future::ready(Ok(
                    if matches!(task.data_file_content, iceberg::spec::DataContentType::Data) {
                        Some(task)
                    } else {
                        None
                    },
                ))
            })
            .try_collect()
            .await?;

        // Apply the three-layer strategy and flatten the result
        let file_groups = strategy.execute(data_files);
        let filtered_data_files: Vec<FileScanTask> = file_groups
            .into_iter()
            .flat_map(|group| group.into_files())
            .collect();

        // Create a FileGroup from the filtered data files
        // The FileGroup constructor will automatically extract delete files
        Ok(FileGroup::new(filtered_data_files))
    }
}
