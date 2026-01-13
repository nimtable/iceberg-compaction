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

use futures::stream::TryStreamExt;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;

use crate::Result;

pub mod analyzer;
pub mod packer;
pub mod strategy;

// Re-export commonly used types for convenience
pub use analyzer::{SnapshotAnalyzer, SnapshotStats};
pub use packer::ListPacker;
pub use strategy::{FileGroup, PlanStrategy};

/// File selection service responsible for selecting files for various operations
pub struct FileSelector;

impl FileSelector {
    /// Get scan tasks from table with specific snapshot ID and apply filtering strategy
    /// Returns groups of files selected and organized by the given strategy
    pub async fn get_scan_tasks_with_strategy(
        table: &Table,
        snapshot_id: i64,
        strategy: PlanStrategy,
        config: &crate::config::CompactionPlanningConfig,
    ) -> Result<Vec<FileGroup>> {
        let data_files = Self::scan_data_files(table, snapshot_id).await?;
        strategy.execute(data_files, config)
    }

    /// Scans and collects all data files from a table snapshot.
    ///
    /// Filters out non-data files (delete files). Returns raw `FileScanTask`s
    /// for downstream processing.
    pub async fn scan_data_files(table: &Table, snapshot_id: i64) -> Result<Vec<FileScanTask>> {
        let scan = table.scan().snapshot_id(snapshot_id).build()?;

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

        Ok(data_files)
    }

    /// Groups pre-scanned tasks using the given strategy, skipping the scan phase.
    ///
    /// Use this when tasks have already been collected (e.g., for stats calculation)
    /// to avoid redundant `plan_files()` calls.
    pub fn group_tasks_with_strategy(
        tasks: Vec<FileScanTask>,
        strategy: PlanStrategy,
        config: &crate::config::CompactionPlanningConfig,
    ) -> Result<Vec<FileGroup>> {
        strategy.execute(tasks, config)
    }
}
