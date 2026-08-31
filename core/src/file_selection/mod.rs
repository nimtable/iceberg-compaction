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

pub mod packer;
pub mod strategy;

pub use packer::ListPacker;
pub use strategy::{FileGroup, PlanStrategy, PlanStrategyOptions};

/// File selection service responsible for selecting files for various operations
pub struct FileSelector;

impl FileSelector {
    /// Validates file sequence metadata before bounded planning filters run.
    pub(crate) fn validate_file_sequence_numbers(data_files: &[FileScanTask]) -> Result<()> {
        if let Some(task) = data_files
            .iter()
            .find(|task| task.file_sequence_number.is_none())
        {
            return Err(crate::CompactionError::Config(format!(
                "bounded compaction requires file_sequence_number for every scanned data file; missing for {}",
                task.data_file_path
            )));
        }
        Ok(())
    }

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
    /// Returns the data-file tasks planned for downstream processing.
    ///
    /// Iceberg's current scan API keeps delete files as lightweight descriptors
    /// nested under each data task, so every top-level task is a data file.
    pub async fn scan_data_files(table: &Table, snapshot_id: i64) -> Result<Vec<FileScanTask>> {
        let scan = table.scan().snapshot_id(snapshot_id).build()?;

        let file_scan_stream = scan.plan_files().await?;

        let data_files: Vec<FileScanTask> = file_scan_stream.try_collect().await?;
        Ok(data_files)
    }

    /// Returns the minimum data sequence among files with applicable deletes.
    ///
    /// `tasks` must contain every live data-file task from the snapshot's complete,
    /// unfiltered `plan_files()` result. Each `task.deletes` must conservatively
    /// include every delete file that may apply; otherwise this threshold could
    /// retire a delete file that is still needed by an affected data file.
    ///
    /// Missing or invalid sequence metadata disables the optimization.
    pub(crate) fn delete_cleanup_min_data_sequence_number(tasks: &[FileScanTask]) -> Option<i64> {
        let mut min_sequence = None;
        for task in tasks.iter().filter(|task| !task.deletes.is_empty()) {
            let sequence = task
                .data_sequence_number
                .filter(|sequence| *sequence >= 0)?;
            min_sequence = Some(min_sequence.map_or(sequence, |min: i64| min.min(sequence)));
        }
        min_sequence
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
