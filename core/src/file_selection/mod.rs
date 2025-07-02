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

use crate::executor::InputFileScanTasks;
use crate::Result;
use futures_async_stream::for_await;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use std::collections::HashMap;

pub mod strategy;

/// File selection service responsible for selecting files for various operations
pub struct FileSelector;

impl FileSelector {
    /// Get scan tasks from table with specific snapshot ID and apply filtering strategy
    pub async fn get_scan_tasks_with_strategy(
        table: &Table,
        snapshot_id: i64,
        strategy: Box<dyn strategy::FileStrategy>,
    ) -> Result<InputFileScanTasks> {
        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .with_delete_file_processing_enabled(true)
            .build()?;

        let file_scan_stream = scan.plan_files().await?;

        let mut data_files = vec![];

        #[for_await]
        for task in file_scan_stream {
            let task: FileScanTask = task?;
            match task.data_file_content {
                iceberg::spec::DataContentType::Data => {
                    data_files.push(task);
                }
                _ => {
                    unreachable!()
                }
            }
        }

        // Apply file filtering strategy
        let filtered_data_files = strategy.filter(data_files)?;

        // Extract delete files from the filtered data files
        Self::build_input_file_scan_tasks(filtered_data_files)
    }

    /// Build InputFileScanTasks from filtered data files
    fn build_input_file_scan_tasks(
        filtered_data_files: Vec<FileScanTask>,
    ) -> Result<InputFileScanTasks> {
        let mut position_delete_files = HashMap::new();
        let mut equality_delete_files = HashMap::new();

        for task in &filtered_data_files {
            for delete_task in task.deletes.iter() {
                match &delete_task.data_file_content {
                    iceberg::spec::DataContentType::PositionDeletes => {
                        let mut delete_task = delete_task.clone();
                        delete_task.project_field_ids = vec![];
                        position_delete_files
                            .insert(delete_task.data_file_path.clone(), delete_task);
                    }
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        let mut delete_task = delete_task.clone();
                        delete_task.project_field_ids = delete_task.equality_ids.clone();
                        equality_delete_files
                            .insert(delete_task.data_file_path.clone(), delete_task);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            }
        }

        Ok(InputFileScanTasks {
            data_files: filtered_data_files,
            position_delete_files: position_delete_files.into_values().collect(),
            equality_delete_files: equality_delete_files.into_values().collect(),
        })
    }
}
