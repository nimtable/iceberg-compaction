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

use futures::TryStreamExt;
use iceberg::table::Table;

use crate::Result;

#[derive(Debug, Clone, Default)]
pub struct SnapshotStats {
    pub total_data_files: usize,
    pub small_files_count: usize,
    pub files_with_deletes_count: usize,
}

pub struct SnapshotAnalyzer;

impl SnapshotAnalyzer {
    pub async fn analyze(
        table: &Table,
        snapshot_id: i64,
        small_file_threshold_bytes: u64,
    ) -> Result<SnapshotStats> {
        let scan = table.scan().snapshot_id(snapshot_id).build()?;
        let file_scan_stream = scan.plan_files().await?;
        let tasks: Vec<_> = file_scan_stream.try_collect().await?;

        let mut stats = SnapshotStats::default();

        for task in tasks {
            match task.data_file_content {
                iceberg::spec::DataContentType::Data => {
                    stats.total_data_files += 1;

                    if task.length < small_file_threshold_bytes {
                        stats.small_files_count += 1;
                    }

                    if !task.deletes.is_empty() {
                        stats.files_with_deletes_count += 1;
                    }
                }
                iceberg::spec::DataContentType::PositionDeletes
                | iceberg::spec::DataContentType::EqualityDeletes => {}
            }
        }

        Ok(stats)
    }
}
