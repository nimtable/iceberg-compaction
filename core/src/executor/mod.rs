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

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::scan::FileScanTask;
use iceberg::{io::FileIO, spec::PartitionSpec};

use crate::config::CompactionConfig;
use iceberg::spec::{DataFile, Schema};

pub mod mock;
pub use mock::MockExecutor;
pub mod datafusion;
pub mod iceberg_writer;
use crate::error::Result;
pub use datafusion::DataFusionExecutor;

#[async_trait]
pub trait CompactionExecutor: Send + Sync + 'static {
    async fn rewrite_files(&self, request: RewriteFilesRequest) -> Result<RewriteFilesResponse>;
}

pub struct RewriteFilesRequest {
    pub file_io: FileIO,
    pub schema: Arc<Schema>,
    pub input_file_scan_tasks: InputFileScanTasks,
    pub config: Arc<CompactionConfig>,
    pub dir_path: String,
    pub partition_spec: Arc<PartitionSpec>,
}

#[derive(Debug, Clone)]
/// InputFileScanTasks contains the file scan tasks for data files, position delete files, and equality delete files.
pub struct InputFileScanTasks {
    pub data_files: Vec<FileScanTask>,
    pub position_delete_files: Vec<FileScanTask>,
    pub equality_delete_files: Vec<FileScanTask>,
}

impl InputFileScanTasks {
    pub fn input_files_count(&self) -> u32 {
        self.data_files.len() as u32
            + self.position_delete_files.len() as u32
            + self.equality_delete_files.len() as u32
    }
}

#[derive(Debug, Clone, Default)]
pub struct RewriteFilesResponse {
    pub data_files: Vec<DataFile>,
    pub stat: RewriteFilesStat,
}

#[derive(Debug, Clone, Default)]
pub struct RewriteFilesStat {
    pub rewritten_files_count: u32,
    pub added_files_count: u32,
    pub rewritten_bytes: u64,
    pub failed_data_files_count: u32,
}

pub enum ExecutorType {
    DataFusion,
    Mock,
}

pub fn create_compaction_executor(executor_type: ExecutorType) -> Box<dyn CompactionExecutor> {
    match executor_type {
        ExecutorType::DataFusion => Box::new(DataFusionExecutor::default()),
        ExecutorType::Mock => Box::new(MockExecutor),
    }
}
