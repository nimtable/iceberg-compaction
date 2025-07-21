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

use crate::common::CompactionMetricsRecorder;
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

    pub metrics_recorder: Option<CompactionMetricsRecorder>,
}

#[derive(Debug, Clone)]
/// `InputFileScanTasks` contains the file scan tasks for data files, position delete files, and equality delete files.
pub struct InputFileScanTasks {
    pub data_files: Vec<FileScanTask>,
    pub position_delete_files: Vec<FileScanTask>,
    pub equality_delete_files: Vec<FileScanTask>,
}

impl InputFileScanTasks {
    pub fn input_files_count(&self) -> usize {
        self.data_files.len() + self.position_delete_files.len() + self.equality_delete_files.len()
    }

    pub fn input_total_bytes(&self) -> u64 {
        self.data_files
            .iter()
            .chain(&self.position_delete_files)
            .chain(&self.equality_delete_files)
            .map(|task| task.file_size_in_bytes)
            .sum()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RewriteFilesResponse {
    pub data_files: Vec<DataFile>,
    pub stats: RewriteFilesStat,
    pub snapshot_id: i64,
}

#[derive(Debug, Clone, Default)]
pub struct RewriteFilesStat {
    pub input_files_count: usize,
    pub output_files_count: usize,
    pub input_total_bytes: u64,
    pub output_total_bytes: u64,

    pub input_data_file_count: usize,
    pub input_position_delete_file_count: usize,
    pub input_equality_delete_file_count: usize,
    pub input_data_file_total_bytes: u64,
    pub input_position_delete_file_total_bytes: u64,
    pub input_equality_delete_file_total_bytes: u64,
}

impl RewriteFilesStat {
    pub fn record_input(&mut self, input_file_scan_tasks: &InputFileScanTasks) {
        self.input_files_count = input_file_scan_tasks.input_files_count();
        self.input_total_bytes = input_file_scan_tasks.input_total_bytes();

        self.input_data_file_count = input_file_scan_tasks.data_files.len();
        self.input_position_delete_file_count = input_file_scan_tasks.position_delete_files.len();
        self.input_equality_delete_file_count = input_file_scan_tasks.equality_delete_files.len();
        self.input_data_file_total_bytes = input_file_scan_tasks
            .data_files
            .iter()
            .map(|task| task.file_size_in_bytes)
            .sum::<u64>();
        self.input_position_delete_file_total_bytes = input_file_scan_tasks
            .position_delete_files
            .iter()
            .map(|task| task.file_size_in_bytes)
            .sum::<u64>();
        self.input_equality_delete_file_total_bytes = input_file_scan_tasks
            .equality_delete_files
            .iter()
            .map(|task| task.file_size_in_bytes)
            .sum::<u64>();
    }

    pub fn record_output(&mut self, data_files: &[DataFile]) {
        self.output_files_count = data_files.len();
        self.output_total_bytes = data_files
            .iter()
            .map(|file| file.file_size_in_bytes())
            .sum::<u64>();
    }
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
