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
use iceberg::{
    io::FileIO, spec::PartitionSpec,
    writer::file_writer::location_generator::DefaultLocationGenerator,
};

use crate::common::CompactionMetricsRecorder;
use crate::config::CompactionExecutionConfig;
use crate::file_selection::FileGroup;
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
    pub file_group: FileGroup,
    pub execution_config: Arc<CompactionExecutionConfig>,
    pub partition_spec: Arc<PartitionSpec>,
    pub metrics_recorder: Option<CompactionMetricsRecorder>,
    pub location_generator: DefaultLocationGenerator,
}

#[derive(Debug, Clone, Default)]
pub struct RewriteFilesResponse {
    pub data_files: Vec<DataFile>,
    pub stats: RewriteFilesStat,
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
    pub fn record_input(&mut self, file_group: &FileGroup) {
        self.input_files_count = file_group.input_files_count();
        self.input_total_bytes = file_group.input_total_bytes();

        self.input_data_file_count = file_group.data_files.len();
        self.input_position_delete_file_count = file_group.position_delete_files.len();
        self.input_equality_delete_file_count = file_group.equality_delete_files.len();
        self.input_data_file_total_bytes = file_group
            .data_files
            .iter()
            .map(|task| task.file_size_in_bytes)
            .sum::<u64>();
        self.input_position_delete_file_total_bytes = file_group
            .position_delete_files
            .iter()
            .map(|task| task.file_size_in_bytes)
            .sum::<u64>();
        self.input_equality_delete_file_total_bytes = file_group
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
