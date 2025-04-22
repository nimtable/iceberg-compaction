/*
 * Copyright 2025 IC
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
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;

use crate::config::CompactionConfig;
use crate::error::CompactionError;
use iceberg::spec::{DataFile, Schema};

pub mod mock;
pub use mock::MockExecutor;
pub mod datafusion;
pub use datafusion::DataFusionExecutor;
pub use ic_codegen::compactor::RewriteFilesStat;

#[async_trait]
pub trait CompactionExecutor: Send + Sync + 'static {
    async fn rewrite_files(
        file_io: FileIO,
        schema: Arc<Schema>,
        input_file_scan_tasks: InputFileScanTasks,
        config: Arc<CompactionConfig>,
        dir_path: String,
    ) -> Result<CompactionResult, CompactionError>;
}

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
pub struct CompactionResult {
    pub data_files: Vec<DataFile>,
    pub stat: RewriteFilesStat,
}
