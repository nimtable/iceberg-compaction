// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains file writer API, and provides methods to write row groups and columns by
//! using row group writers and column writers respectively.

use std::collections::HashSet;
use std::sync::Arc;

use futures::StreamExt;
use iceberg::spec::{DataFile, Schema};
use iceberg::table::Table;

use crate::error::Result;
use crate::executor::InputFileScanTasks;
use crate::executor::datafusion::datafusion_processor::{
    DataFusionTaskContext, DatafusionProcessor,
};
use crate::{CompactionConfig, CompactionError};
use iceberg::io::FileIO;

pub struct CompactionTester {
    datafusion_processor: DatafusionProcessor,
    input_datafusion_task_ctx: Option<DataFusionTaskContext>,
    output_datafusion_task_ctx: Option<DataFusionTaskContext>,
}

impl CompactionTester {
    pub async fn new(
        input_file_scan_tasks: InputFileScanTasks,
        output_files: Vec<DataFile>,
        schema: Arc<Schema>,
        config: Arc<CompactionConfig>,
        file_io: FileIO,
        table: Table,
    ) -> Result<Self> {
        let snapshot_id = table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(|| CompactionError::Config("Snapshot id is not set".to_owned()))?;
        let scan = table.scan().from_snapshot_id(snapshot_id).build()?;
        let output_file_paths = output_files
            .iter()
            .map(|f| f.file_path())
            .collect::<HashSet<_>>();
        let mut output_file_scan_tasks_full_table = scan.plan_files().await?;
        let mut output_file_scan_tasks = vec![];
        while let Some(file) = output_file_scan_tasks_full_table.as_mut().next().await {
            let file = file?;
            if output_file_paths.contains(file.data_file_path()) {
                output_file_scan_tasks.push(file);
            }
        }

        let input_datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema.clone())
            .with_input_data_files(input_file_scan_tasks)
            .with_need_order(true)
            .build_merge_on_read()?;
        let output_datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema)
            .with_data_files(output_file_scan_tasks)
            .with_need_order(true)
            .build_merge_on_read()?;
        let new_config = Arc::new(
            CompactionConfig::builder()
                .batch_parallelism(config.batch_parallelism / 2)
                .target_partitions(1)
                .data_file_prefix(config.data_file_prefix.clone())
                .validate_compaction(true)
                .target_file_size(config.target_file_size)
                .build(),
        );
        let datafusion_processor = DatafusionProcessor::new(new_config, file_io);
        Ok(Self {
            datafusion_processor,
            input_datafusion_task_ctx: Some(input_datafusion_task_ctx),
            output_datafusion_task_ctx: Some(output_datafusion_task_ctx),
        })
    }

    pub async fn validate(&mut self) -> Result<()> {
        let input_datafusion_task_ctx = self.input_datafusion_task_ctx.take().ok_or_else(|| {
            CompactionError::Config("Input datafusion task context is not set".to_owned())
        })?;
        let output_datafusion_task_ctx =
            self.output_datafusion_task_ctx.take().ok_or_else(|| {
                CompactionError::Config("Output datafusion task context is not set".to_owned())
            })?;
        let (mut input_batchs, _) = self
            .datafusion_processor
            .execute(input_datafusion_task_ctx)
            .await?;
        let (mut output_batchs, _) = self
            .datafusion_processor
            .execute(output_datafusion_task_ctx)
            .await?;
        if input_batchs.len() != output_batchs.len() || input_batchs.len() != 1 {
            return Err(CompactionError::CompactionTest(format!(
                "Input and output batchs length mismatch: {} != {} != 1",
                input_batchs.len(),
                output_batchs.len()
            )));
        }

        let mut input_batch = input_batchs.pop().unwrap();
        let mut output_batch = output_batchs.pop().unwrap();

        let mut input_stream = input_batch.as_mut();
        let mut output_stream = output_batch.as_mut();
        loop {
            match (input_stream.next().await, output_stream.next().await) {
                (Some(input_batch), Some(output_batch)) => {
                    let input_batch = input_batch?;
                    let output_batch = output_batch?;
                    if input_batch != output_batch {
                        return Err(CompactionError::CompactionTest(format!(
                            "Input and output batchs mismatch: {:?} != {:?}",
                            input_batch, output_batch
                        )));
                    }
                }
                (None, None) => {
                    break;
                }
                _ => {
                    return Err(CompactionError::CompactionTest(
                        "Input and output batchs length mismatch".to_owned(),
                    ));
                }
            }
        }

        Ok(())
    }
}
