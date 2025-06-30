/*
 * Copyright 2025 iceberg-compact
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

use datafusion::arrow::array::RecordBatch;
use futures::future;
use iceberg::{
    spec::DataFile,
    writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder},
};
use iceberg::{ErrorKind, Result};
use tokio::task::JoinHandle;

use crate::config::{DEFAULT_MAX_CONCURRENT_CLOSES, DEFAULT_TARGET_FILE_SIZE};

/// RollingIcebergWriter wraps an IcebergWriter and splits output files by target size.
pub struct RollingIcebergWriter<B, D> {
    /// Builder for creating new inner writers.
    inner_writer_builder: B,
    /// The current active writer.
    inner_writer: Option<D>,
    /// Target file size in bytes. When exceeded, a new file is started.
    target_file_size: u64,
    /// Collected data files that have been closed.
    data_files: Vec<DataFile>,
    /// Current written size of the active file.
    current_written_size: u64,
    /// Futures for all closing writers.
    close_futures: Vec<JoinHandle<Result<Vec<DataFile>>>>,
    /// Maximum number of concurrent close operations allowed.
    max_concurrent_closes: usize,
}

#[async_trait::async_trait]
impl<B, D> IcebergWriter for RollingIcebergWriter<B, D>
where
    B: IcebergWriterBuilder<R = D>,
    D: IcebergWriter + CurrentFileStatus,
{
    /// Write a RecordBatch. If the current file size plus the new batch size
    /// exceeds the target, close the current file and start a new one.
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        let input_size = input.get_array_memory_size();

        // If adding this batch would exceed the target file size, close current file and start a new one.
        if need_build_new_file(
            self.current_written_size,
            input_size as u64,
            self.target_file_size,
        ) {
            // Take the current writer and spawn its close operation
            if let Some(mut inner_writer) = self.inner_writer.take() {
                // If we've reached the max concurrent closes, wait for one to complete
                if self.close_futures.len() >= self.max_concurrent_closes {
                    self.wait_for_one_close().await?;
                }

                let close_handle = tokio::spawn(async move { inner_writer.close().await });
                self.close_futures.push(close_handle);
            }

            // Create a new writer
            self.current_written_size = 0;
        }

        // Write the batch to the current writer.
        if self.inner_writer.is_none() {
            self.inner_writer = Some(self.inner_writer_builder.clone().build().await?);
        }
        self.inner_writer.as_mut().unwrap().write(input).await?;
        self.current_written_size += input_size as u64;
        Ok(())
    }

    /// Close the writer, ensuring all data files are finalized and returned.
    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let mut data_files = std::mem::take(&mut self.data_files);

        // Wait for all pending close operations to complete
        let close_futures = std::mem::take(&mut self.close_futures);
        for close_handle in close_futures {
            match close_handle.await {
                Ok(Ok(files)) => data_files.extend(files),
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(iceberg::Error::new(
                        ErrorKind::Unexpected,
                        format!("Failed to join close task: {}", e),
                    ))
                }
            }
        }

        // Close the current writer
        if let Some(mut writer) = self.inner_writer.take() {
            data_files.extend(writer.close().await?);
        }
        Ok(data_files)
    }
}

impl<B, D> RollingIcebergWriter<B, D> {
    /// Wait for one close operation to complete and collect its result.
    async fn wait_for_one_close(&mut self) -> Result<()> {
        if self.close_futures.is_empty() {
            return Ok(());
        }

        // Use select_all to wait for the first future to complete
        let (result, _index, remaining) =
            future::select_all(std::mem::take(&mut self.close_futures)).await;

        // Put back the remaining futures
        self.close_futures = remaining;

        // Handle the completed result
        match result {
            Ok(Ok(files)) => {
                self.data_files.extend(files);
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(e) => Err(iceberg::Error::new(
                ErrorKind::Unexpected,
                format!("Failed to join close task: {}", e),
            )),
        }
    }
}

pub fn need_build_new_file(
    current_written_size: u64,
    input_size: u64,
    target_file_size: u64,
) -> bool {
    // If the current file size is less than 10% of the target size, don't build a new file.
    if current_written_size < target_file_size / 10 {
        return false;
    }
    // If the total size of the current file and the new batch would exceed 1.5x the target size, build a new file.
    if current_written_size + input_size > target_file_size * 3 / 2 {
        return true;
    }
    // If the total size of the current file and the new batch would exceed the target size, build a new file.
    if current_written_size + input_size > target_file_size
        && current_written_size > target_file_size * 7 / 10
    {
        return true;
    }
    false
}

#[derive(Clone)]
/// Builder for RollingIcebergWriter.
pub struct RollingIcebergWriterBuilder<B> {
    inner_builder: B,
    target_file_size: Option<u64>,
    max_concurrent_closes: Option<usize>,
}

impl<B> RollingIcebergWriterBuilder<B> {
    /// Create a new RollingIcebergWriterBuilder.
    pub fn new(inner_builder: B) -> Self {
        Self {
            inner_builder,
            target_file_size: None,
            max_concurrent_closes: None,
        }
    }

    /// Set the target file size in bytes.
    pub fn with_target_file_size(mut self, target_file_size: u64) -> Self {
        self.target_file_size = Some(target_file_size);
        self
    }

    /// Set the maximum number of concurrent close operations.
    pub fn with_max_concurrent_closes(mut self, max_concurrent_closes: usize) -> Self {
        self.max_concurrent_closes = Some(max_concurrent_closes);
        self
    }
}

#[async_trait::async_trait]
impl<B> IcebergWriterBuilder for RollingIcebergWriterBuilder<B>
where
    B: IcebergWriterBuilder,
    B::R: IcebergWriter + CurrentFileStatus,
{
    type R = RollingIcebergWriter<B, B::R>;

    /// Build a new RollingIcebergWriter.
    async fn build(self) -> Result<Self::R> {
        Ok(RollingIcebergWriter {
            inner_writer_builder: self.inner_builder.clone(),
            inner_writer: Some(self.inner_builder.build().await?),
            target_file_size: self.target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE), // Default to 1 GB
            data_files: Vec::new(),
            current_written_size: 0,
            close_futures: Vec::new(),
            max_concurrent_closes: self
                .max_concurrent_closes
                .unwrap_or(DEFAULT_MAX_CONCURRENT_CLOSES), // Default to 4 concurrent closes
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_need_build_new_file_total_size_exceeds_threshold() {
        let target_size = 1000;

        // Test when total size exceeds 1.5x target size
        assert!(need_build_new_file(800, 800, target_size)); // 1600 > 1500
        assert!(need_build_new_file(1000, 600, target_size)); // 1600 > 1500
    }

    #[test]
    fn test_need_build_new_file_normal_cases() {
        let target_size = 1000;

        // Case 1: Current file size > 70% and total size would exceed target
        assert!(need_build_new_file(800, 300, target_size));

        // Case 2: Current file size > 70% but total size would not exceed target
        assert!(!need_build_new_file(800, 100, target_size));

        // Case 3: Current file size < 70% even though total size would exceed target
        assert!(!need_build_new_file(600, 500, target_size));
    }

    #[test]
    fn test_need_build_new_file_edge_cases() {
        let target_size = 1000;

        // Empty file case
        assert!(!need_build_new_file(0, 2000, target_size));

        // Exactly at 70% threshold
        assert!(!need_build_new_file(700, 400, target_size));

        // Just over 70% threshold
        assert!(need_build_new_file(701, 400, target_size));

        // Exactly at 1.5x threshold
        assert!(!need_build_new_file(0, 1500, target_size));

        // Just over 1.5x threshold
        assert!(!need_build_new_file(1, 1501, target_size));
    }
}
