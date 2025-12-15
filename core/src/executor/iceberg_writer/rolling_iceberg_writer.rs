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

use datafusion::arrow::array::RecordBatch;
use futures::future;
use iceberg::spec::PartitionKey;
use iceberg::{ErrorKind, Result};
use iceberg::{
    spec::DataFile,
    writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder},
};
use tokio::task::JoinHandle;

use crate::config::{
    DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION, DEFAULT_MAX_CONCURRENT_CLOSES,
    DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR, DEFAULT_TARGET_FILE_SIZE,
};

/// Size estimation tracker for dynamic file size prediction
#[derive(Debug, Clone)]
pub struct SizeEstimationTracker {
    /// Whether dynamic size estimation is enabled
    enabled: bool,
    /// Current size estimation ratio (`physical_size / memory_size`), None if not learned yet
    ratio: Option<f64>,
    /// Smoothing factor for ratio updates
    smoothing_factor: f64,
    /// Last known physical written size
    last_physical_size: u64,
    /// Current in-memory size that hasn't been flushed
    current_memory_size: u64,
}

impl SizeEstimationTracker {
    pub fn new(enabled: bool, smoothing_factor: f64) -> Self {
        Self {
            enabled,
            ratio: None,
            smoothing_factor,
            last_physical_size: 0,
            current_memory_size: 0,
        }
    }

    /// Update the tracker with new physical size and memory size information
    pub fn update_physical_size(&mut self, new_physical_size: u64) {
        if new_physical_size == self.last_physical_size {
            return;
        }

        let physical_growth = new_physical_size - self.last_physical_size;

        if self.enabled && self.current_memory_size > 0 {
            let calculated_ratio = physical_growth as f64 / self.current_memory_size as f64;
            self.update_ratio(calculated_ratio);
        }

        self.last_physical_size = new_physical_size;
        self.current_memory_size = 0;
    }

    /// Add memory size to the tracker
    pub fn add_memory_size(&mut self, size: u64) {
        self.current_memory_size += size;
    }

    /// Estimate physical size from memory size using learned compression ratio
    pub fn estimate_physical_size(&self, memory_size: u64) -> u64 {
        if self.enabled {
            if let Some(ratio) = self.ratio {
                (memory_size as f64 * ratio) as u64
            } else {
                memory_size
            }
        } else {
            memory_size
        }
    }

    /// Calculate total estimated size including physical size
    pub fn calculate_total_estimated_size(&self) -> u64 {
        let estimated_memory_size = self.estimate_physical_size(self.current_memory_size);
        self.last_physical_size + estimated_memory_size
    }

    /// Reset tracking for a new file (but keep the learned ratio)
    pub fn reset_for_new_file(&mut self) {
        self.last_physical_size = 0;
        self.current_memory_size = 0;
    }

    /// Get current estimation ratio
    pub fn get_ratio(&self) -> f64 {
        self.ratio.unwrap_or(1.0)
    }

    /// Get current memory size
    pub fn get_current_memory_size(&self) -> u64 {
        self.current_memory_size
    }

    /// Get last physical size
    pub fn get_last_physical_size(&self) -> u64 {
        self.last_physical_size
    }

    /// Check if estimation is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Update ratio based on complete file information
    pub fn update_ratio_from_complete_file(&mut self, final_physical_size: u64, memory_size: u64) {
        if !self.enabled || memory_size == 0 || final_physical_size == 0 {
            return;
        }

        let calculated_ratio = final_physical_size as f64 / memory_size as f64;
        self.update_ratio(calculated_ratio);

        tracing::debug!(
            "Updated ratio from complete file: final_physical={}, memory_size={}, calculated_ratio={:.3}, smoothed_ratio={:.3}",
            final_physical_size,
            memory_size,
            calculated_ratio,
            self.get_ratio()
        );
    }

    /// Update the ratio using exponential moving average
    fn update_ratio(&mut self, calculated_ratio: f64) {
        match self.ratio {
            Some(current_ratio) => {
                self.ratio = Some(
                    (1.0 - self.smoothing_factor) * current_ratio
                        + self.smoothing_factor * calculated_ratio,
                );
            }
            None => {
                self.ratio = Some(calculated_ratio);
            }
        }
    }
}

type CloseFuture = Vec<JoinHandle<Result<(Vec<DataFile>, Option<(u64, u64)>)>>>;

/// `RollingIcebergWriter` wraps an `IcebergWriter` and splits output files by target size.
///
/// # Features
///
/// ## Dynamic Size Estimation (Optional)
///
/// This writer can optionally use dynamic size estimation to improve file size prediction.
/// This feature learns the actual compression ratio from write operations and uses it to better estimate
/// when to roll over to a new file. When disabled, it uses conservative 1:1 memory-to-disk size assumption.
///
/// # Usage Examples
///
/// ## Basic Usage (Default behavior)
/// ```rust,ignore
/// use iceberg_compaction_core::executor::iceberg_writer::rolling_iceberg_writer::RollingIcebergWriterBuilder;
///
/// // Create a basic rolling writer (dynamic size estimation disabled by default)
/// let writer = RollingIcebergWriterBuilder::new(inner_builder)
///     .with_target_file_size(1024 * 1024 * 1024) // 1GB
///     .build(None)?;
/// ```
///
/// ## With Dynamic Size Estimation
/// ```rust,ignore
/// use iceberg_compaction_core::executor::iceberg_writer::rolling_iceberg_writer::RollingIcebergWriterBuilder;
///
/// // Create a rolling writer with dynamic size estimation enabled
/// let writer = RollingIcebergWriterBuilder::new(inner_builder)
///     .with_target_file_size(1024 * 1024 * 1024) // 1GB
///     .with_dynamic_size_estimation(true)       // Enable dynamic size estimation
///     .with_size_estimation_smoothing_factor(0.3) // Adjust smoothing (optional)
///     .build(None)?;
/// ```
///
/// ## Custom Configuration
/// ```rust,ignore
/// use iceberg_compaction_core::executor::iceberg_writer::rolling_iceberg_writer::RollingIcebergWriterBuilder;
///
/// // Fully customized rolling writer
/// let writer = RollingIcebergWriterBuilder::new(inner_builder)
///     .with_target_file_size(512 * 1024 * 1024)  // 512MB
///     .with_max_concurrent_closes(8)              // Allow 8 concurrent closes
///     .with_dynamic_size_estimation(true)         // Enable dynamic size estimation
///     .with_size_estimation_smoothing_factor(0.2) // More stable estimation
///     .build(None)?;
/// ```
pub struct RollingIcebergWriter<B, D> {
    /// Builder for creating new inner writers.
    inner_writer_builder: B,
    /// The current active writer.
    inner_writer: Option<D>,
    /// Target file size in bytes. When exceeded, a new file is started.
    target_file_size: u64,
    /// Collected data files that have been closed.
    data_files: Vec<DataFile>,
    /// Size estimation tracker for dynamic file size prediction.
    size_tracker: SizeEstimationTracker,
    /// Futures for all closing writers with their file size information (files, `small_file_info`).
    /// `small_file_info` is Some((`final_physical_size`, `unflushed_memory_size`)) only for small files that need ratio calculation.
    close_futures: CloseFuture,
    /// Maximum number of concurrent close operations allowed.
    max_concurrent_closes: usize,
    /// Optional partition key for the writer.
    partition_key: Option<PartitionKey>,
}

#[async_trait::async_trait]
impl<B, D> IcebergWriter for RollingIcebergWriter<B, D>
where
    B: IcebergWriterBuilder<R = D>,
    D: IcebergWriter + CurrentFileStatus,
{
    /// Write a `RecordBatch`. If the current file size plus the new batch size
    /// exceeds the target, close the current file and start a new one.
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        let input_size = input.get_array_memory_size() as u64;

        if let Some(ref writer) = self.inner_writer {
            let current_physical_size = writer.current_written_size() as u64;
            let old_physical_size = self.size_tracker.get_last_physical_size();
            self.size_tracker
                .update_physical_size(current_physical_size);

            if current_physical_size != old_physical_size {
                tracing::debug!(
                    "Updated size estimation ratio: physical_growth={}, memory_size={}, current_ratio={:.3}",
                    current_physical_size - old_physical_size,
                    self.size_tracker.get_current_memory_size(),
                    self.size_tracker.get_ratio()
                );
            }
        }

        // Calculate estimated sizes
        let estimated_input_size = self.size_tracker.estimate_physical_size(input_size);
        let total_estimated_size = self.size_tracker.calculate_total_estimated_size();

        if self.size_tracker.is_enabled() {
            let estimated_memory_size = self
                .size_tracker
                .estimate_physical_size(self.size_tracker.get_current_memory_size());
            tracing::debug!(
                "Size estimation: physical={}, memory={}, estimated_memory={}, input={}, estimated_input={}, total_estimated={}, estimation_ratio={:.3}",
                self.size_tracker.get_last_physical_size(),
                self.size_tracker.get_current_memory_size(),
                estimated_memory_size,
                input_size,
                estimated_input_size,
                total_estimated_size,
                self.size_tracker.get_ratio()
            );
        }

        // If adding this batch would exceed the target file size, close current file and start a new one.
        if need_build_new_file(
            total_estimated_size,
            estimated_input_size,
            self.target_file_size,
        ) {
            if let Some(mut inner_writer) = self.inner_writer.take() {
                if self.close_futures.len() >= self.max_concurrent_closes {
                    self.wait_for_one_close().await?;
                }

                let unflushed_memory_size = if self.should_track_small_file() {
                    self.size_tracker.get_current_memory_size()
                } else {
                    0
                };

                let close_handle = tokio::spawn(async move {
                    let files = inner_writer.close().await?;
                    let small_file_info = if unflushed_memory_size > 0 && files.len() == 1 {
                        let final_physical_size = files[0].file_size_in_bytes();
                        Some((final_physical_size, unflushed_memory_size))
                    } else {
                        None
                    };

                    Ok((files, small_file_info))
                });
                self.close_futures.push(close_handle);
            }

            self.size_tracker.reset_for_new_file();
        }

        // Write the batch to the current writer.
        if self.inner_writer.is_none() {
            self.inner_writer = Some(
                self.inner_writer_builder
                    .clone()
                    .build(self.partition_key.clone())
                    .await?,
            );
        }
        self.inner_writer.as_mut().unwrap().write(input).await?;

        self.size_tracker.add_memory_size(input_size);
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let mut data_files = std::mem::take(&mut self.data_files);

        let close_futures = std::mem::take(&mut self.close_futures);
        for close_handle in close_futures {
            match close_handle.await {
                Ok(Ok((files, small_file_info))) => {
                    self.update_ratio_if_needed(small_file_info);
                    data_files.extend(files);
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(iceberg::Error::new(
                        ErrorKind::Unexpected,
                        format!("Failed to join close task: {}", e),
                    ));
                }
            }
        }

        if let Some(mut writer) = self.inner_writer.take() {
            let files = writer.close().await?;

            if self.should_track_small_file() {
                let unflushed_memory_size = self.size_tracker.get_current_memory_size();
                let small_file_info = self.extract_small_file_info(&files, unflushed_memory_size);
                self.update_ratio_if_needed(small_file_info);
            }

            data_files.extend(files);
        }
        Ok(data_files)
    }
}

impl<B, D> RollingIcebergWriter<B, D> {
    /// Check if this is a small file that needs ratio tracking
    fn should_track_small_file(&self) -> bool {
        self.size_tracker.is_enabled()
            && self.size_tracker.get_last_physical_size() == 0
            && self.size_tracker.get_current_memory_size() > 0
    }

    /// Extract small file info for ratio calculation from closed files
    fn extract_small_file_info(
        &self,
        files: &[DataFile],
        unflushed_memory_size: u64,
    ) -> Option<(u64, u64)> {
        if files.len() == 1 {
            let final_physical_size = files[0].file_size_in_bytes();
            if final_physical_size > 0 {
                return Some((final_physical_size, unflushed_memory_size));
            }
        }
        None
    }

    fn update_ratio_if_needed(&mut self, small_file_info: Option<(u64, u64)>) {
        if let Some((final_physical_size, unflushed_memory_size)) = small_file_info {
            self.size_tracker
                .update_ratio_from_complete_file(final_physical_size, unflushed_memory_size);
        }
    }

    async fn wait_for_one_close(&mut self) -> Result<()> {
        if self.close_futures.is_empty() {
            return Ok(());
        }

        let (result, _index, remaining) =
            future::select_all(std::mem::take(&mut self.close_futures)).await;

        self.close_futures = remaining;

        match result {
            Ok(Ok((files, small_file_info))) => {
                self.update_ratio_if_needed(small_file_info);
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
    if current_written_size < target_file_size / 10 {
        return false;
    }
    if current_written_size + input_size > target_file_size * 3 / 2 {
        return true;
    }
    if current_written_size + input_size > target_file_size
        && current_written_size > target_file_size * 7 / 10
    {
        return true;
    }
    false
}

#[derive(Clone)]
pub struct RollingIcebergWriterBuilder<B> {
    inner_builder: B,
    target_file_size: Option<u64>,
    max_concurrent_closes: Option<usize>,
    enable_dynamic_size_estimation: Option<bool>,
    size_estimation_smoothing_factor: Option<f64>,
    partition_key: Option<PartitionKey>,
}

impl<B> RollingIcebergWriterBuilder<B> {
    pub fn new(inner_builder: B) -> Self {
        Self {
            inner_builder,
            target_file_size: None,
            max_concurrent_closes: None,
            enable_dynamic_size_estimation: None,
            size_estimation_smoothing_factor: None,
            partition_key: None,
        }
    }

    pub fn with_target_file_size(mut self, target_file_size: u64) -> Self {
        self.target_file_size = Some(target_file_size);
        self
    }

    pub fn with_max_concurrent_closes(mut self, max_concurrent_closes: usize) -> Self {
        self.max_concurrent_closes = Some(max_concurrent_closes);
        self
    }

    pub fn with_dynamic_size_estimation(mut self, enable: bool) -> Self {
        self.enable_dynamic_size_estimation = Some(enable);
        self
    }

    /// Set the smoothing factor for size estimation updates.
    /// Lower values make the estimation more stable but slower to adapt.
    /// Higher values make the estimation more responsive but potentially more volatile.
    pub fn with_size_estimation_smoothing_factor(mut self, factor: f64) -> Self {
        self.size_estimation_smoothing_factor = Some(factor);
        self
    }

    pub fn with_partition_key(mut self, partition_key: PartitionKey) -> Self {
        self.partition_key = Some(partition_key);
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

    async fn build(self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        let enable_estimation = self
            .enable_dynamic_size_estimation
            .unwrap_or(DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION);
        let smoothing_factor = self
            .size_estimation_smoothing_factor
            .unwrap_or(DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR);

        Ok(RollingIcebergWriter {
            inner_writer_builder: self.inner_builder.clone(),
            inner_writer: Some(self.inner_builder.build(partition_key.clone()).await?),
            target_file_size: self.target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE),
            data_files: Vec::new(),
            size_tracker: SizeEstimationTracker::new(enable_estimation, smoothing_factor),
            close_futures: Vec::new(),
            max_concurrent_closes: self
                .max_concurrent_closes
                .unwrap_or(DEFAULT_MAX_CONCURRENT_CLOSES),
            partition_key: self.partition_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_need_build_new_file() {
        let target_size = 1000;

        // Total size exceeds 1.5x threshold
        assert!(need_build_new_file(800, 800, target_size));
        assert!(need_build_new_file(1000, 600, target_size));

        // Normal cases
        assert!(need_build_new_file(800, 300, target_size)); // >70% and exceeds target
        assert!(!need_build_new_file(800, 100, target_size)); // >70% but within target
        assert!(!need_build_new_file(600, 500, target_size)); // <70% even if exceeds target

        // Edge cases
        assert!(!need_build_new_file(0, 2000, target_size)); // Empty file
        assert!(!need_build_new_file(700, 400, target_size)); // Exactly at 70%
        assert!(need_build_new_file(701, 400, target_size)); // Just over 70%
        assert!(!need_build_new_file(0, 1500, target_size)); // Exactly at 1.5x
        assert!(!need_build_new_file(1, 1501, target_size)); // Just over 1.5x but tiny file
    }

    #[test]
    fn test_builder() {
        // Default configuration
        let builder = RollingIcebergWriterBuilder::new("dummy_inner_builder");
        assert!(builder.enable_dynamic_size_estimation.is_none());
        assert!(builder.size_estimation_smoothing_factor.is_none());

        // Custom configuration
        let builder = RollingIcebergWriterBuilder::new("dummy_inner_builder")
            .with_dynamic_size_estimation(true)
            .with_size_estimation_smoothing_factor(0.5);
        assert_eq!(builder.enable_dynamic_size_estimation, Some(true));
        assert_eq!(builder.size_estimation_smoothing_factor, Some(0.5));

        // Fluent interface
        let builder = RollingIcebergWriterBuilder::new("dummy_inner_builder")
            .with_target_file_size(1024 * 1024)
            .with_max_concurrent_closes(8)
            .with_dynamic_size_estimation(true)
            .with_size_estimation_smoothing_factor(0.2);

        assert_eq!(builder.target_file_size, Some(1024 * 1024));
        assert_eq!(builder.max_concurrent_closes, Some(8));
        assert_eq!(builder.enable_dynamic_size_estimation, Some(true));
        assert_eq!(builder.size_estimation_smoothing_factor, Some(0.2));
    }

    #[test]
    fn test_size_estimation_tracker_basic() {
        // Disabled tracker
        let mut tracker = SizeEstimationTracker::new(false, 0.3);
        assert!(!tracker.is_enabled());
        assert_eq!(tracker.get_ratio(), 1.0);

        tracker.add_memory_size(1000);
        assert_eq!(tracker.get_current_memory_size(), 1000);

        let estimated_memory = tracker.estimate_physical_size(500);
        let estimated_input = tracker.estimate_physical_size(200);
        assert_eq!(estimated_memory, 500);
        assert_eq!(estimated_input, 200);

        let total = tracker.calculate_total_estimated_size();
        assert_eq!(total, 1000); // 0 physical + 1000 memory (disabled, so 1:1 ratio)

        // Enabled tracker - basic functionality
        let mut tracker = SizeEstimationTracker::new(true, 0.3);
        assert!(tracker.is_enabled());
        assert_eq!(tracker.get_ratio(), 1.0);

        tracker.add_memory_size(1000);
        assert_eq!(tracker.get_current_memory_size(), 1000);

        tracker.update_physical_size(300);
        assert!((tracker.get_ratio() - 0.3).abs() < 0.001);
        assert_eq!(tracker.get_current_memory_size(), 0);
        assert_eq!(tracker.get_last_physical_size(), 300);

        // No memory data case
        tracker.update_physical_size(500);
        assert_eq!(tracker.get_ratio(), 0.3); // Should remain unchanged
    }

    #[test]
    fn test_size_estimation_tracker_advanced() {
        let mut tracker = SizeEstimationTracker::new(true, 0.3);

        // Test with learned ratio
        tracker.add_memory_size(1000);
        tracker.update_physical_size(400);
        assert!((tracker.get_ratio() - 0.4).abs() < 0.001);

        let estimated_memory = tracker.estimate_physical_size(1000);
        let estimated_input = tracker.estimate_physical_size(500);
        assert_eq!(estimated_memory, 400);
        assert_eq!(estimated_input, 200);

        tracker.add_memory_size(800);
        let total = tracker.calculate_total_estimated_size();
        assert_eq!(total, 720); // 400 + 320

        // Test reset preserves ratio
        let original_ratio = tracker.get_ratio();
        tracker.reset_for_new_file();
        assert_eq!(tracker.get_last_physical_size(), 0);
        assert_eq!(tracker.get_current_memory_size(), 0);
        assert_eq!(tracker.get_ratio(), original_ratio);

        // Test smoothing with multiple updates
        let mut tracker = SizeEstimationTracker::new(true, 0.5);
        tracker.add_memory_size(1000);
        tracker.update_physical_size(300);
        assert!((tracker.get_ratio() - 0.3).abs() < 0.001);

        tracker.add_memory_size(1000);
        tracker.update_physical_size(1000);
        let expected_ratio = 0.5 * 0.3 + 0.5 * 0.7;
        assert!((tracker.get_ratio() - expected_ratio).abs() < 0.001);
    }

    #[test]
    fn test_update_ratio_from_complete_file_basic() {
        let mut tracker = SizeEstimationTracker::new(true, 0.3);

        tracker.add_memory_size(1000);
        assert_eq!(tracker.get_current_memory_size(), 1000);
        assert_eq!(tracker.get_last_physical_size(), 0);

        let final_physical_size = 400u64;
        let memory_size = 1000u64;

        tracker.update_ratio_from_complete_file(final_physical_size, memory_size);

        assert!((tracker.get_ratio() - 0.4).abs() < 0.001);
    }

    #[test]
    fn test_update_ratio_from_complete_file_uses_passed_parameters() {
        let mut tracker = SizeEstimationTracker::new(true, 0.5);

        // Set up tracker with some state
        tracker.add_memory_size(2000);
        tracker.update_physical_size(800); // This would give ratio 0.4
        assert!((tracker.get_ratio() - 0.4).abs() < 0.001);

        // Reset for new file (this simulates what happens when starting a new file)
        tracker.reset_for_new_file();
        assert_eq!(tracker.get_current_memory_size(), 0);
        assert_eq!(tracker.get_last_physical_size(), 0);

        // Add some memory for the new file
        tracker.add_memory_size(500);

        // Now use update_ratio_from_complete_file with different values
        // This simulates the bug fix: use the passed parameters, not tracker state
        let final_physical_size = 150u64; // This gives ratio 0.3 (150/500)
        let memory_size = 500u64;

        tracker.update_ratio_from_complete_file(final_physical_size, memory_size);

        // Ratio should be smoothed: 0.5 * 0.4 + 0.5 * 0.3 = 0.35
        let expected_ratio = 0.5 * 0.4 + 0.5 * 0.3;
        assert!((tracker.get_ratio() - expected_ratio).abs() < 0.001);
    }

    #[test]
    fn test_update_ratio_from_complete_file_multiple_small_files() {
        let mut tracker = SizeEstimationTracker::new(true, 0.2); // Low smoothing for more stable results

        // First small file: 1000 memory -> 300 physical (ratio 0.3)
        tracker.update_ratio_from_complete_file(300, 1000);
        assert!((tracker.get_ratio() - 0.3).abs() < 0.001);

        // Second small file: 800 memory -> 400 physical (ratio 0.5)
        // Expected: 0.8 * 0.3 + 0.2 * 0.5 = 0.24 + 0.1 = 0.34
        tracker.update_ratio_from_complete_file(400, 800);
        let expected_ratio = 0.8 * 0.3 + 0.2 * 0.5;
        assert!((tracker.get_ratio() - expected_ratio).abs() < 0.001);

        // Third small file: 1200 memory -> 600 physical (ratio 0.5)
        // Expected: 0.8 * 0.34 + 0.2 * 0.5 = 0.272 + 0.1 = 0.372
        tracker.update_ratio_from_complete_file(600, 1200);
        let expected_ratio = 0.8 * expected_ratio + 0.2 * 0.5;
        assert!((tracker.get_ratio() - expected_ratio).abs() < 0.001);
    }

    #[test]
    fn test_update_ratio_from_complete_file_edge_cases() {
        let mut tracker = SizeEstimationTracker::new(true, 0.3);

        // Case 1: Zero memory size - should not update ratio
        let original_ratio = tracker.get_ratio();
        tracker.update_ratio_from_complete_file(100, 0);
        assert_eq!(tracker.get_ratio(), original_ratio);

        // Case 2: Zero physical size - should not update ratio
        tracker.update_ratio_from_complete_file(0, 1000);
        assert_eq!(tracker.get_ratio(), original_ratio);

        // Case 3: Disabled tracker - should not update ratio
        let mut disabled_tracker = SizeEstimationTracker::new(false, 0.3);
        let disabled_original_ratio = disabled_tracker.get_ratio();
        disabled_tracker.update_ratio_from_complete_file(500, 1000);
        assert_eq!(disabled_tracker.get_ratio(), disabled_original_ratio);
    }

    #[test]
    fn test_update_ratio_from_complete_file_preserves_ratio_across_resets() {
        let mut tracker = SizeEstimationTracker::new(true, 0.3);

        tracker.update_ratio_from_complete_file(400, 1000);
        assert!((tracker.get_ratio() - 0.4).abs() < 0.001);

        for _ in 0..3 {
            tracker.reset_for_new_file();
            assert_eq!(tracker.get_current_memory_size(), 0);
            assert_eq!(tracker.get_last_physical_size(), 0);
            assert!((tracker.get_ratio() - 0.4).abs() < 0.001);
        }

        tracker.update_ratio_from_complete_file(600, 1000);
        let expected_ratio = 0.7 * 0.4 + 0.3 * 0.6;
        assert!((tracker.get_ratio() - expected_ratio).abs() < 0.001);
    }

    #[derive(Clone)]
    struct MockIcebergWriterBuilder {
        should_flush: bool,
        compression_ratio: f64,
    }

    impl MockIcebergWriterBuilder {
        fn new(should_flush: bool, compression_ratio: f64) -> Self {
            Self {
                should_flush,
                compression_ratio,
            }
        }
    }

    #[async_trait::async_trait]
    impl IcebergWriterBuilder for MockIcebergWriterBuilder {
        type R = MockIcebergWriter;

        async fn build(self, _partition_key: Option<PartitionKey>) -> Result<Self::R> {
            Ok(MockIcebergWriter {
                written_size: 0,
                should_flush: self.should_flush,
                total_memory_written: 0,
                compression_ratio: self.compression_ratio,
            })
        }
    }

    struct MockIcebergWriter {
        written_size: usize,
        should_flush: bool,
        total_memory_written: u64,
        compression_ratio: f64,
    }

    #[async_trait::async_trait]
    impl IcebergWriter for MockIcebergWriter {
        async fn write(&mut self, input: RecordBatch) -> Result<()> {
            let memory_size = input.get_array_memory_size();
            self.total_memory_written += memory_size as u64;

            if self.should_flush {
                let physical_growth = (memory_size as f64 * self.compression_ratio) as usize;
                self.written_size += physical_growth;
            }

            Ok(())
        }

        async fn close(&mut self) -> Result<Vec<DataFile>> {
            let final_size = if self.should_flush {
                self.written_size as u64
            } else {
                (self.total_memory_written as f64 * self.compression_ratio) as u64
            };

            Ok(vec![create_mock_data_file(final_size)])
        }
    }

    impl CurrentFileStatus for MockIcebergWriter {
        fn current_written_size(&self) -> usize {
            self.written_size
        }

        fn current_file_path(&self) -> String {
            "mock_file.parquet".to_owned()
        }

        fn current_row_num(&self) -> usize {
            100
        }
    }

    // Helper function to create mock DataFile using DataFileBuilder
    fn create_mock_data_file(size: u64) -> iceberg::spec::DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Struct};

        DataFileBuilder::default()
            .content(iceberg::spec::DataContentType::Data)
            .file_path("mock_file.parquet".to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(100)
            .file_size_in_bytes(size)
            .key_metadata(None)
            .partition_spec_id(0)
            .build()
            .expect("Failed to build mock DataFile")
    }

    fn create_mock_record_batch(memory_size: usize) -> RecordBatch {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let num_rows = std::cmp::max(1, memory_size / 16);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let id_array = Int64Array::from_iter_values(0..num_rows as i64);
        let name_array =
            StringArray::from_iter_values((0..num_rows).map(|i| format!("name_{}", i)));

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_rolling_iceberg_writer_small_file_ratio_learning() {
        let compression_ratio = 0.3;
        let mock_builder = MockIcebergWriterBuilder::new(false, compression_ratio);

        let mut rolling_writer = RollingIcebergWriterBuilder::new(mock_builder)
            .with_target_file_size(10000)
            .with_dynamic_size_estimation(true)
            .with_size_estimation_smoothing_factor(0.5)
            .build(None)
            .await
            .unwrap();

        assert_eq!(rolling_writer.size_tracker.get_ratio(), 1.0);

        let batch1 = create_mock_record_batch(400);
        let batch2 = create_mock_record_batch(300);
        let batch3 = create_mock_record_batch(300);

        let total_memory = batch1.get_array_memory_size() as u64
            + batch2.get_array_memory_size() as u64
            + batch3.get_array_memory_size() as u64;

        rolling_writer.write(batch1).await.unwrap();
        rolling_writer.write(batch2).await.unwrap();
        rolling_writer.write(batch3).await.unwrap();

        assert_eq!(rolling_writer.size_tracker.get_ratio(), 1.0);
        assert_eq!(rolling_writer.size_tracker.get_last_physical_size(), 0);

        let data_files = rolling_writer.close().await.unwrap();

        assert_eq!(data_files.len(), 1);

        let expected_file_size = (total_memory as f64 * compression_ratio) as u64;
        assert_eq!(data_files[0].file_size_in_bytes(), expected_file_size);

        assert!((rolling_writer.size_tracker.get_ratio() - compression_ratio).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_rolling_iceberg_writer_large_file_vs_small_file() {
        // Test mixed scenario: large file with flushes, then small file without flushes
        let compression_ratio = 0.4;

        // First, test large file with flushes
        let mock_builder_large = MockIcebergWriterBuilder::new(true, compression_ratio); // Enable flushes

        let mut rolling_writer = RollingIcebergWriterBuilder::new(mock_builder_large.clone())
            .with_target_file_size(10000)
            .with_dynamic_size_estimation(true)
            .with_size_estimation_smoothing_factor(0.5)
            .build(None)
            .await
            .unwrap();

        // Write to large file (triggers flushes) - multiple writes for realistic flush detection
        let large_batch1 = create_mock_record_batch(500);
        let large_batch2 = create_mock_record_batch(500);

        // First write - creates flush but ratio not detected yet
        rolling_writer.write(large_batch1).await.unwrap();
        // Second write - should detect the flush from first write and update ratio
        rolling_writer.write(large_batch2).await.unwrap();

        // Should have learned ratio from flush
        let actual_ratio_after_large = rolling_writer.size_tracker.get_ratio();
        assert!((actual_ratio_after_large - compression_ratio).abs() < 0.001);

        let data_files = rolling_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);

        // Now test small file scenario with different compression ratio
        let small_compression_ratio = 0.25;
        let mock_builder_small = MockIcebergWriterBuilder::new(false, small_compression_ratio); // No flushes

        let mut rolling_writer = RollingIcebergWriterBuilder::new(mock_builder_small)
            .with_target_file_size(10000)
            .with_dynamic_size_estimation(true)
            .with_size_estimation_smoothing_factor(0.5)
            .build(None)
            .await
            .unwrap();

        // Manually set the learned ratio from previous file
        rolling_writer.size_tracker.ratio = Some(compression_ratio);

        // Write small batch
        let small_batch = create_mock_record_batch(500);
        let actual_memory_size = small_batch.get_array_memory_size() as u64;
        rolling_writer.write(small_batch).await.unwrap();

        // Should use existing ratio for estimation
        let estimated_size = rolling_writer.size_tracker.calculate_total_estimated_size();
        let expected_estimated = (actual_memory_size as f64 * compression_ratio) as u64; // 0 physical + estimated memory
        assert_eq!(estimated_size, expected_estimated);

        // Close and verify ratio is updated with EMA
        let data_files = rolling_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);

        // Mock calculates final_size as total_memory_written * small_compression_ratio
        let expected_final_size = (actual_memory_size as f64 * small_compression_ratio) as u64;
        assert_eq!(data_files[0].file_size_in_bytes(), expected_final_size);

        // Ratio from close: expected_final_size / actual_memory_size = small_compression_ratio
        // EMA: 0.5 * compression_ratio + 0.5 * small_compression_ratio
        let expected_ema_ratio = 0.5 * compression_ratio + 0.5 * small_compression_ratio;
        let actual_ratio = rolling_writer.size_tracker.get_ratio();
        assert!((actual_ratio - expected_ema_ratio).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_rolling_iceberg_writer_multiple_small_files() {
        // Test multiple small files to verify ratio learning accumulation
        let compression_ratio = 0.35;
        let mock_builder = MockIcebergWriterBuilder::new(false, compression_ratio);

        let mut rolling_writer = RollingIcebergWriterBuilder::new(mock_builder.clone())
            .with_target_file_size(500) // Small target to trigger multiple files
            .with_dynamic_size_estimation(true)
            .with_size_estimation_smoothing_factor(0.3) // Moderate smoothing
            .build(None)
            .await
            .unwrap();

        let mut all_data_files = Vec::new();

        // Write multiple batches to create multiple small files
        for i in 0..3 {
            let batch = create_mock_record_batch(600); // Each batch ~600 bytes, exceeds target of 500
            rolling_writer.write(batch).await.unwrap();

            // Force file rollover by writing another batch that exceeds target
            if i < 2 {
                // Don't force rollover on last iteration
                let trigger_batch = create_mock_record_batch(100);
                rolling_writer.write(trigger_batch).await.unwrap();
            }
        }

        // Close final file and collect all results
        let final_data_files = rolling_writer.close().await.unwrap();
        all_data_files.extend(final_data_files);

        // Should have learned ratio progressively
        // Each small file should contribute to ratio learning with compression_ratio
        let final_ratio = rolling_writer.size_tracker.get_ratio();
        assert!(
            (final_ratio - compression_ratio).abs() < 0.1,
            "Final ratio: {}, expected: {}",
            final_ratio,
            compression_ratio
        );

        // Verify files were created
        assert!(!all_data_files.is_empty());

        // Verify each file has correct compression ratio
        for file in &all_data_files {
            // The file size should reflect the compression ratio
            assert!(file.file_size_in_bytes() > 0);
        }
    }

    #[tokio::test]
    async fn test_rolling_iceberg_writer_edge_cases() {
        // Case 1: Zero compression ratio (file size same as memory)
        let mock_builder_no_compression = MockIcebergWriterBuilder::new(false, 1.0);
        let mut rolling_writer = RollingIcebergWriterBuilder::new(mock_builder_no_compression)
            .with_target_file_size(10000)
            .with_dynamic_size_estimation(true)
            .build(None)
            .await
            .unwrap();

        let batch = create_mock_record_batch(1000);
        let memory_size = batch.get_array_memory_size() as u64;
        rolling_writer.write(batch).await.unwrap();

        let data_files = rolling_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].file_size_in_bytes(), memory_size);
        assert!((rolling_writer.size_tracker.get_ratio() - 1.0).abs() < 0.001);

        // Case 2: Very high compression ratio
        let mock_builder_high_compression = MockIcebergWriterBuilder::new(false, 0.1);
        let mut rolling_writer = RollingIcebergWriterBuilder::new(mock_builder_high_compression)
            .with_target_file_size(10000)
            .with_dynamic_size_estimation(true)
            .build(None)
            .await
            .unwrap();

        let batch = create_mock_record_batch(1000);
        let memory_size = batch.get_array_memory_size() as u64;
        rolling_writer.write(batch).await.unwrap();

        let data_files = rolling_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);
        assert_eq!(
            data_files[0].file_size_in_bytes(),
            (memory_size as f64 * 0.1) as u64
        );
        assert!((rolling_writer.size_tracker.get_ratio() - 0.1).abs() < 0.001);
    }
}
