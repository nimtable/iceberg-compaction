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
use iceberg::{
    spec::DataFile,
    writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder},
};
use iceberg::{ErrorKind, Result};
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
    /// Current size estimation ratio (physical_size / memory_size), None if not learned yet
    ratio: Option<f64>,
    /// Smoothing factor for ratio updates
    smoothing_factor: f64,
    /// Last known physical written size
    last_physical_size: u64,
    /// Current in-memory size that hasn't been flushed
    current_memory_size: u64,
}

impl SizeEstimationTracker {
    /// Create a new size estimation tracker
    pub fn new(enabled: bool, smoothing_factor: f64) -> Self {
        Self {
            enabled,
            ratio: None, // No learned ratio yet
            smoothing_factor,
            last_physical_size: 0,
            current_memory_size: 0,
        }
    }

    /// Update the tracker with new physical size and memory size information
    pub fn update_physical_size(&mut self, new_physical_size: u64) {
        if new_physical_size == self.last_physical_size {
            return; // No change
        }

        let physical_growth = new_physical_size - self.last_physical_size;

        // Update ratio if enabled and we have memory data to compare against
        if self.enabled && self.current_memory_size > 0 {
            let calculated_ratio = physical_growth as f64 / self.current_memory_size as f64;

            // Use exponential moving average to smooth the ratio, or set initial ratio
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

        self.last_physical_size = new_physical_size;
        self.current_memory_size = 0; // Reset memory size after flush
    }

    /// Add memory size to the tracker
    pub fn add_memory_size(&mut self, size: u64) {
        self.current_memory_size += size;
    }

    /// Estimate the actual size of memory data and input data
    pub fn estimate_sizes(&self, memory_size: u64, input_size: u64) -> (u64, u64) {
        if self.enabled {
            if let Some(ratio) = self.ratio {
                let estimated_memory_size = (memory_size as f64 * ratio) as u64;
                let estimated_input_size = (input_size as f64 * ratio) as u64;
                (estimated_memory_size, estimated_input_size)
            } else {
                // No learned ratio yet, use conservative 1:1 assumption
                (memory_size, input_size)
            }
        } else {
            // Conservative 1:1 assumption when disabled
            (memory_size, input_size)
        }
    }

    /// Calculate total estimated size including physical size
    pub fn calculate_total_estimated_size(&self, memory_size: u64, input_size: u64) -> u64 {
        let (estimated_memory_size, _) = self.estimate_sizes(memory_size, input_size);
        self.last_physical_size + estimated_memory_size
    }

    /// Reset tracking for a new file (but keep the learned ratio)
    pub fn reset_for_new_file(&mut self) {
        self.last_physical_size = 0;
        self.current_memory_size = 0;
        // Keep the ratio as it's still useful for the new file
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
}

/// RollingIcebergWriter wraps an IcebergWriter and splits output files by target size.
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
/// ```rust,no_run
/// use iceberg_compaction_core::executor::iceberg_writer::rolling_iceberg_writer::RollingIcebergWriterBuilder;
///
/// // Create a basic rolling writer (dynamic size estimation disabled by default)
/// let writer = RollingIcebergWriterBuilder::new(inner_builder)
///     .with_target_file_size(1024 * 1024 * 1024) // 1GB
///     .build()
///     .await?;
/// ```
///
/// ## With Dynamic Size Estimation
/// ```rust,no_run
/// use iceberg_compaction_core::executor::iceberg_writer::rolling_iceberg_writer::RollingIcebergWriterBuilder;
///
/// // Create a rolling writer with dynamic size estimation enabled
/// let writer = RollingIcebergWriterBuilder::new(inner_builder)
///     .with_target_file_size(1024 * 1024 * 1024) // 1GB
///     .with_dynamic_size_estimation(true)       // Enable dynamic size estimation
///     .with_size_estimation_smoothing_factor(0.3) // Adjust smoothing (optional)
///     .build()
///     .await?;
/// ```
///
/// ## Custom Configuration
/// ```rust,no_run
/// use iceberg_compaction_core::executor::iceberg_writer::rolling_iceberg_writer::RollingIcebergWriterBuilder;
///
/// // Fully customized rolling writer
/// let writer = RollingIcebergWriterBuilder::new(inner_builder)
///     .with_target_file_size(512 * 1024 * 1024)  // 512MB
///     .with_max_concurrent_closes(8)              // Allow 8 concurrent closes
///     .with_dynamic_size_estimation(true)         // Enable dynamic size estimation
///     .with_size_estimation_smoothing_factor(0.2) // More stable estimation
///     .build()
///     .await?;
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
        let input_size = input.get_array_memory_size() as u64;

        // Update physical written size and reset memory size if it changed
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

        // Calculate total estimated size using size tracker
        let total_estimated_size = self.size_tracker.calculate_total_estimated_size(
            self.size_tracker.get_current_memory_size(),
            input_size,
        );

        let (estimated_memory_size, estimated_input_size) = self
            .size_tracker
            .estimate_sizes(self.size_tracker.get_current_memory_size(), input_size);

        if self.size_tracker.is_enabled() {
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
            // Take the current writer and spawn its close operation
            if let Some(mut inner_writer) = self.inner_writer.take() {
                // If we've reached the max concurrent closes, wait for one to complete
                if self.close_futures.len() >= self.max_concurrent_closes {
                    self.wait_for_one_close().await?;
                }

                let close_handle = tokio::spawn(async move { inner_writer.close().await });
                self.close_futures.push(close_handle);
            }

            // Reset size tracking for new file but keep size estimation ratio
            self.size_tracker.reset_for_new_file();
        }

        // Write the batch to the current writer.
        if self.inner_writer.is_none() {
            self.inner_writer = Some(self.inner_writer_builder.clone().build().await?);
        }
        self.inner_writer.as_mut().unwrap().write(input).await?;

        // Update memory size tracking
        self.size_tracker.add_memory_size(input_size);
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
                    ));
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
    enable_dynamic_size_estimation: Option<bool>,
    size_estimation_smoothing_factor: Option<f64>,
}

impl<B> RollingIcebergWriterBuilder<B> {
    /// Create a new RollingIcebergWriterBuilder.
    pub fn new(inner_builder: B) -> Self {
        Self {
            inner_builder,
            target_file_size: None,
            max_concurrent_closes: None,
            enable_dynamic_size_estimation: None,
            size_estimation_smoothing_factor: None,
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

    /// Enable dynamic size estimation for better file size prediction.
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
        let enable_estimation = self
            .enable_dynamic_size_estimation
            .unwrap_or(DEFAULT_ENABLE_DYNAMIC_SIZE_ESTIMATION);
        let smoothing_factor = self
            .size_estimation_smoothing_factor
            .unwrap_or(DEFAULT_SIZE_ESTIMATION_SMOOTHING_FACTOR);

        Ok(RollingIcebergWriter {
            inner_writer_builder: self.inner_builder.clone(),
            inner_writer: Some(self.inner_builder.build().await?),
            target_file_size: self.target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE),
            data_files: Vec::new(),
            size_tracker: SizeEstimationTracker::new(enable_estimation, smoothing_factor),
            close_futures: Vec::new(),
            max_concurrent_closes: self
                .max_concurrent_closes
                .unwrap_or(DEFAULT_MAX_CONCURRENT_CLOSES),
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

    #[test]
    fn test_builder_configuration() {
        // Test builder with default values
        let builder = RollingIcebergWriterBuilder::new("dummy_inner_builder");
        assert!(builder.enable_dynamic_size_estimation.is_none());
        assert!(builder.size_estimation_smoothing_factor.is_none());

        // Test builder with custom values
        let builder = RollingIcebergWriterBuilder::new("dummy_inner_builder")
            .with_dynamic_size_estimation(true)
            .with_size_estimation_smoothing_factor(0.5);
        assert_eq!(builder.enable_dynamic_size_estimation, Some(true));
        assert_eq!(builder.size_estimation_smoothing_factor, Some(0.5));
    }

    #[test]
    fn test_builder_fluent_interface() {
        // Test that all builder methods can be chained
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

    // Tests for SizeEstimationTracker
    #[test]
    fn test_size_estimation_tracker_disabled() {
        let mut tracker = SizeEstimationTracker::new(false, 0.3);

        // When disabled, should always use 1:1 ratio
        assert!(!tracker.is_enabled());
        assert_eq!(tracker.get_ratio(), 1.0);

        // Adding memory size should work
        tracker.add_memory_size(1000);
        assert_eq!(tracker.get_current_memory_size(), 1000);

        // Size estimation should be 1:1
        let (estimated_memory, estimated_input) = tracker.estimate_sizes(500, 200);
        assert_eq!(estimated_memory, 500);
        assert_eq!(estimated_input, 200);

        // Total estimation should be physical + memory (1:1)
        let total = tracker.calculate_total_estimated_size(500, 200);
        assert_eq!(total, 500); // 0 physical + 500 memory
    }

    #[test]
    fn test_size_estimation_tracker_enabled_basic() {
        let mut tracker = SizeEstimationTracker::new(true, 0.3);

        assert!(tracker.is_enabled());
        assert_eq!(tracker.get_ratio(), 1.0); // Initial ratio

        // Add some memory size
        tracker.add_memory_size(1000);
        assert_eq!(tracker.get_current_memory_size(), 1000);

        // Update with physical size (simulating flush)
        // If 1000 bytes memory became 300 bytes physical, ratio should be 0.3
        tracker.update_physical_size(300);
        assert!((tracker.get_ratio() - 0.3).abs() < 0.001);

        // Memory size should be reset after update
        assert_eq!(tracker.get_current_memory_size(), 0);
        assert_eq!(tracker.get_last_physical_size(), 300);
    }

    #[test]
    fn test_size_estimation_tracker_smoothing() {
        let mut tracker = SizeEstimationTracker::new(true, 0.5); // 50% smoothing

        // First update: 1000 memory -> 300 physical (ratio 0.3)
        tracker.add_memory_size(1000);
        tracker.update_physical_size(300);
        assert!((tracker.get_ratio() - 0.3).abs() < 0.001);

        // Second update: 1000 memory -> 700 physical (new ratio 0.7)
        // With 50% smoothing: 0.5 * 0.3 + 0.5 * 0.7 = 0.5
        tracker.add_memory_size(1000);
        tracker.update_physical_size(1000); // Physical size goes from 300 to 1000
        let expected_ratio = 0.5 * 0.3 + 0.5 * 0.7; // EMA calculation
        assert!((tracker.get_ratio() - expected_ratio).abs() < 0.001);
    }

    #[test]
    fn test_size_estimation_tracker_no_memory_data() {
        let mut tracker = SizeEstimationTracker::new(true, 0.3);

        // Update physical size without any memory data
        tracker.update_physical_size(500);
        assert_eq!(tracker.get_ratio(), 1.0); // Should remain initial ratio
    }

    #[test]
    fn test_size_estimation_tracker_estimate_with_learned_ratio() {
        let mut tracker = SizeEstimationTracker::new(true, 0.3);

        // Learn a ratio of 0.4
        tracker.add_memory_size(1000);
        tracker.update_physical_size(400);
        assert!((tracker.get_ratio() - 0.4).abs() < 0.001);

        // Now estimate sizes with the learned ratio
        let (estimated_memory, estimated_input) = tracker.estimate_sizes(1000, 500);
        assert_eq!(estimated_memory, 400); // 1000 * 0.4
        assert_eq!(estimated_input, 200); // 500 * 0.4

        // Test total estimation
        tracker.add_memory_size(800);
        let total = tracker.calculate_total_estimated_size(800, 300);
        // Physical (400) + estimated memory (800 * 0.4 = 320) = 720
        assert_eq!(total, 720);
    }

    #[test]
    fn test_size_estimation_tracker_reset_for_new_file() {
        let mut tracker = SizeEstimationTracker::new(true, 0.3);

        // Set up some state
        tracker.add_memory_size(1000);
        tracker.update_physical_size(400);
        let original_ratio = tracker.get_ratio();

        // Reset for new file
        tracker.reset_for_new_file();

        // Sizes should be reset but ratio should be preserved
        assert_eq!(tracker.get_last_physical_size(), 0);
        assert_eq!(tracker.get_current_memory_size(), 0);
        assert_eq!(tracker.get_ratio(), original_ratio); // Ratio preserved
    }

    #[test]
    fn test_size_estimation_tracker_multiple_updates() {
        let mut tracker = SizeEstimationTracker::new(true, 0.2); // Low smoothing for stability

        // Simulate multiple flush cycles
        let test_cases = vec![
            (1000, 300), // ratio 0.3
            (2000, 800), // ratio 0.4
            (1500, 600), // ratio 0.4
            (1200, 480), // ratio 0.4
        ];

        let mut last_physical = 0;
        for (memory_size, total_physical) in test_cases {
            tracker.add_memory_size(memory_size);
            tracker.update_physical_size(last_physical + total_physical);
            last_physical += total_physical;
        }

        // Final ratio should be close to 0.4 due to smoothing
        let final_ratio = tracker.get_ratio();
        assert!(final_ratio > 0.3 && final_ratio < 0.5);
    }

    #[test]
    fn test_size_estimation_integration() {
        // Test the integration between all components
        let tracker = SizeEstimationTracker::new(true, 0.3);

        // Test that sizes make sense
        let (mem_est, input_est) = tracker.estimate_sizes(1000, 500);
        let total = tracker.calculate_total_estimated_size(1000, 500);

        // With initial 1.0 ratio
        assert_eq!(mem_est, 1000);
        assert_eq!(input_est, 500);
        assert_eq!(total, 1000); // 0 physical + 1000 memory

        // Test need_build_new_file with realistic values
        assert!(!need_build_new_file(total, input_est, 10000)); // Should not build new file
        assert!(need_build_new_file(8000, 3000, 10000)); // Should build new file (11000 > 10000 and 8000 > 7000)
    }
}
