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

pub mod metrics;

use std::num::NonZeroUsize;

// Re-export metrics components
pub use metrics::CompactionMetricsRecorder;
pub use metrics::Metrics;

// Use a default value of 1 as the safest option.
// See https://doc.rust-lang.org/std/thread/fn.available_parallelism.html#limitations
// for more details.
const DEFAULT_PARALLELISM: usize = 1;

pub(crate) fn available_parallelism() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or_else(|_err| {
        // Failed to get the level of parallelism.

        // Using a default value.
        NonZeroUsize::new(DEFAULT_PARALLELISM).unwrap()
    })
}
