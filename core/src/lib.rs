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

pub mod common;
pub mod compaction;
pub mod config;
pub mod error;
pub mod executor;
pub mod file_selection;
pub mod test_utils;

pub use config::CompactionConfig;
pub use error::{CompactionError, Result};
pub use executor::CompactionExecutor;
pub use file_selection::strategy::FileStrategyFactory;

// Re-export iceberg related crates
pub use iceberg;
pub use iceberg_catalog_memory;
pub use iceberg_catalog_rest;
