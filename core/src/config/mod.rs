/*
 * Copyright 2025 BergLoom
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

use serde::Deserialize;
use serde_with::serde_as;

const DEFAULT_PREFIX: &str = "10";

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct CompactionConfig {
    #[serde(default = "default_batch_parallelism")]
    pub batch_parallelism: usize,
    #[serde(default = "default_target_partitions")]
    pub target_partitions: usize,
    #[serde(default = "default_data_file_prefix")]
    pub data_file_prefix: String,
    #[serde(default = "default_target_file_size")]
    pub target_file_size: usize,
    #[serde(default = "default_max_record_batch_rows")]
    pub max_record_batch_rows: usize,
}

fn default_batch_parallelism() -> usize {
    4
}

fn default_target_partitions() -> usize {
    4
}

fn default_data_file_prefix() -> String {
    DEFAULT_PREFIX.to_string()
}

fn default_target_file_size() -> usize {
    1024 * 1024 * 1024 // 1 GB
}

fn default_max_record_batch_rows() -> usize {
    1024
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            batch_parallelism: default_batch_parallelism(),
            target_partitions: default_target_partitions(),
            data_file_prefix: default_data_file_prefix(),
            target_file_size: default_target_file_size(),
            max_record_batch_rows: default_max_record_batch_rows(),
        }
    }
}
