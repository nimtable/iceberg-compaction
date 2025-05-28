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

use mixtrics::metrics::{BoxedCounterVec, BoxedHistogramVec, BoxedRegistry, Buckets};

pub struct Metrics {
    pub compaction_commit_counter: BoxedCounterVec,
    pub compaction_duration: BoxedHistogramVec,

    pub compaction_rewritten_bytes: BoxedCounterVec,
    pub compaction_rewritten_files_count: BoxedCounterVec,
    pub compaction_added_files_count: BoxedCounterVec,
    pub compaction_failed_data_files_count: BoxedCounterVec,

    pub compaction_commit_duration: BoxedHistogramVec,
}

impl Metrics {
    pub fn new(registry: BoxedRegistry) -> Self {
        let compaction_commit_counter = registry.register_counter_vec(
            "compaction_commit_counter".into(),
            "BergLoom compaction total commit counts".into(),
            &["table_ident"],
        );

        let compaction_duration = registry.register_histogram_vec_with_buckets(
            "compaction_duration".into(),
            "BergLoom compaction duration in seconds".into(),
            &["table_ident"],
            Buckets::exponential(
                1.0, 2.0, 20, // Start at 1 second, double each bucket, up to 20 buckets
            ),
        );

        let compaction_rewritten_bytes = registry.register_counter_vec(
            "compaction_rewritten_bytes".into(),
            "BergLoom compaction rewritten bytes".into(),
            &["table_ident"],
        );

        let compaction_rewritten_files_count = registry.register_counter_vec(
            "compaction_rewritten_files_count".into(),
            "BergLoom compaction rewritten files count".into(),
            &["table_ident"],
        );

        let compaction_added_files_count = registry.register_counter_vec(
            "compaction_added_files_count".into(),
            "BergLoom compaction added files count".into(),
            &["table_ident"],
        );

        let compaction_failed_data_files_count = registry.register_counter_vec(
            "compaction_failed_data_files_count".into(),
            "BergLoom compaction failed data files count".into(),
            &["table_ident"],
        );

        let compaction_commit_duration = registry.register_histogram_vec_with_buckets(
            "compaction_commit_duration".into(),
            "BergLoom compaction commit duration in milliseconds".into(),
            &["table_ident"],
            Buckets::exponential(
                1.0, 2.0, 20, // Start at 1 millisecond, double each bucket, up to 20 buckets
            ),
        );

        Self {
            compaction_commit_counter,
            compaction_duration,
            compaction_rewritten_bytes,
            compaction_rewritten_files_count,
            compaction_added_files_count,
            compaction_failed_data_files_count,
            compaction_commit_duration,
        }
    }
}
