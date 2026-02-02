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

//! Automatic compaction with runtime strategy selection.
//!
//! This module provides [`AutoCompactionPlanner`] for single-scan planning and
//! [`AutoCompaction`] for end-to-end automatic compaction workflows.

use std::borrow::Cow;
use std::sync::Arc;

use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use mixtrics::metrics::BoxedRegistry;

use super::{
    CommitManagerRetryConfig, Compaction, CompactionBuilder, CompactionPlan, CompactionResult,
};
use crate::Result;
use crate::config::AutoCompactionConfig;
use crate::executor::ExecutorType;
use crate::file_selection::{FileSelector, PlanStrategy, SnapshotStats};

/// Planner that performs analysis and plan generation in a single scan.
///
/// Combines snapshot analysis (stats computation) and file grouping into one
/// `plan_files()` call, avoiding the redundant IO of separate analyze-then-plan flows.
pub struct AutoCompactionPlanner {
    config: AutoCompactionConfig,
}

impl AutoCompactionPlanner {
    pub fn new(config: AutoCompactionConfig) -> Self {
        Self { config }
    }

    /// Plans compaction for a table branch.
    ///
    /// Returns empty vector if no files need compaction.
    pub async fn plan_compaction_with_branch(
        &self,
        table: &Table,
        to_branch: &str,
    ) -> Result<Vec<CompactionPlan>> {
        let Some(snapshot) = table.metadata().snapshot_for_ref(to_branch) else {
            return Ok(vec![]);
        };

        let snapshot_id = snapshot.snapshot_id();

        let tasks = FileSelector::scan_data_files(table, snapshot_id).await?;
        let undersized_threshold_bytes = self
            .config
            .maintenance_undersized_threshold_bytes
            .unwrap_or(self.config.target_file_size_bytes);

        let stats = Self::compute_stats(
            &tasks,
            self.config.small_file_threshold_bytes,
            self.config.min_delete_file_count_threshold,
            undersized_threshold_bytes,
        );

        let Some(planning_config) = self.config.resolve(&stats) else {
            return Ok(vec![]);
        };

        let strategy = PlanStrategy::from(&planning_config);
        let file_groups =
            FileSelector::group_tasks_with_strategy(tasks, strategy, &planning_config)?;

        let plans = file_groups
            .into_iter()
            .map(|fg| CompactionPlan::new(fg, to_branch.to_owned(), snapshot_id))
            .filter(|p| p.has_files())
            .collect();

        Ok(plans)
    }

    /// Computes statistics from pre-scanned tasks without additional IO.
    fn compute_stats(
        tasks: &[FileScanTask],
        small_file_threshold_bytes: u64,
        min_delete_file_count_threshold: usize,
        undersized_threshold_bytes: u64,
    ) -> SnapshotStats {
        let mut stats = SnapshotStats::default();

        for task in tasks {
            stats.total_data_files += 1;

            if task.length < small_file_threshold_bytes {
                stats.small_files_count += 1;
            }

            if min_delete_file_count_threshold > 0
                && task.deletes.len() >= min_delete_file_count_threshold
            {
                stats.delete_heavy_files_count += 1;
            }

            if task.length < undersized_threshold_bytes {
                stats.undersized_files_count += 1;
            }
        }

        stats
    }
}

/// Builder for [`AutoCompaction`].
pub struct AutoCompactionBuilder {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    auto_config: AutoCompactionConfig,

    catalog_name: Option<Cow<'static, str>>,
    executor_type: Option<ExecutorType>,
    registry: Option<BoxedRegistry>,
    commit_retry_config: Option<CommitManagerRetryConfig>,
    to_branch: Option<Cow<'static, str>>,
}

impl AutoCompactionBuilder {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        auto_config: AutoCompactionConfig,
    ) -> Self {
        Self {
            catalog,
            table_ident,
            auto_config,

            catalog_name: None,
            executor_type: None,
            registry: None,
            commit_retry_config: None,
            to_branch: None,
        }
    }

    pub fn with_executor_type(mut self, executor_type: ExecutorType) -> Self {
        self.executor_type = Some(executor_type);
        self
    }

    pub fn with_catalog_name(mut self, catalog_name: impl Into<Cow<'static, str>>) -> Self {
        self.catalog_name = Some(catalog_name.into());
        self
    }

    pub fn with_registry(mut self, registry: BoxedRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn with_retry_config(mut self, retry_config: CommitManagerRetryConfig) -> Self {
        self.commit_retry_config = Some(retry_config);
        self
    }

    pub fn with_to_branch(mut self, to_branch: impl Into<Cow<'static, str>>) -> Self {
        self.to_branch = Some(to_branch.into());
        self
    }

    pub fn build(self) -> AutoCompaction {
        let mut inner_builder = CompactionBuilder::new(self.catalog, self.table_ident);

        if let Some(name) = self.catalog_name {
            inner_builder = inner_builder.with_catalog_name(name);
        }
        if let Some(et) = self.executor_type {
            inner_builder = inner_builder.with_executor_type(et);
        }
        if let Some(reg) = self.registry {
            inner_builder = inner_builder.with_registry(reg);
        }
        if let Some(retry) = self.commit_retry_config {
            inner_builder = inner_builder.with_retry_config(retry);
        }
        if let Some(to_branch) = self.to_branch {
            inner_builder = inner_builder.with_to_branch(to_branch);
        }

        AutoCompaction {
            inner: inner_builder.build(),
            auto_config: self.auto_config,
        }
    }
}

/// Automatic compaction with runtime strategy selection.
///
/// Selects the appropriate compaction strategy (small files, files with deletes,
/// or full) based on snapshot statistics and executes the compaction workflow.
pub struct AutoCompaction {
    inner: Compaction,
    auto_config: AutoCompactionConfig,
}

impl AutoCompaction {
    /// Runs automatic compaction.
    ///
    /// Returns `None` if no strategy matches or no files need compaction.
    pub async fn compact(&self) -> Result<Option<CompactionResult>> {
        let overall_start_time = std::time::Instant::now();

        let table = self
            .inner
            .catalog
            .load_table(&self.inner.table_ident)
            .await?;

        let planner = AutoCompactionPlanner::new(self.auto_config.clone());
        let plans = planner
            .plan_compaction_with_branch(&table, &self.inner.to_branch)
            .await?;

        if plans.is_empty() {
            return Ok(None);
        }

        let rewrite_results = self
            .inner
            .concurrent_rewrite_plans(plans, &self.auto_config.execution, &table)
            .await?;

        if rewrite_results.is_empty() {
            return Ok(None);
        }

        let commit_start_time = std::time::Instant::now();
        let final_table = self
            .inner
            .commit_rewrite_results(rewrite_results.clone())
            .await?;

        if self.auto_config.execution.enable_validate_compaction {
            self.inner
                .run_validations(rewrite_results.clone(), &final_table)
                .await?;
        }

        self.inner
            .record_overall_metrics(&rewrite_results, overall_start_time, commit_start_time);

        let merged_result = self
            .inner
            .merge_rewrite_results_to_compaction_result(rewrite_results, Some(final_table));

        Ok(Some(merged_result))
    }
}
