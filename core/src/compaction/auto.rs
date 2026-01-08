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

use std::borrow::Cow;
use std::sync::Arc;

use iceberg::{Catalog, TableIdent};
use mixtrics::metrics::BoxedRegistry;

use super::{
    CommitManagerRetryConfig, Compaction, CompactionBuilder, CompactionPlanner, CompactionResult,
};
use crate::Result;
use crate::config::{AutoCompactionConfig, CompactionPlanningConfig};
use crate::executor::ExecutorType;
use crate::file_selection::analyzer::SnapshotAnalyzer;

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

pub struct AutoCompaction {
    inner: Compaction,
    auto_config: AutoCompactionConfig,
}

impl AutoCompaction {
    /// Resolves which compaction strategy to use based on snapshot statistics.
    pub async fn resolve(&self) -> Result<Option<CompactionPlanningConfig>> {
        let table = self
            .inner
            .catalog
            .load_table(&self.inner.table_ident)
            .await?;

        let snapshot = match table.metadata().snapshot_for_ref(&self.inner.to_branch) {
            Some(s) => s,
            None => return Ok(None),
        };

        let stats = SnapshotAnalyzer::analyze(
            &table,
            snapshot.snapshot_id(),
            self.auto_config.small_file_threshold_bytes,
        )
        .await?;

        Ok(self.auto_config.resolve(&stats))
    }

    /// Runs auto compaction with the same execution semantics as [`Compaction::compact`].
    ///
    /// Unlike `Compaction` which uses a pre-configured planning strategy, this method
    /// auto-resolves the strategy by analyzing snapshot statistics at runtime.
    /// All other aspects (concurrent execution, single transaction commit, validation)
    /// remain identical to ensure consistent behavior.
    ///
    /// Returns `None` if no strategy matches or no files need compaction.
    pub async fn compact(&self) -> Result<Option<CompactionResult>> {
        let overall_start_time = std::time::Instant::now();

        let table = self
            .inner
            .catalog
            .load_table(&self.inner.table_ident)
            .await?;

        let snapshot = match table.metadata().snapshot_for_ref(&self.inner.to_branch) {
            Some(s) => s,
            None => return Ok(None),
        };

        // Analyze snapshot and resolve strategy based on current state
        let stats = SnapshotAnalyzer::analyze(
            &table,
            snapshot.snapshot_id(),
            self.auto_config.small_file_threshold_bytes,
        )
        .await?;

        let planning = match self.auto_config.resolve(&stats) {
            Some(p) => p,
            None => return Ok(None),
        };

        // Generate and execute compaction plans
        let planner = CompactionPlanner::new(planning);
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

        // Commit and validate
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
