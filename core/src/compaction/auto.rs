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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoSelectedStrategy {
    FilesWithDeletes,
    SmallFiles,
}

impl AutoSelectedStrategy {
    fn from_planning_config(config: &crate::config::CompactionPlanningConfig) -> Option<Self> {
        match config {
            crate::config::CompactionPlanningConfig::FilesWithDeletes(_) => {
                Some(AutoSelectedStrategy::FilesWithDeletes)
            }
            crate::config::CompactionPlanningConfig::SmallFiles(_) => {
                Some(AutoSelectedStrategy::SmallFiles)
            }
            crate::config::CompactionPlanningConfig::Full(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoPlanReason {
    Recommended,
    NoSnapshot,
    NoCandidate,
    NoPlansProduced,
    BudgetCapped,
}

#[derive(Debug, Clone)]
pub struct AutoPlanReport {
    pub selected_strategy: Option<AutoSelectedStrategy>,
    pub plans: Vec<CompactionPlan>,
    /// Total input bytes for selected plans: data + delete files.
    pub planned_input_bytes: u64,
    /// Total input file count for selected plans: data + delete files.
    pub planned_input_files: usize,
    /// `planned_data_bytes / total_data_bytes` (0 if total is 0).
    pub rewrite_ratio: f64,
    pub reason: AutoPlanReason,
}

impl AutoPlanReport {
    fn empty(reason: AutoPlanReason) -> Self {
        Self {
            selected_strategy: None,
            plans: vec![],
            planned_input_bytes: 0,
            planned_input_files: 0,
            rewrite_ratio: 0.0,
            reason,
        }
    }
}

fn compute_total_data_bytes(tasks: &[FileScanTask]) -> u64 {
    tasks.iter().map(|t| t.length).sum()
}

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
    /// Returns an empty vector when the planner does not return executable plans.
    ///
    /// Use [`plan_compaction_report_with_branch`](Self::plan_compaction_report_with_branch)
    /// when callers need to distinguish between `NoSnapshot`, `NoCandidate`,
    /// `NoPlansProduced`, and budget-capped empty results.
    pub async fn plan_compaction_with_branch(
        &self,
        table: &Table,
        to_branch: &str,
    ) -> Result<Vec<CompactionPlan>> {
        let report = self
            .plan_compaction_report_with_branch(table, to_branch)
            .await?;

        Ok(report.plans)
    }

    /// Plans compaction for a table branch and returns a report including cost and reason.
    pub async fn plan_compaction_report_with_branch(
        &self,
        table: &Table,
        to_branch: &str,
    ) -> Result<AutoPlanReport> {
        let Some(snapshot) = table.metadata().snapshot_for_ref(to_branch) else {
            return Ok(AutoPlanReport::empty(AutoPlanReason::NoSnapshot));
        };

        let snapshot_id = snapshot.snapshot_id();

        let mut tasks = Some(FileSelector::scan_data_files(table, snapshot_id).await?);
        let total_data_bytes = compute_total_data_bytes(tasks.as_ref().unwrap());
        let stats = Self::compute_stats(
            tasks.as_ref().unwrap(),
            self.config.small_file_threshold_bytes,
            self.config.min_delete_file_count_threshold,
        );

        let delete_candidate = self.config.files_with_deletes_candidate(&stats);
        let small_candidate = self.config.small_files_candidate(&stats);
        if delete_candidate.is_none() && small_candidate.is_none() {
            return Ok(AutoPlanReport::empty(AutoPlanReason::NoCandidate));
        }

        let delete_report = if let Some(planning_config) = delete_candidate {
            let delete_tasks = if small_candidate.is_some() {
                tasks.as_ref().unwrap().clone()
            } else {
                tasks.take().unwrap()
            };
            let report = Self::build_report(
                delete_tasks,
                planning_config,
                to_branch,
                snapshot_id,
                total_data_bytes,
                AutoPlanReason::Recommended,
            )?;
            if report.plans.is_empty() {
                Some(report)
            } else {
                return Ok(Self::cap_report_plans(
                    report,
                    total_data_bytes,
                    self.config.max_auto_plans_per_run,
                ));
            }
        } else {
            None
        };

        let small_report = if let Some(planning_config) = small_candidate {
            Some(Self::build_report(
                tasks.take().unwrap(),
                planning_config,
                to_branch,
                snapshot_id,
                total_data_bytes,
                AutoPlanReason::Recommended,
            )?)
        } else {
            None
        };

        Ok(Self::select_report(
            delete_report,
            small_report,
            total_data_bytes,
            self.config.max_auto_plans_per_run,
        ))
    }

    fn build_report(
        tasks: Vec<FileScanTask>,
        planning_config: crate::config::CompactionPlanningConfig,
        to_branch: &str,
        snapshot_id: i64,
        total_data_bytes: u64,
        reason: AutoPlanReason,
    ) -> Result<AutoPlanReport> {
        let selected_strategy = AutoSelectedStrategy::from_planning_config(&planning_config);
        let strategy = PlanStrategy::from(&planning_config);
        let file_groups =
            FileSelector::group_tasks_with_strategy(tasks, strategy, &planning_config)?;

        let plans: Vec<CompactionPlan> = file_groups
            .into_iter()
            .map(|fg| CompactionPlan::new(fg, to_branch.to_owned(), snapshot_id))
            .filter(|p| p.has_files())
            .collect();
        Ok(Self::report_from_plans(
            selected_strategy,
            plans,
            total_data_bytes,
            reason,
        ))
    }

    fn cap_report_plans(
        report: AutoPlanReport,
        total_data_bytes: u64,
        max_plans: usize,
    ) -> AutoPlanReport {
        if report.plans.is_empty() {
            return AutoPlanReport {
                reason: AutoPlanReason::NoPlansProduced,
                ..report
            };
        }

        if max_plans == 0 {
            return AutoPlanReport {
                selected_strategy: report.selected_strategy,
                plans: vec![],
                planned_input_bytes: 0,
                planned_input_files: 0,
                rewrite_ratio: 0.0,
                reason: AutoPlanReason::BudgetCapped,
            };
        }

        if report.plans.len() <= max_plans {
            return report;
        }

        let plans: Vec<CompactionPlan> = report.plans.into_iter().take(max_plans).collect();
        Self::report_from_plans(
            report.selected_strategy,
            plans,
            total_data_bytes,
            AutoPlanReason::BudgetCapped,
        )
    }

    fn select_report(
        delete_report: Option<AutoPlanReport>,
        small_report: Option<AutoPlanReport>,
        total_data_bytes: u64,
        max_plans: usize,
    ) -> AutoPlanReport {
        if let Some(report) = delete_report.filter(|report| !report.plans.is_empty()) {
            return Self::cap_report_plans(report, total_data_bytes, max_plans);
        }

        if let Some(report) = small_report.filter(|report| !report.plans.is_empty()) {
            return Self::cap_report_plans(report, total_data_bytes, max_plans);
        }

        AutoPlanReport::empty(AutoPlanReason::NoPlansProduced)
    }

    fn report_from_plans(
        selected_strategy: Option<AutoSelectedStrategy>,
        plans: Vec<CompactionPlan>,
        total_data_bytes: u64,
        reason: AutoPlanReason,
    ) -> AutoPlanReport {
        let planned_data_bytes = plans.iter().map(|p| p.file_group.total_size).sum::<u64>();
        let planned_input_bytes = plans.iter().map(CompactionPlan::total_bytes).sum();
        let planned_input_files = plans.iter().map(CompactionPlan::file_count).sum();
        let rewrite_ratio = if total_data_bytes == 0 {
            0.0
        } else {
            planned_data_bytes as f64 / total_data_bytes as f64
        };

        AutoPlanReport {
            selected_strategy,
            plans,
            planned_input_bytes,
            planned_input_files,
            rewrite_ratio,
            reason,
        }
    }

    /// Computes statistics from pre-scanned tasks without additional IO.
    fn compute_stats(
        tasks: &[FileScanTask],
        small_file_threshold_bytes: u64,
        min_delete_file_count_threshold: usize,
    ) -> SnapshotStats {
        let mut stats = SnapshotStats::default();

        for task in tasks {
            stats.total_data_files += 1;

            let is_small = task.length < small_file_threshold_bytes;
            if is_small {
                stats.small_files_count += 1;
            }

            let is_delete_heavy = min_delete_file_count_threshold > 0
                && task.deletes.len() >= min_delete_file_count_threshold;
            if is_delete_heavy {
                stats.delete_heavy_files_count += 1;
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

#[cfg(test)]
mod tests {
    use iceberg::spec::{DataContentType, DataFileFormat, Schema};

    use super::*;

    #[test]
    fn test_compute_total_data_bytes() {
        fn make_task(length: u64, path: &str) -> FileScanTask {
            FileScanTask {
                start: 0,
                length,
                record_count: Some(1),
                data_file_path: path.to_owned(),
                data_file_content: DataContentType::Data,
                data_file_format: DataFileFormat::Parquet,
                schema: std::sync::Arc::new(Schema::builder().build().unwrap()),
                project_field_ids: vec![],
                predicate: None,
                deletes: vec![],
                sequence_number: 1,
                equality_ids: None,
                file_size_in_bytes: length,
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: true,
            }
        }

        let tasks = vec![make_task(10, "a.parquet"), make_task(20, "b.parquet")];
        assert_eq!(compute_total_data_bytes(&tasks), 30);
    }

    fn make_task(length: u64, path: &str) -> FileScanTask {
        FileScanTask {
            start: 0,
            length,
            record_count: Some(1),
            data_file_path: path.to_owned(),
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: std::sync::Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: 1,
            equality_ids: None,
            file_size_in_bytes: length,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    fn make_plan(length: u64, path: &str) -> CompactionPlan {
        CompactionPlan::new(
            crate::file_selection::FileGroup::new(vec![make_task(length, path)]),
            "main",
            1,
        )
    }

    fn make_report(
        selected_strategy: AutoSelectedStrategy,
        lengths: &[u64],
        total_data_bytes: u64,
    ) -> AutoPlanReport {
        AutoCompactionPlanner::report_from_plans(
            Some(selected_strategy),
            lengths
                .iter()
                .enumerate()
                .map(|(idx, length)| make_plan(*length, &format!("{idx}.parquet")))
                .collect(),
            total_data_bytes,
            AutoPlanReason::Recommended,
        )
    }

    #[test]
    fn test_cap_report_plans_limits_plan_count() {
        let report = AutoPlanReport {
            selected_strategy: Some(AutoSelectedStrategy::FilesWithDeletes),
            plans: vec![
                make_plan(10, "a.parquet"),
                make_plan(20, "b.parquet"),
                make_plan(30, "c.parquet"),
            ],
            planned_input_bytes: 60,
            planned_input_files: 3,
            rewrite_ratio: 0.6,
            reason: AutoPlanReason::Recommended,
        };

        let capped = AutoCompactionPlanner::cap_report_plans(report, 100, 2);
        assert_eq!(capped.reason, AutoPlanReason::BudgetCapped);
        assert_eq!(capped.plans.len(), 2);
        assert_eq!(capped.planned_input_bytes, 30);
        assert_eq!(capped.planned_input_files, 2);
        assert_eq!(capped.rewrite_ratio, 0.3);
    }

    #[test]
    fn test_cap_report_plans_keeps_reason_when_within_budget() {
        let report = AutoPlanReport {
            selected_strategy: Some(AutoSelectedStrategy::FilesWithDeletes),
            plans: vec![make_plan(10, "a.parquet")],
            planned_input_bytes: 10,
            planned_input_files: 1,
            rewrite_ratio: 0.1,
            reason: AutoPlanReason::Recommended,
        };

        let capped = AutoCompactionPlanner::cap_report_plans(report, 100, 1);
        assert_eq!(capped.reason, AutoPlanReason::Recommended);
        assert_eq!(capped.plans.len(), 1);
        assert_eq!(capped.planned_input_bytes, 10);
        assert_eq!(capped.planned_input_files, 1);
    }

    #[test]
    fn test_cap_report_plans_returns_empty_when_budget_is_zero() {
        let report = make_report(AutoSelectedStrategy::FilesWithDeletes, &[10], 100);

        let capped = AutoCompactionPlanner::cap_report_plans(report, 100, 0);
        assert_eq!(capped.reason, AutoPlanReason::BudgetCapped);
        assert!(capped.plans.is_empty());
        assert_eq!(capped.planned_input_bytes, 0);
        assert_eq!(capped.planned_input_files, 0);
    }

    #[test]
    fn test_select_report_prefers_delete_plan() {
        let delete_report = Some(make_report(
            AutoSelectedStrategy::FilesWithDeletes,
            &[10],
            100,
        ));
        let small_report = Some(make_report(AutoSelectedStrategy::SmallFiles, &[20], 100));

        let selected = AutoCompactionPlanner::select_report(delete_report, small_report, 100, 10);
        assert_eq!(
            selected.selected_strategy,
            Some(AutoSelectedStrategy::FilesWithDeletes)
        );
        assert_eq!(selected.plans.len(), 1);
        assert_eq!(selected.reason, AutoPlanReason::Recommended);
    }

    #[test]
    fn test_select_report_falls_back_to_small_plan() {
        let delete_report = Some(AutoPlanReport::empty(AutoPlanReason::NoPlansProduced));
        let small_report = Some(make_report(AutoSelectedStrategy::SmallFiles, &[20], 100));

        let selected = AutoCompactionPlanner::select_report(delete_report, small_report, 100, 10);
        assert_eq!(
            selected.selected_strategy,
            Some(AutoSelectedStrategy::SmallFiles)
        );
        assert_eq!(selected.plans.len(), 1);
        assert_eq!(selected.reason, AutoPlanReason::Recommended);
    }

    #[test]
    fn test_select_report_returns_empty_when_all_candidates_are_empty() {
        let selected = AutoCompactionPlanner::select_report(
            Some(AutoPlanReport::empty(AutoPlanReason::NoPlansProduced)),
            Some(AutoPlanReport::empty(AutoPlanReason::NoPlansProduced)),
            100,
            10,
        );

        assert!(selected.plans.is_empty());
        assert_eq!(selected.selected_strategy, None);
        assert_eq!(selected.reason, AutoPlanReason::NoPlansProduced);
    }
}
