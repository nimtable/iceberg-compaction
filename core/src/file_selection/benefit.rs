/*
 * Copyright 2026 iceberg-compaction
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

//! Trustworthy per-file delete benefit for candidate planning.

use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, DataFileFormat};

use super::strategy::{DeleteFileCountFilterStrategy, FileFilterStrategy};
use crate::config::DeleteSelection;
use crate::{CompactionError, Result};

/// Selects files by the inclusive percentage of physical rows deleted.
#[derive(Debug, Clone, Copy)]
pub struct DeletedRowRatioFilterStrategy {
    min_percent: u32,
}

impl DeletedRowRatioFilterStrategy {
    /// Rejects percentages outside `0..=100`.
    pub fn new(min_percent: u32) -> Result<Self> {
        if min_percent > 100 {
            return Err(CompactionError::Config(format!(
                "deleted-row ratio percentage must be between 0 and 100; got {min_percent}"
            )));
        }
        Ok(Self { min_percent })
    }
}

impl FileFilterStrategy for DeletedRowRatioFilterStrategy {
    /// Evaluates delete benefit, failing when the selected metric cannot be determined.
    ///
    /// Ratio selection validates all eligible data files, including clean files.
    /// A zero ratio still requires at least one deleted row.
    fn matches(&self, task: &FileScanTask) -> Result<bool> {
        let unavailable = |reason: &str| CompactionError::DeleteStatistics {
            data_file_path: task.data_file_path.clone(),
            reason: reason.to_owned(),
        };
        if task.start != 0 || task.length != task.file_size_in_bytes {
            return Err(unavailable(
                "ratio selection requires a complete data-file task",
            ));
        }
        let records = task
            .record_count
            .ok_or_else(|| unavailable("missing physical record count"))?;
        let deleted = match task.deletes.as_slice() {
            [] => 0,
            [delete]
                if delete.file_type == DataContentType::PositionDeletes
                    && delete.file_format == DataFileFormat::Puffin
                    && delete.referenced_data_file.as_deref()
                        == Some(task.data_file_path.as_str())
                    && delete.content_offset.is_some_and(|offset| offset >= 0)
                    && delete.content_size_in_bytes.is_some_and(|size| size > 0) =>
            {
                delete
                    .record_count
                    .ok_or_else(|| unavailable("missing deletion-vector cardinality"))?
            }
            _ => {
                return Err(unavailable(
                    "expected one file-scoped Puffin deletion vector; shared, equality, Parquet or multiple deletes do not provide an exact deleted-row count",
                ));
            }
        };
        if deleted > records {
            return Err(unavailable(
                "deleted-row count exceeds physical record count",
            ));
        }
        Ok(deleted > 0
            && u128::from(deleted) * 100 >= u128::from(records) * u128::from(self.min_percent))
    }
}

impl std::fmt::Display for DeletedRowRatioFilterStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeletedRowRatio[>={}%]", self.min_percent)
    }
}

impl DeleteSelection {
    pub(crate) fn into_filter(self) -> Result<Box<dyn FileFilterStrategy>> {
        match self {
            Self::FileCount(count) => Ok(Box::new(DeleteFileCountFilterStrategy::new(count))),
            Self::DeletedRowRatioPercent(percent) => {
                Ok(Box::new(DeletedRowRatioFilterStrategy::new(percent)?))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::scan::FileScanTaskDeleteFile;
    use iceberg::spec::{Literal, Schema, Struct};

    use super::*;
    use crate::config::{
        AutoCompactionConfig, BinPackConfig, CompactionPlanningConfig, FileGroupScope,
        FilesWithDeletesConfig, FullCompactionConfig, GroupFilters, GroupingStrategy,
        SmallFilesConfig,
    };
    use crate::file_selection::strategy::{
        AnyFileFilter, GroupingStrategyEnum, MinGroupFileCountStrategy, PlanStrategyOptions,
        SingleGroupingStrategy, SizeFilterStrategy,
    };
    use crate::file_selection::{FileGroup, PlanStrategy};

    fn file(path: &str, bytes: u64, records: u64, deleted: Option<u64>) -> FileScanTask {
        let mut task = FileScanTask::builder()
            .with_data_file_path(path.to_owned())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_file_size_in_bytes(bytes)
            .with_start(0)
            .with_length(bytes)
            .with_record_count(Some(records))
            .with_schema(Arc::new(Schema::builder().build().unwrap()))
            .with_project_field_ids(vec![1, 2])
            .with_sequence_number(1)
            .with_file_sequence_number(Some(1))
            .with_case_sensitive(true)
            .build();
        if let Some(deleted) = deleted {
            task.deletes = vec![
                FileScanTaskDeleteFile::builder()
                    .with_file_path(format!("{path}.puffin"))
                    .with_file_size_in_bytes(100)
                    .with_file_type(DataContentType::PositionDeletes)
                    .with_partition_spec_id(0)
                    .with_file_format(DataFileFormat::Puffin)
                    .with_referenced_data_file(Some(path.to_owned()))
                    .with_content_offset(Some(10))
                    .with_content_size_in_bytes(Some(20))
                    .with_record_count(Some(deleted))
                    .with_sequence_number(10)
                    .build(),
            ];
        }
        task
    }

    fn auto() -> AutoCompactionConfig {
        AutoCompactionConfig {
            small_file_threshold_bytes: 32,
            delete_selection: Some(DeleteSelection::DeletedRowRatioPercent(20)),
            group_filters: Some(GroupFilters {
                min_group_file_count: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn groups(config: CompactionPlanningConfig, files: Vec<FileScanTask>) -> Vec<FileGroup> {
        PlanStrategy::try_from(&config)
            .unwrap()
            .execute(files, &config)
            .unwrap()
    }

    fn paths(groups: &[FileGroup]) -> Vec<&str> {
        let mut paths = groups
            .iter()
            .flat_map(|group| &group.data_files)
            .map(|file| file.data_file_path.as_str())
            .collect::<Vec<_>>();
        paths.sort_unstable();
        paths
    }

    #[test]
    fn ratio_is_inclusive_positive_and_overflow_safe() {
        for (records, deleted, percent, expected) in [
            (100, 19, 20, false),
            (100, 20, 20, true),
            (100, 100, 100, true),
            (100, 0, 0, false),
            (100, 1, 0, true),
            (0, 0, 0, false),
            (u64::MAX, u64::MAX / 2, 50, false),
            (u64::MAX, u64::MAX / 2 + 1, 50, true),
        ] {
            assert_eq!(
                DeletedRowRatioFilterStrategy::new(percent)
                    .unwrap()
                    .matches(&file("data", 10, records, Some(deleted)))
                    .unwrap(),
                expected
            );
        }
        assert!(
            !DeletedRowRatioFilterStrategy::new(0)
                .unwrap()
                .matches(&file("clean", 10, 100, None))
                .unwrap()
        );
    }

    #[test]
    fn ratio_rejects_untrustworthy_statistics_with_file_context() {
        let good = file("data", 10, 100, Some(20));
        let mut cases = Vec::new();
        let mut task = good.clone();
        task.record_count = None;
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].record_count = None;
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].record_count = Some(101);
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].file_type = DataContentType::EqualityDeletes;
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].file_format = DataFileFormat::Parquet;
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].referenced_data_file = None;
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].referenced_data_file = Some("other".to_owned());
        cases.push(task);
        let mut task = good.clone();
        task.deletes.push(task.deletes[0].clone());
        cases.push(task);
        let mut task = good.clone();
        let mut second = task.deletes[0].clone();
        second.content_offset = Some(40);
        task.deletes.push(second);
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].content_offset = Some(-1);
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].content_size_in_bytes = Some(0);
        cases.push(task);
        let mut task = good;
        task.length -= 1;
        cases.push(task);

        for task in cases {
            let error = DeletedRowRatioFilterStrategy::new(20)
                .unwrap()
                .matches(&task)
                .unwrap_err();
            assert!(matches!(
                error,
                CompactionError::DeleteStatistics { ref data_file_path, .. } if data_file_path == "data"
            ));
        }
    }

    #[test]
    fn auto_validates_ratio_even_for_small_or_non_candidate_files() {
        let config = CompactionPlanningConfig::Auto(auto());
        for bytes in [10, 100] {
            let mut task = file("missing", bytes, 100, None);
            task.record_count = None;
            let error = PlanStrategy::try_from(&config)
                .unwrap()
                .execute(vec![task], &config)
                .unwrap_err();
            assert_eq!(
                error.to_string(),
                "Delete statistics unavailable for missing: missing physical record count"
            );
        }
    }

    #[test]
    fn sequence_bound_precedes_ratio_but_never_filters_newer_deletes() {
        let config = CompactionPlanningConfig::Auto(AutoCompactionConfig {
            max_file_sequence_number: Some(3),
            ..auto()
        });
        let mut old = file("old", 100, 100, Some(20));
        old.file_sequence_number = Some(3);
        let mut new = file("new", 100, 100, None);
        new.file_sequence_number = Some(4);
        new.record_count = None;
        let result = groups(config, vec![old, new]);
        assert_eq!(paths(&result), vec!["old"]);
        assert_eq!(result[0].position_delete_files[0].sequence_number, 10);
    }

    #[test]
    fn auto_combines_merge_and_delete_plans_without_duplicates() {
        let result = groups(CompactionPlanningConfig::Auto(auto()), vec![
            file("small", 10, 100, None),
            file("small-heavy", 10, 100, Some(20)),
            file("large-heavy", 100, 100, Some(20)),
            file("healthy", 100, 100, None),
        ]);
        assert_eq!(result.len(), 2);
        assert_eq!(paths(&result), vec!["large-heavy", "small", "small-heavy"]);
        assert_eq!(paths(&result[..1]), vec!["small", "small-heavy"]);
        assert_eq!(paths(&result[1..]), vec!["large-heavy"]);
    }

    #[test]
    fn merge_gates_do_not_constrain_delete_candidates_and_are_inclusive() {
        let config = AutoCompactionConfig {
            group_filters: Some(GroupFilters {
                min_group_file_count: Some(3),
                min_group_size_bytes: Some(30),
            }),
            ..auto()
        };
        let result = groups(CompactionPlanningConfig::Auto(config.clone()), vec![
            file("small", 10, 100, None),
            file("heavy", 10, 100, Some(20)),
        ]);
        assert_eq!(result.len(), 1);
        assert_eq!(paths(&result), vec!["heavy"]);
        assert_eq!(result[0].data_file_count, 1);
        let result = groups(CompactionPlanningConfig::Auto(config.clone()), vec![file(
            "heavy",
            10,
            100,
            Some(20),
        )]);
        assert_eq!(paths(&result), vec!["heavy"]);
        let result = groups(CompactionPlanningConfig::Auto(config), vec![
            file("a", 10, 100, None),
            file("b", 10, 100, None),
            file("c", 10, 100, Some(20)),
        ]);
        assert_eq!(result.len(), 1);
        assert_eq!(paths(&result), vec!["a", "b", "c"]);

        for deleted in [None, Some(19), Some(20)] {
            let result = groups(CompactionPlanningConfig::Auto(auto()), vec![file(
                "singleton",
                10,
                100,
                deleted,
            )]);
            assert_eq!(
                paths(&result),
                if deleted == Some(20) {
                    vec!["singleton"]
                } else {
                    vec![]
                }
            );
        }
    }

    #[test]
    fn benefit_preserves_partition_and_binpack_boundaries() {
        let config = CompactionPlanningConfig::Auto(AutoCompactionConfig {
            grouping_strategy: GroupingStrategy::BinPack(BinPackConfig::new(20)),
            ..auto()
        });
        let mut separate = file("separate", 10, 100, None);
        separate.partition = Some(Struct::from_iter([Some(Literal::int(1))]));
        let result = groups(config, vec![
            file("a", 10, 100, None),
            file("b", 10, 100, None),
            file("heavy", 100, 100, Some(20)),
            separate,
        ]);
        let mut counts = result
            .iter()
            .map(|group| group.data_file_count)
            .collect::<Vec<_>>();
        counts.sort_unstable();
        assert_eq!(counts, vec![1, 2]);
        assert_eq!(paths(&result), vec!["a", "b", "heavy"]);

        let result = groups(
            CompactionPlanningConfig::Auto(AutoCompactionConfig {
                file_group_scope: FileGroupScope::Table,
                ..auto()
            }),
            vec![file("a", 10, 100, None), {
                let mut b = file("b", 10, 100, None);
                b.partition = Some(Struct::from_iter([Some(Literal::int(1))]));
                b
            }],
        );
        assert_eq!(result.len(), 1);
        assert_eq!(paths(&result), vec!["a", "b"]);
    }

    #[test]
    fn defaults_full_and_legacy_count_do_not_require_ratio_statistics() {
        let mut equality = file("equality", 10, 100, Some(20));
        equality.deletes[0].file_type = DataContentType::EqualityDeletes;
        equality.record_count = None;
        equality.deletes[0].record_count = None;
        let result = groups(
            CompactionPlanningConfig::FilesWithDeletes(FilesWithDeletesConfig {
                min_delete_file_count_threshold: 1,
                ..Default::default()
            }),
            vec![equality.clone()],
        );
        assert_eq!(paths(&result), vec!["equality"]);
        let result = groups(
            CompactionPlanningConfig::Full(FullCompactionConfig::default()),
            vec![equality],
        );
        assert_eq!(paths(&result), vec!["equality"]);
        let result = groups(
            CompactionPlanningConfig::SmallFiles(SmallFilesConfig::default()),
            vec![file("clean", 10, 100, None)],
        );
        assert_eq!(paths(&result), vec!["clean"]);
    }

    #[test]
    fn explicit_selection_overrides_count_for_auto_and_deletes() {
        let task = file("heavy", 100, 100, Some(20));
        let result = groups(
            CompactionPlanningConfig::FilesWithDeletes(FilesWithDeletesConfig {
                delete_selection: Some(DeleteSelection::DeletedRowRatioPercent(20)),
                ..Default::default()
            }),
            vec![task.clone()],
        );
        assert_eq!(paths(&result), vec!["heavy"]);
        let result = groups(
            CompactionPlanningConfig::Auto(AutoCompactionConfig {
                small_file_threshold_bytes: 0,
                min_delete_file_count_threshold: 0,
                ..auto()
            }),
            vec![task],
        );
        assert_eq!(paths(&result), vec!["heavy"]);
    }

    #[test]
    fn separate_file_scoped_blobs_can_share_one_puffin_file() {
        let mut a = file("a", 100, 100, Some(20));
        let mut b = file("b", 100, 100, Some(30));
        a.deletes[0].file_path = "shared.puffin".to_owned();
        b.deletes[0].file_path = "shared.puffin".to_owned();
        b.deletes[0].content_offset = Some(40);
        let result = groups(CompactionPlanningConfig::Auto(auto()), vec![a, b]);
        assert_eq!(paths(&result), vec!["a", "b"]);
        assert_eq!(result[0].position_delete_files.len(), 2);
        let mut ranges = result[0]
            .position_delete_files
            .iter()
            .map(|file| (file.start, file.length, file.record_count))
            .collect::<Vec<_>>();
        ranges.sort_unstable();
        assert_eq!(ranges, vec![(10, 20, Some(20)), (40, 20, Some(30))]);
    }

    #[test]
    fn merge_benefit_is_independent_of_output_parallelism() {
        let config = CompactionPlanningConfig::SmallFiles(SmallFilesConfig {
            small_file_threshold_bytes: 32,
            target_file_size_bytes: 10,
            group_filters: Some(GroupFilters {
                min_group_file_count: Some(2),
                ..Default::default()
            }),
            max_output_parallelism: 4,
            ..Default::default()
        });
        assert_eq!(
            paths(&groups(config.clone(), vec![file("a", 20, 100, None)])),
            Vec::<&str>::new()
        );
        let result = groups(config, vec![
            file("a", 20, 100, None),
            file("b", 20, 100, None),
        ]);
        assert_eq!(paths(&result), vec!["a", "b"]);
        assert_eq!(result[0].output_parallelism, 4);
    }

    #[test]
    fn large_delete_candidate_cannot_satisfy_small_file_merge_gates() {
        let config = CompactionPlanningConfig::Auto(AutoCompactionConfig {
            group_filters: Some(GroupFilters {
                min_group_file_count: Some(2),
                min_group_size_bytes: Some(20),
            }),
            ..auto()
        });
        let result = groups(config, vec![
            file("small", 10, 100, None),
            file("large-heavy", 100, 100, Some(20)),
        ]);
        assert_eq!(result.len(), 1);
        assert_eq!(paths(&result), vec!["large-heavy"]);
    }

    #[test]
    fn delete_candidates_use_binpack_instead_of_singleton_fallback() {
        let config = CompactionPlanningConfig::Auto(AutoCompactionConfig {
            grouping_strategy: GroupingStrategy::BinPack(BinPackConfig::new(20)),
            group_filters: Some(GroupFilters {
                min_group_file_count: Some(10),
                min_group_size_bytes: Some(1000),
            }),
            ..auto()
        });
        let result = groups(config, vec![
            file("a", 10, 100, Some(20)),
            file("b", 10, 100, Some(30)),
            file("c", 10, 100, Some(40)),
        ]);
        assert_eq!(paths(&result), vec!["a", "b", "c"]);
        let mut counts = result
            .iter()
            .map(|group| group.data_file_count)
            .collect::<Vec<_>>();
        counts.sort_unstable();
        assert_eq!(counts, vec![1, 2]);
    }

    #[test]
    fn filter_combinators_propagate_errors_and_short_circuit() {
        let mut missing = file("missing", 10, 100, None);
        missing.record_count = None;
        let ratio = DeletedRowRatioFilterStrategy::new(20).unwrap();
        let all = || SizeFilterStrategy {
            min_size: None,
            max_size: None,
        };

        for filter in [
            AnyFileFilter::new(all()).and(ratio),
            AnyFileFilter::match_none().or(ratio),
            AnyFileFilter::new(ratio).and(all()),
            AnyFileFilter::new(ratio).or(all()),
        ] {
            assert_eq!(
                filter.matches(&missing).unwrap_err().to_string(),
                "Delete statistics unavailable for missing: missing physical record count"
            );
        }
        assert!(
            !AnyFileFilter::match_none()
                .and(ratio)
                .matches(&missing)
                .unwrap()
        );
        assert!(
            AnyFileFilter::new(all())
                .or(ratio)
                .matches(&missing)
                .unwrap()
        );
    }

    #[test]
    fn invalid_ratio_is_rejected_when_constructing_the_filter_or_strategy() {
        for percent in [101, u32::MAX] {
            assert_eq!(
                DeletedRowRatioFilterStrategy::new(percent)
                    .unwrap_err()
                    .to_string(),
                format!(
                    "Invalid configuration: deleted-row ratio percentage must be between 0 and 100; got {percent}"
                )
            );
        }
        let configs = [
            CompactionPlanningConfig::Auto(AutoCompactionConfig {
                small_file_threshold_bytes: 0,
                delete_selection: Some(DeleteSelection::DeletedRowRatioPercent(101)),
                ..Default::default()
            }),
            CompactionPlanningConfig::FilesWithDeletes(FilesWithDeletesConfig {
                delete_selection: Some(DeleteSelection::DeletedRowRatioPercent(101)),
                ..Default::default()
            }),
        ];
        for config in configs {
            assert_eq!(
                PlanStrategy::try_from(&config).unwrap_err().to_string(),
                "Invalid configuration: deleted-row ratio percentage must be between 0 and 100; got 101"
            );
        }
    }

    #[test]
    fn custom_merge_and_delete_filters_share_common_eligibility() {
        let strategy = PlanStrategy::new_with_options(
            PlanStrategyOptions::new(
                vec![Box::new(SizeFilterStrategy {
                    min_size: None,
                    max_size: Some(500),
                })],
                GroupingStrategyEnum::Single(SingleGroupingStrategy),
                vec![Box::new(MinGroupFileCountStrategy { min_file_count: 2 })],
            )
            .with_merge_filters(vec![Box::new(SizeFilterStrategy {
                min_size: None,
                max_size: Some(32),
            })])
            .with_delete_filters(vec![Box::new(
                DeletedRowRatioFilterStrategy::new(20).unwrap(),
            )]),
        );
        let mut outside = file("outside", 1000, 100, None);
        outside.record_count = None;
        let result = strategy
            .execute(
                vec![
                    file("small", 10, 100, None),
                    file("both", 10, 100, Some(20)),
                    file("delete-only", 100, 100, Some(20)),
                    outside,
                ],
                &CompactionPlanningConfig::default(),
            )
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(paths(&result[..1]), vec!["both", "small"]);
        assert_eq!(paths(&result[1..]), vec!["delete-only"]);
    }
}
