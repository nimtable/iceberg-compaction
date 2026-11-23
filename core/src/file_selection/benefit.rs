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

use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{DataContentType, DataFileFormat};

use super::strategy::{DeleteFileCountFilterStrategy, FileFilterDiagnostics, FileFilterStrategy};
use crate::config::DeleteSelection;
use crate::{CompactionError, Result};

/// Selects files by deleted-row percentage, skipping non-exact delete layouts.
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

/// Returns physical and deleted-row counts for a validated file-scoped position delete.
/// Returns `None` for other valid delete layouts.
/// Returns an error for invalid delete layouts, which are not eligible for selection.
fn position_delete_counts(
    task: &FileScanTask,
    delete: &FileScanTaskDeleteFile,
) -> Result<Option<(u64, u64)>> {
    let unavailable = |reason: &str| CompactionError::DeleteStatistics {
        data_file_path: task.data_file_path.clone(),
        reason: reason.to_owned(),
    };
    match delete.file_type {
        DataContentType::PositionDeletes => {}
        DataContentType::EqualityDeletes => return Ok(None),
        _ => return Err(unavailable("invalid delete content type or format")),
    }
    match delete.referenced_data_file.as_deref() {
        None if delete.file_format == DataFileFormat::Parquet => return Ok(None),
        Some(path) if path == task.data_file_path => {}
        _ => return Err(unavailable("invalid file-scoped position-delete reference")),
    }

    let records = task
        .record_count
        .ok_or_else(|| unavailable("missing physical record count"))?;
    let deleted = delete
        .record_count
        .ok_or_else(|| unavailable("missing position-delete record count"))?;
    if deleted > records {
        return Err(unavailable(
            "deleted-row count exceeds physical record count",
        ));
    }
    Ok(Some((records, deleted)))
}

impl FileFilterStrategy for DeletedRowRatioFilterStrategy {
    /// Uses one file-scoped Puffin or Parquet position delete for exact statistics.
    ///
    /// Other valid layouts are skipped.
    /// A zero ratio still requires at least one deleted row.
    fn matches(&self, task: &FileScanTask) -> Result<bool> {
        self.matches_with_diagnostics(task, &mut FileFilterDiagnostics::default())
    }

    fn matches_with_diagnostics(
        &self,
        task: &FileScanTask,
        diagnostics: &mut FileFilterDiagnostics,
    ) -> Result<bool> {
        match task.deletes.as_slice() {
            [] => return Ok(false),
            [delete] => {
                if let Some((records, deleted)) = position_delete_counts(task, delete)? {
                    return Ok(deleted > 0
                        && u128::from(deleted) * 100
                            >= u128::from(records) * u128::from(self.min_percent));
                }
            }
            _ => {}
        }
        diagnostics.warn(
            "Skipping ratio-based delete selection because the delete layout has no exact deleted-row ratio",
        );
        Ok(false)
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
            Self::FileCount(0) => Err(CompactionError::Config(
                "delete-file count must be greater than zero".to_owned(),
            )),
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

    use iceberg::spec::{DataFileFormat, Literal, Schema, Struct};

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

    fn parquet_file(path: &str, bytes: u64, records: u64, deleted: u64) -> FileScanTask {
        let mut task = file(path, bytes, records, Some(deleted));
        task.deletes[0].file_path = format!("{path}.pos.parquet");
        task.deletes[0].file_format = DataFileFormat::Parquet;
        task.deletes[0].content_offset = None;
        task.deletes[0].content_size_in_bytes = None;
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
            for task in [
                file("data", 10, records, Some(deleted)),
                parquet_file("data", 10, records, deleted),
            ] {
                assert_eq!(
                    DeletedRowRatioFilterStrategy::new(percent)
                        .unwrap()
                        .matches(&task)
                        .unwrap(),
                    expected
                );
            }
        }
        assert!(
            !DeletedRowRatioFilterStrategy::new(0)
                .unwrap()
                .matches(&file("clean", 10, 100, None))
                .unwrap()
        );
    }

    #[test]
    fn parquet_ratio_supports_auto_and_delete_plans_with_newer_deletes() {
        let configs = [
            CompactionPlanningConfig::Auto(AutoCompactionConfig {
                max_file_sequence_number: Some(1),
                ..auto()
            }),
            CompactionPlanningConfig::FilesWithDeletes(FilesWithDeletesConfig {
                delete_selection: Some(DeleteSelection::DeletedRowRatioPercent(20)),
                max_file_sequence_number: Some(1),
                ..Default::default()
            }),
        ];
        for config in configs {
            let mut newer = parquet_file("newer", 100, 100, 20);
            newer.file_sequence_number = Some(2);
            newer.record_count = None;
            let result = groups(config.clone(), vec![
                parquet_file("heavy", 100, 100, 20),
                parquet_file("below-ratio", 100, 100, 19),
                file("healthy", 100, 100, None),
                newer,
            ]);
            assert_eq!(paths(&result), vec!["heavy"]);
            assert_eq!(result[0].position_delete_files.len(), 1);
            let delete = &result[0].position_delete_files[0];
            assert_eq!(delete.data_file_format, DataFileFormat::Parquet);
            assert_eq!((delete.start, delete.length), (0, 100));
            assert_eq!(delete.record_count, Some(20));
            assert_eq!(delete.sequence_number, 10);
            assert_eq!(
                paths(&groups(config, vec![file("rewritten", 100, 100, None)])),
                Vec::<&str>::new()
            );
        }
    }

    #[test]
    fn parquet_ratio_rejects_invalid_file_scoped_statistics() {
        let good = parquet_file("data", 100, 100, 20);
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
        let mut task = good;
        task.deletes[0].referenced_data_file = Some("other".to_owned());
        cases.push(task);
        for task in cases {
            assert!(matches!(
                DeletedRowRatioFilterStrategy::new(20)
                    .unwrap()
                    .matches(&task)
                    .unwrap_err(),
                CompactionError::DeleteStatistics { ref data_file_path, .. } if data_file_path == "data"
            ));
            let mut multiple = task.clone();
            multiple
                .deletes
                .push(parquet_file("data", 100, 100, 20).deletes.remove(0));
            let mut mixed = task.clone();
            let mut equality = task.deletes[0].clone();
            equality.file_type = DataContentType::EqualityDeletes;
            mixed.deletes.push(equality);
            let mut mixed_reversed = mixed.clone();
            mixed_reversed.deletes.reverse();
            for task in [multiple, mixed, mixed_reversed] {
                assert!(
                    !DeletedRowRatioFilterStrategy::new(20)
                        .unwrap()
                        .matches(&task)
                        .unwrap()
                );
            }
        }
    }

    #[test]
    fn ratio_skips_non_exact_layouts_without_blocking_other_plans() {
        let good = file("legacy", 100, 100, Some(20));
        let mut equality = good.clone();
        equality.deletes[0].file_type = DataContentType::EqualityDeletes;
        equality.deletes[0].file_format = DataFileFormat::Parquet;
        equality.deletes[0].referenced_data_file = None;
        equality.deletes[0].record_count = None;
        equality.record_count = None;
        let mut position = equality.clone();
        position.deletes[0].file_type = DataContentType::PositionDeletes;
        position.record_count = Some(100);
        position.deletes[0].record_count = Some(100);
        let mut position_without_counts = position.clone();
        position_without_counts.record_count = None;
        position_without_counts.deletes[0].record_count = None;
        let mut multiple_unsupported = equality.clone();
        multiple_unsupported
            .deletes
            .push(position_without_counts.deletes[0].clone());
        let mut multiple = good.clone();
        let mut second = multiple.deletes[0].clone();
        second.content_offset = Some(40);
        multiple.deletes.push(second);
        let mut mixed = good.clone();
        mixed.deletes.push(equality.deletes[0].clone());
        let mut mixed_reversed = mixed.clone();
        mixed_reversed.deletes.reverse();
        let mut multiple_parquet = parquet_file("legacy", 100, 100, 20);
        let mut second = multiple_parquet.deletes[0].clone();
        second.file_path = "legacy.second.pos.parquet".to_owned();
        multiple_parquet.deletes.push(second);
        let mut mixed_position = good.clone();
        mixed_position
            .deletes
            .push(parquet_file("legacy", 100, 100, 20).deletes.remove(0));

        for legacy in [
            equality,
            position,
            position_without_counts,
            multiple_unsupported,
            multiple,
            mixed,
            mixed_reversed,
            multiple_parquet,
            mixed_position,
        ] {
            let result = groups(CompactionPlanningConfig::Auto(auto()), vec![
                file("a", 10, 100, None),
                file("b", 10, 100, None),
                file("below-ratio", 100, 100, Some(19)),
                file("healthy", 100, 100, None),
                legacy.clone(),
            ]);
            assert_eq!(result.len(), 1);
            assert_eq!(paths(&result), vec!["a", "b"]);

            let mut small = legacy.clone();
            small.file_size_in_bytes = 10;
            small.length = 10;
            let result = groups(CompactionPlanningConfig::Auto(auto()), vec![
                file("a", 10, 100, None),
                small.clone(),
            ]);
            assert_eq!(result.len(), 1);
            assert_eq!(paths(&result), vec!["a", "legacy"]);
            let result = groups(
                CompactionPlanningConfig::Auto(AutoCompactionConfig {
                    group_filters: Some(GroupFilters {
                        min_group_file_count: Some(10),
                        ..Default::default()
                    }),
                    ..auto()
                }),
                vec![file("a", 10, 100, None), small],
            );
            assert_eq!(paths(&result), Vec::<&str>::new());

            let config = CompactionPlanningConfig::FilesWithDeletes(FilesWithDeletesConfig {
                delete_selection: Some(DeleteSelection::DeletedRowRatioPercent(20)),
                ..Default::default()
            });
            assert_eq!(
                paths(&groups(config.clone(), vec![
                    legacy,
                    file("below-ratio", 100, 100, Some(19)),
                    file("heavy", 100, 100, Some(20)),
                    file("healthy", 100, 100, None),
                ])),
                vec!["heavy"]
            );
            assert_eq!(
                paths(&groups(config, vec![file("rewritten", 100, 100, None)])),
                Vec::<&str>::new()
            );
        }
    }

    #[test]
    fn explicit_zero_count_is_rejected_without_changing_legacy_zero_behavior() {
        for config in [
            CompactionPlanningConfig::Auto(AutoCompactionConfig {
                delete_selection: Some(DeleteSelection::FileCount(0)),
                ..Default::default()
            }),
            CompactionPlanningConfig::FilesWithDeletes(FilesWithDeletesConfig {
                delete_selection: Some(DeleteSelection::FileCount(0)),
                ..Default::default()
            }),
        ] {
            assert_eq!(
                PlanStrategy::try_from(&config).unwrap_err().to_string(),
                "Invalid configuration: delete-file count must be greater than zero"
            );
        }
        let result = groups(
            CompactionPlanningConfig::Auto(AutoCompactionConfig {
                small_file_threshold_bytes: 0,
                min_delete_file_count_threshold: 0,
                ..Default::default()
            }),
            vec![
                file("clean", 100, 100, None),
                file("heavy", 100, 100, Some(20)),
            ],
        );
        assert_eq!(paths(&result), Vec::<&str>::new());
        let result = groups(
            CompactionPlanningConfig::FilesWithDeletes(FilesWithDeletesConfig {
                min_delete_file_count_threshold: 0,
                ..Default::default()
            }),
            vec![file("clean", 100, 100, None)],
        );
        assert_eq!(paths(&result), vec!["clean"]);
    }

    #[test]
    fn explicit_positive_count_does_not_select_clean_or_rewritten_files() {
        for config in [
            CompactionPlanningConfig::Auto(AutoCompactionConfig {
                small_file_threshold_bytes: 0,
                min_delete_file_count_threshold: 0,
                delete_selection: Some(DeleteSelection::FileCount(1)),
                ..Default::default()
            }),
            CompactionPlanningConfig::FilesWithDeletes(FilesWithDeletesConfig {
                min_delete_file_count_threshold: 0,
                delete_selection: Some(DeleteSelection::FileCount(1)),
                ..Default::default()
            }),
        ] {
            assert_eq!(
                paths(&groups(config.clone(), vec![
                    file("clean", 100, 100, None),
                    file("heavy", 100, 100, Some(20)),
                ])),
                vec!["heavy"]
            );
            assert_eq!(
                paths(&groups(config, vec![file("rewritten", 100, 100, None)])),
                Vec::<&str>::new()
            );
        }
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
        task.deletes[0].file_type = DataContentType::Data;
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].referenced_data_file = None;
        cases.push(task);
        let mut task = good.clone();
        task.deletes[0].referenced_data_file = Some("other".to_owned());
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
            let mut mixed = task.clone();
            let mut legacy = task.deletes[0].clone();
            legacy.file_type = DataContentType::PositionDeletes;
            legacy.file_format = DataFileFormat::Parquet;
            mixed.deletes.push(legacy);
            let mut multiple = task.clone();
            multiple
                .deletes
                .push(file("data", 10, 100, Some(20)).deletes.remove(0));
            let mut mixed_reversed = mixed.clone();
            mixed_reversed.deletes.reverse();
            for task in [mixed, mixed_reversed, multiple] {
                assert!(
                    !DeletedRowRatioFilterStrategy::new(20)
                        .unwrap()
                        .matches(&task)
                        .unwrap()
                );
            }
        }
    }

    #[test]
    fn auto_validates_single_position_delete_counts_even_for_small_files() {
        let config = CompactionPlanningConfig::Auto(auto());
        for bytes in [10, 100] {
            let mut task = file("missing", bytes, 100, Some(20));
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
        let mut missing = file("missing", 10, 100, Some(20));
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
            for result in [
                filter.matches(&missing),
                filter.matches_with_diagnostics(&missing, &mut FileFilterDiagnostics::default()),
            ] {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "Delete statistics unavailable for missing: missing physical record count"
                );
            }
        }
        assert!(
            !AnyFileFilter::match_none()
                .and(ratio)
                .matches_with_diagnostics(&missing, &mut FileFilterDiagnostics::default())
                .unwrap()
        );
        assert!(
            AnyFileFilter::new(all())
                .or(ratio)
                .matches_with_diagnostics(&missing, &mut FileFilterDiagnostics::default())
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
