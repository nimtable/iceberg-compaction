# Compaction Design

This document describes the current design boundaries of `Full`, `SmallFiles`, `FilesWithDeletes`, and `Auto`, as well as the responsibility split between `Auto` and external callers.

## Goals

1. Users should only need to call `Auto`, without manually selecting a compaction type.
2. `Auto` should make decisions based only on the current snapshot, without assuming the caller knows historical execution state.
3. `Auto` should prefer localized rewrites with explicit candidate sets.
4. `Auto` should not rewrite healthy files across the whole table by default.

## Terms

- `data file`: a `FileScanTask` where `data_file_content == Data`
- `delete-heavy`: matches the configured `delete_selection`, or the legacy
  `min_delete_file_count_threshold` when no explicit selection is set
- `candidate set`: the set of data files that a strategy is allowed to include in compaction
- `file group scope`: the boundary used before applying the grouping strategy; `Partition` keeps groups within one Iceberg partition, while `Table` lets the strategy group all selected files together
- `group gating`: thresholds applied to small-file merge groups, after `file group scope` and `grouping_strategy`; delete candidates do not inherit merge gates
- `fixed-point rewrite`: for the input files rewritten in the current run, the newly committed snapshot should cause them to leave that strategy's candidate set
- `sequence bound`: an optional inclusive `max_file_sequence_number` supplied by the caller; only files with `file_sequence_number <= bound` belong to that planning attempt

## Strategy Model

### `Full`

- Intended use: explicit/manual full-table rewrite
- Candidate set: all data files
- Default file group scope: `Partition`
- Use `FileGroupScope::Table` with `GroupingStrategy::Single` when the full-table rewrite must be planned as one file group
- Does not need to be fixed-point
- Is not used as an `Auto` fallback

### `SmallFiles`

- Intended use: append-only or general size-based compaction
- Candidate set: `file_size < small_file_threshold_bytes`
- Default file group scope: `Partition`
- Explicit `Table` scope is allowed for manual planning, but group gating then evaluates groups across all selected partitions instead of per partition
- May use `group_filters` for group gating
- Must be fixed-point: rewritten input files that reach the target threshold should leave the candidate set in the newly committed snapshot

### `FilesWithDeletes`

- Intended use: timely cleanup of delete-heavy files
- Candidate set: matches `delete_selection`, or the legacy delete-file count threshold
- Default file group scope: `Partition`
- Explicit `Table` scope is allowed for manual planning
- Uses the configured grouping strategy without small-file merge gates
- Must be fixed-point: rewritten delete-heavy input files should leave the candidate set in the newly committed snapshot

### Sequence-Bounded Planning

All four planning configurations (`Full`, `SmallFiles`, `FilesWithDeletes`, and
`Auto`) accept an optional `max_file_sequence_number`. When it is set:

- the planner validates that every scanned data file has a `file_sequence_number`;
  missing metadata is an explicit planning error
- the inclusive sequence filter runs before strategy filters, grouping, and group
  gating
- `Auto` applies the sequence bound before both merge and delete predicates

The planner does not retain round state. Callers that need a retryable compaction
round should keep the boundary and pass it unchanged on every planning attempt.

## `Auto`

`Auto` identifies two localized candidate sets from per-file predicates:

```text
merge_candidate(file) = is_small(file)
delete_candidate(file) = is_delete_heavy(file)
```

`small_file_threshold_bytes` and `min_delete_file_count_threshold` configure
the two predicates directly. Zero disables the corresponding predicate.

The predicates are evaluated from one scan. Both candidate sets use the same
file-group scope and grouping implementation:

1. Group small-file merge candidates and apply caller-provided `group_filters`
2. Remove files covered by accepted merges from the delete candidate set
3. Group the remaining delete candidates, without merge gates

Design focus:

- A file matching both predicates is rewritten once
- Small delete-heavy files may participate in accepted small-file merges
- A large delete-heavy file cannot make an otherwise insufficient small-file merge pass
- The default file group scope is `Partition`

### Custom File Predicates

All file predicates implement `FileFilterStrategy::matches() -> Result<bool>`.
Size, sequence, delete-count and deleted-row-ratio selection use this same
interface. `DeleteSelection` chooses which concrete delete filter is constructed.

AND/OR composition preserves short-circuiting and propagates errors from
evaluated predicates. The trait has no configuration-validation method.
`DeletedRowRatioFilterStrategy::new(percent)` rejects percentages outside
`0..=100`; its percentage field is private, so a constructed ratio filter always
has valid configuration. `matches()` only checks per-file statistics.

`PlanStrategy::try_from(&config)` constructs these filters and propagates an
invalid ratio before snapshot scanning. The `Auto` and `FilesWithDeletes`
strategy factories therefore return `Result<PlanStrategy>`. Other filters,
combinators and explicit strategy constructors remain infallible.

`PlanStrategyOptions` separates common eligibility filters from merge and delete
filters. Common filters apply before both paths. `with_merge_filters` and
`with_delete_filters` each accept an AND-combined list, using the same predicate
executor. An empty merge-filter list selects all eligible files; an empty
delete-filter list disables delete planning. Use `AnyFileFilter::match_none()` to
disable merge planning explicitly.

The merge and delete lists are evaluated independently. A successful merge
predicate never suppresses a delete-statistics error. Custom implementations of
the previous boolean `matches()` API must now return `Result<bool>`.

### Delete Benefit

`Auto` and `FilesWithDeletes` accept an optional `delete_selection`:

- `DeleteSelection::FileCount(n)` uses the applicable delete-descriptor count.
- `DeleteSelection::DeletedRowRatioPercent(p)` uses the inclusive percentage of
  physical data rows deleted. `p` must be in `0..=100`. Zero still requires a
  positive deleted-row count.

Explicit selection overrides the legacy count threshold, including Auto's
zero-disable behavior. Without it, the count-based candidate predicate is unchanged.

Ratio selection requires a complete data-file task with a physical record
count. A clean file has zero deleted rows. A file with deletes must have exactly
one file-scoped Puffin position-delete descriptor referencing that data file,
with a valid blob range and an exact cardinality not exceeding the physical
record count.

Shared position-delete files, equality deletes, Parquet position deletes and
multiple descriptors cannot establish an exact per-file deleted-row count.
Their record counts must not be added: positions may overlap, and equality or
shared deletes need not remove every row from this data file.

Unsupported layouts and missing statistics return
`CompactionError::DeleteStatistics`, not an empty plan. Statistics are checked
for every sequence-eligible file, even if it is small or not a candidate.
The data-file sequence bound never filters newer applicable deletes.
`Full` and count-based selection do not require these statistics.

### Merge Gates and Delete Benefit

`SmallFiles` and `Auto` use `GroupFilters` to express merge thresholds. Set
`min_group_file_count` to 2 to avoid healthy singleton rewrites. Without a count
gate, singleton small-file rewrites remain allowed. There is no hard-coded
minimum count or separate group-benefit policy.

Delete selection is an independent reason to rewrite a file. Delete candidates
do not need to meet merge count or size thresholds. They use the same grouping
implementation, including partition boundaries and bin packing, rather than a
singleton fallback. `FilesWithDeletes` therefore has no `group_filters` field.

If a small-file merge is rejected, its delete-heavy members remain eligible on
the delete path; healthy members remain unselected. This decision is independent
of estimated output-file count. `Full` is unchanged and has no merge gates.

For deletion-vector tables:

```rust
use iceberg_compaction_core::config::{
    AutoCompactionConfig, DeleteSelection, GroupFilters,
};

let planning = AutoCompactionConfig {
    delete_selection: Some(DeleteSelection::DeletedRowRatioPercent(20)),
    group_filters: Some(GroupFilters {
        min_group_file_count: Some(2),
        ..Default::default()
    }),
    ..Default::default()
};
```

## Why `Auto` Does Not Fall Back to `Full`

Both `SmallFiles` and `FilesWithDeletes` have explicit candidate sets. After successful execution, those rewritten files usually leave the candidate set, so repeated high-frequency invocations tend to converge naturally.

`Full` does not have this property. Its candidate set is the entire table. If it were used as a normal `Auto` fallback, frequent invocations could repeatedly rewrite healthy parquet files that are already close to `target_file_size`.

For that reason, `Auto` does not introduce a full-like special case and does not use `Full` as a fallback path.

## High-Frequency Invocation Boundaries

Selective paths try to converge naturally because successfully rewritten files
usually leave the candidate set.

The current design does not guarantee:

- cooldown
- deduplication of repeated invocations against the same snapshot
- "already executed within the last N minutes"
- throttling based on historical execution state

The reason is straightforward: those signals are outside the snapshot-local view of the current compaction planner.

## Responsibility Split Between the Library and External Systems

### Responsibilities of the Library

- Scan the current snapshot and produce candidate plans
- Combine merge and delete plans without rewriting overlapping candidates twice

### Responsibilities of External Systems

- Decide when to call `Auto`
- Bound how many returned plans are admitted for one scheduling run
- Decide whether snapshot age or snapshot count should gate triggering
- Decide whether repeated calls against the same snapshot should be skipped
- Implement cooldown or other cross-invocation throttling policies

Here, "external systems" means callers or scheduling infrastructure outside the compaction library. The current library provides planning and execution, but does not provide a built-in scheduler.
