# Compaction Design

This document describes the current design boundaries of `Full`, `SmallFiles`, `FilesWithDeletes`, and `Auto`, as well as the responsibility split between `Auto` and external callers.

## Goals

1. Users should only need to call `Auto`, without manually selecting a compaction type.
2. `Auto` should make decisions based only on the current snapshot, without assuming the caller knows historical execution state.
3. `Auto` should prefer localized rewrites with explicit candidate sets.
4. `Auto` should not rewrite healthy files across the whole table by default.
5. The amount of work performed by a single `Auto` run must be bounded.

## Terms

- `data file`: a `FileScanTask` where `data_file_content == Data`
- `delete-heavy`: when `min_delete_file_count_threshold > 0`, `deletes.len() >= min_delete_file_count_threshold`
- `candidate set`: the set of data files that a strategy is allowed to include in compaction
- `file group scope`: the boundary used before applying the grouping strategy; `Partition` keeps groups within one Iceberg partition, while `Table` lets the strategy group all selected files together
- `group gating`: group-level thresholds used to avoid frequent small rewrites; these thresholds are applied after `file group scope` and `grouping_strategy`
- `plan budget`: the maximum number of plans that `Auto` is allowed to execute in a single run
- `fixed-point rewrite`: for the input files rewritten in the current run, the newly committed snapshot should cause them to leave that strategy's candidate set

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
- Candidate set: `deletes.len() >= min_delete_file_count_threshold`
- Default file group scope: `Partition`
- Explicit `Table` scope is allowed for manual planning, but group gating then evaluates groups across all selected partitions instead of per partition
- May use `group_filters` for group gating
- Must be fixed-point: rewritten delete-heavy input files should leave the candidate set in the newly committed snapshot

## `Auto`

`Auto` builds one localized candidate set from two per-file predicates:

```text
candidate(file) = is_small(file) OR is_delete_heavy(file)
```

`small_file_threshold_bytes` and `min_delete_file_count_threshold` configure
the two predicates directly. Zero disables the corresponding predicate.

The predicates are composed before grouping. Auto then runs the unified
candidate set through exactly one planning pipeline:

1. File-level `small OR delete-heavy` selection
2. File-group scoping and grouping
3. Caller-provided group filters
4. Per-run plan budget

Design focus:

- A file matching both predicates enters the candidate set once
- Small and delete-heavy files may be compacted in the same group
- The default file group scope is `Partition`
- The plan budget applies to the final unified plan set

## Why `Auto` Does Not Fall Back to `Full`

Both `SmallFiles` and `FilesWithDeletes` have explicit candidate sets. After successful execution, those rewritten files usually leave the candidate set, so repeated high-frequency invocations tend to converge naturally.

`Full` does not have this property. Its candidate set is the entire table. If it were used as a normal `Auto` fallback, frequent invocations could repeatedly rewrite healthy parquet files that are already close to `target_file_size`.

For that reason, `Auto` does not introduce a full-like special case and does not use `Full` as a fallback path.

## Planner Budget

`planning.max_plans_per_run` is planner-level configuration, not external invocation policy. Its default is unlimited.
It is represented as a positive integer budget; zero is not a valid configuration value.

The planner directly returns executable plans, so budget enforcement happens
inside the planner rather than requiring callers to trim the result again.

The current budget unit is `plan count`, not input bytes. This assumes grouping already keeps the size of each individual plan within a reasonable range.

## High-Frequency Invocation Boundaries

The current design only guarantees two things:

1. Selective paths will try to converge naturally
2. The work done by a single `Auto` run is bounded by the planner budget

The current design does not guarantee:

- cooldown
- deduplication of repeated invocations against the same snapshot
- "already executed within the last N minutes"
- throttling based on historical execution state

The reason is straightforward: those signals are outside the snapshot-local view of the current compaction planner.

## Responsibility Split Between the Library and External Systems

### Responsibilities of the Library

- Scan the current snapshot and produce candidate plans
- Build one unified candidate set from the configured predicates
- Apply `planning.max_plans_per_run`

### Responsibilities of External Systems

- Decide when to call `Auto`
- Decide whether snapshot age or snapshot count should gate triggering
- Decide whether repeated calls against the same snapshot should be skipped
- Implement cooldown or other cross-invocation throttling policies

Here, "external systems" means callers or scheduling infrastructure outside the compaction library. The current library provides planning and execution, but does not provide a built-in scheduler.
