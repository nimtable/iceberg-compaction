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
- `delete-heavy`: `deletes.len() >= min_delete_file_count_threshold`
- `candidate set`: the set of data files that a strategy is allowed to include in compaction
- `group gating`: group-level thresholds used to avoid frequent small rewrites
- `plan budget`: the maximum number of plans that `Auto` is allowed to execute in a single run
- `fixed-point rewrite`: for the input files rewritten in the current run, the newly committed snapshot should cause them to leave that strategy's candidate set

## Strategy Model

### `Full`

- Intended use: explicit/manual full-table rewrite
- Candidate set: all data files
- Does not need to be fixed-point
- Is not used as an `Auto` fallback

### `SmallFiles`

- Intended use: append-only or general size-based compaction
- Candidate set: `file_size < small_file_threshold_bytes`
- May use `group_filters` for group gating
- Must be fixed-point: rewritten input files that reach the target threshold should leave the candidate set in the newly committed snapshot

### `FilesWithDeletes`

- Intended use: timely cleanup of delete-heavy files
- Candidate set: `deletes.len() >= min_delete_file_count_threshold`
- May use `group_filters` for group gating
- `Auto` does not rewrite or override caller-provided group gating for this strategy
- Must be fixed-point: rewritten delete-heavy input files should leave the candidate set in the newly committed snapshot

## `Auto` Planner

`Auto` only chooses between two localized rewrite strategies: `FilesWithDeletes` and `SmallFiles`.

Within a single scan, `Auto` produces two candidate plan sets:

1. `FilesWithDeletes` plan
2. `SmallFiles` plan

It then applies the following fixed decision order:

1. If the delete plan is non-empty, select `FilesWithDeletes`
2. Otherwise, if the small-files plan is non-empty, select `SmallFiles`
3. Apply `max_auto_plans_per_run` uniformly to the selected plan set
4. If the selected plan set becomes empty after capping, return an empty result

Design focus:

- Only choose localized rewrite strategies
- Apply a uniform budget to the final selected plan set

## Why `Auto` Does Not Fall Back to `Full`

Both `SmallFiles` and `FilesWithDeletes` have explicit candidate sets. After successful execution, those rewritten files usually leave the candidate set, so repeated high-frequency invocations tend to converge naturally.

`Full` does not have this property. Its candidate set is the entire table. If it were used as a normal `Auto` fallback, frequent invocations could repeatedly rewrite healthy parquet files that are already close to `target_file_size`.

For that reason, `Auto` does not introduce a full-like special case and does not use `Full` as a fallback path.

## Planner Budget

`max_auto_plans_per_run` is planner-level configuration, not external invocation policy. Its default is unlimited.

Rationale:

- The planner directly returns executable plans, so budget enforcement should happen inside the planner
- `planned_input_bytes`, `planned_input_files`, `rewrite_ratio`, and `reason` stay consistent with the final returned plan set
- It avoids forcing upper layers to recalculate report fields after trimming plans again

The current budget unit is `plan count`, not input bytes. This assumes grouping already keeps the size of each individual plan within a reasonable range.

## Return Semantics

### Executable Results

- `Recommended`: safe to execute by default
- `BudgetCapped`: a candidate strategy was selected, but only the subset of plans within the configured budget is returned

### Empty Results

- `NoCandidate`: no strategy threshold was met
- `NoPlansProduced`: a strategy threshold was met, but all plans were filtered out by group gating
- `NoSnapshot`: the target branch does not have an associated snapshot

### Report Path

The report result retains the following fields:

- `selected_strategy`
- `plans`
- `planned_input_bytes`
- `planned_input_files`
- `rewrite_ratio`
- `reason`

The `reason` in the report must match the final returned plan set. In other words, if the planner caps the plan set due to budget, `reason` must describe the capped result rather than the pre-cap candidate state.

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
- Choose between `FilesWithDeletes` and `SmallFiles`
- Apply `max_auto_plans_per_run`
- Return a report that is consistent with the final returned plan set

### Responsibilities of External Systems

- Decide when to call `Auto`
- Decide whether snapshot age or snapshot count should gate triggering
- Decide whether repeated calls against the same snapshot should be skipped
- Implement cooldown or other cross-invocation throttling policies

Here, "external systems" means callers or scheduling infrastructure outside the compaction library. The current library provides planning and execution, but does not provide a built-in scheduler.
