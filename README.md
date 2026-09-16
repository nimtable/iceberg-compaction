# Compaction Runtime for Apache Iceberg™

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE)

`iceberg-compaction` is a Rust library for rewriting Apache Iceberg table data files using Apache DataFusion. Embed it in a service to select files, apply deletes, write replacement Parquet files, and commit the results through an Iceberg catalog.

**Disclaimer:** This project is not affiliated with or endorsed by the Apache Software Foundation. “Apache”, “Apache Iceberg”, and related marks are trademarks of the ASF.

## Capabilities

- **Selective or full compaction:** choose small files, files with many associated delete files, their union, or all eligible data files.
- **File grouping:** use a single group or bin-packing, within each partition or across the table, with optional minimum group size and file-count filters.
- **Delete handling:** apply equality and position deletes when rewriting data, including the Iceberg v3 deletion-vector read path.
- **Partitioning and sorting:** write to the table's current partition spec and sort order. Sort fields currently support identity transforms only.
- **Execution controls:** configure read/write parallelism, concurrent plans, target file size, Parquet writer properties, and an optional shared DataFusion memory budget with disk spilling.
- **Commit and visibility:** commit replacement files in one Iceberg transaction, target a branch, retry supported commit failures with exponential backoff, and collect file/byte statistics and metrics.

The core crate is [`iceberg-compaction-core`](./core/). Catalog and storage access use the pinned [RisingWave fork of `iceberg-rust`](https://github.com/risingwavelabs/iceberg-rust); the repository includes local-filesystem and REST-catalog/S3 examples. See [Cargo.toml](./Cargo.toml) for the exact dependency revisions and enabled storage features.

## Quick start

Install Rust with [rustup](https://rustup.rs/). Use the nightly toolchain pinned in [`rust-toolchain.toml`](./rust-toolchain.toml).

From the repository root, run the example that needs no external services:

```bash
cargo run -p iceberg-compaction-example
```

This [example](./examples/memory-catalog/src/main.rs) creates an in-memory catalog and an empty table in a temporary local warehouse. It exercises setup and the no-work result; it does not generate data to compact.

For a populated table in a REST catalog, configure the [REST example](./examples/rest-catalog/src/main.rs) before running it:

1. Add the catalog `uri` and any required warehouse and authentication properties to `iceberg_configs`.
2. Set the namespace and table name to an existing table.
3. Configure S3 access. The example enables environment-based credential loading by default; its alternative inline values are placeholders.

```bash
cargo run -p rest-catalog-example
```

## Using the library

The following function accepts an existing catalog and table identifier. It selects the `Auto` strategy explicitly, enables bin-packing, and sets a 512 MiB DataFusion execution budget:

```rust
use std::sync::Arc;

use iceberg_compaction_core::compaction::{CompactionBuilder, CompactionResult};
use iceberg_compaction_core::config::{
    AutoCompactionConfig, BinPackConfig, CompactionConfig, CompactionExecutionConfig,
    CompactionPlanningConfig, GroupingStrategy,
};
use iceberg_compaction_core::iceberg::{Catalog, TableIdent};
use iceberg_compaction_core::Result;

pub async fn compact_table(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
) -> Result<Option<CompactionResult>> {
    let planning = AutoCompactionConfig {
        grouping_strategy: GroupingStrategy::BinPack(BinPackConfig::default()),
        ..Default::default()
    };
    let execution = CompactionExecutionConfig {
        max_memory_bytes: Some(512 * 1024 * 1024),
        ..Default::default()
    };
    let config = CompactionConfig::new(CompactionPlanningConfig::Auto(planning), execution);

    CompactionBuilder::new(catalog, table_ident)
        .with_config(Arc::new(config))
        .build()
        .compact()
        .await
}
```

`compact()` returns `None` when no files are selected. On success, `CompactionResult` contains output data files, input/output file and byte statistics, and the updated table. Types from the pinned Iceberg dependency are available through the `iceberg_compaction_core::iceberg` re-export.

### Planning strategies

Set `CompactionConfig.planning` to one of these [`CompactionPlanningConfig`](./core/src/config/mod.rs) variants:

| Strategy | Candidate files |
| --- | --- |
| `Full` (default) | All data files eligible for the planning run. Group filters are not applied. |
| `SmallFiles` | Data files smaller than `small_file_threshold_bytes` (default: 32 MiB). |
| `FilesWithDeletes` | Data files with at least `min_delete_file_count_threshold` associated delete files (default: 128). Set it to `1` to select files with any deletes; `0` disables this filter. |
| `Auto` | The union of small files and files meeting the delete-file threshold, grouped together once. A zero threshold disables that predicate; setting both to zero selects nothing. |

The delete threshold counts **delete files associated with a data file**, not deleted rows or a deletion ratio. All strategies support an optional inclusive `max_file_sequence_number` bound to exclude newer files from a planning run.

Grouping is independent of candidate selection:

- `FileGroupScope::Partition` (default) groups each partition separately; `Table` groups the selected files across partitions. Output still follows the table's partition spec.
- `GroupingStrategy::Single` (default) makes one group per scope. `BinPack(BinPackConfig)` splits candidates into groups with a configurable target size (default: 100 GiB).
- `GroupFilters` can require a minimum group size or number of data files for `SmallFiles`, `FilesWithDeletes`, and `Auto`.

### Execution and resource settings

Configure these fields in [`CompactionExecutionConfig`](./core/src/config/mod.rs):

| Setting | Default | Purpose |
| --- | --- | --- |
| `target_file_size_bytes` | 1 GiB | Rolling output-file size target. The planning config has a separate field of the same name for parallelism estimates; set both when changing the intended file size. |
| `max_concurrent_compaction_plans` | 4 | Maximum concurrent rewrites in the managed `compact()` workflow. |
| `max_memory_bytes` | `None` | Optional DataFusion memory-pool budget. A positive value enables spilling for spill-capable operators. |
| `spill_dir` | `None` | Spill directory; defaults to the OS temporary directory when a memory budget is enabled. |
| `write_parquet_properties` | ZSTD; 128 MiB row groups | Parquet writer settings. |
| `enable_validate_compaction` | `false` | Additional result validation after the managed workflow commits. |
| `enable_prefetch` | `false` | Experimental whole-file prefetch; files carrying deletes bypass this path. |

Read/write parallelism is configured in the planning strategy (`max_input_parallelism`, `max_output_parallelism`, and `enable_heuristic_output_parallelism`).

A configured memory pool is shared by concurrent rewrites on the same `DataFusionExecutor`; its first budgeted request determines the pool's budget and spill directory. It covers DataFusion-accounted memory, not all process allocations. By default the execution pool is unbounded. The old `enable_dynamic_size_estimation` and `size_estimation_smoothing_factor` settings are deprecated no-ops.

### Workflow and observability

`compact()` manages planning, concurrent rewrites, and a single commit. For caller-controlled scheduling, use:

1. `plan_compaction()` to obtain plans.
2. `rewrite_plan(plan, execution_config, table)` to produce replacement files without committing.
3. `commit_rewrite_results(results)` to commit a batch from the same planning snapshot and branch.

In this workflow the caller controls concurrency; `max_concurrent_compaction_plans` only applies to `compact()`. See the [compaction API](./core/src/compaction/mod.rs) for details and `compact_with_plan()` for executing and committing one plan.

Use `CompactionBuilder::with_to_branch()` to target a branch (default: `main`), `with_retry_config()` to configure commit retries, and `with_registry()` to supply a `mixtrics` metrics registry. The default registry is a no-op. [Metrics](./core/src/common/metrics.rs) cover commit failures and duration, plan execution, input/output files and bytes, and DataFusion batch processing; `with_catalog_name()` supplies the catalog label.

## Maintenance boundaries

Compaction commits replacement data files and updates table metadata. It does **not** immediately delete the old data objects from storage: retained snapshots may still reference them. Snapshot expiration and orphan-file cleanup require separate maintenance through the catalog/Iceberg APIs or the embedding service.

This crate provides compaction primitives, not a persistent scheduler or resumable job service. Z-order clustering and standalone snapshot-expiration, manifest-rewrite, and orphan-cleanup APIs are not exposed by this crate. Table-sort-order rewriting is supported with the identity-transform limitation above.

## Benchmarking and development

The [benchmark tool](./integration-tests/src/bin/README.md) can generate mock data with equality and position deletes, compact a populated table, and report statistics. Configure the REST catalog and storage in a copy of [`mock_iceberg.yaml`](./integration-tests/testdata/mock_iceberg.yaml), then run from the repository root:

```bash
cargo run -p iceberg-compaction-integration-tests --bin bench -- bench /path/to/config.yaml
```

Other subcommands are `mock_table`, `bench_without_table`, and `delete_table`.

```bash
make check             # Rust formatting, Clippy, and TOML checks
make unit-test         # Library unit tests, excluding integration tests
make integration-test  # Requires Docker/Podman and Docker Compose
cargo doc -p iceberg-compaction-core --no-deps --open
```

The integration suite starts MinIO and an Iceberg REST catalog. See [CONTRIBUTING.md](./CONTRIBUTING.md) for setup and contribution guidance, and [STYLE.md](./STYLE.md) for documentation conventions.
