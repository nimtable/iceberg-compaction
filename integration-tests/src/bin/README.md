# Bench binary usage

This binary (`bench`) lives under `integration-tests/src/bin/bench.rs`. It can:
- Create an Iceberg table from a YAML config and write mock data
- Run compaction on that table and print simple stats
- Delete the table

Default config path: `./testdata/mock_iceberg.yaml` (relative to the `integration-tests/` crate root). If you run commands from the repository root, pass the explicit path (see examples below).

---

## Run from the repository root

Use `-p` to target the `integration-tests` crate explicitly.

- Create table (generate mock data only; no compaction):

```bash
cargo run --bin bench -- mock_table  [config_path]
```

- Run compaction only (table must already exist):

```bash
cargo run --bin bench -- bench_without_table [config_path]
```

- Create data and then run compaction (one-shot flow):

```bash
cargo run --bin bench -- bench [config_path]
```

- Delete table:

```bash
cargo run --bin bench -- delete_table [config_path]
```

---

## Subcommands

- `mock_table [config_path]`: Create namespace/table and write data files (including equality/position deletes) using the YAML config.
- `bench_without_table [config_path]`: Run compaction only (expects an existing table with data).
- `bench [config_path]`: Create/write data, then run compaction (combines the two steps).
- `delete_table [config_path]`: Delete the table defined in the YAML (ignores "not found" errors).

`config_path` is optional; defaults to `./testdata/mock_iceberg.yaml` relative to the `integration-tests` crate.

---

## Notes

- Ensure the Rest Catalog endpoint, warehouse path, and S3 credentials in the YAML are valid and reachable.
