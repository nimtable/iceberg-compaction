# Copilot Instructions

You are contributing to BergLoom, a Rust library for Apache Iceberg table compaction.

## Project Overview

- **Language**: Rust (nightly toolchain, see `rust-toolchain.toml`)
- **Type**: Library for Apache Iceberg table compaction
- **Main crate**: `iceberg-compaction-core` in `core/`

## Project Structure

```
core/src/
├── compaction/    # Compaction logic (auto.rs, validator.rs)
├── config/        # Configuration types
├── error/         # Error types
├── executor/      # Execution engine (DataFusion integration)
├── file_selection/# File selection strategies (analyzer, packer, strategy)
└── common/        # Shared utilities (metrics)
```

## Build & Validation

See [CONTRIBUTING.md](../CONTRIBUTING.md) for build commands.

Always run `make check` before submitting changes.

## Documentation Style

See [STYLE.md](../STYLE.md) for the full documentation guide.
