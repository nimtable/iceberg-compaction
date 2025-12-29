# Contributing to iceberg-compaction

Thank you for your interest in contributing to iceberg-compaction! This document provides everything you need to start contributing.

## Your First Contribution

1. [Fork the repository](https://github.com/nimtable/iceberg-compaction/fork) into your own GitHub account.
2. Create a new Git branch for your changes.
3. Make your changes and run `make check` to validate.
4. Submit your branch as a pull request to the main repository.

## Prerequisites

- **Rust**: Nightly toolchain (specified in `rust-toolchain`). Install via [rustup](https://rustup.rs/).
- **Docker or Podman**: Required for integration tests. For macOS, we recommend [OrbStack](https://orbstack.dev/) or [Podman](https://podman.io/) as alternatives to Docker Desktop.
- **Docker Compose**: Required for running test infrastructure

> **Tip**: Run `make setup` to automatically install the toolchain and required tools.

## Project Structure

```
iceberg-compaction/
├── core/                    # Core library (iceberg-compaction-core)
│   └── src/
│       ├── compaction/      # Compaction logic and validation
│       ├── config/          # Configuration management
│       ├── error/           # Error types
│       ├── executor/        # Execution engine (DataFusion integration)
│       ├── file_selection/  # File selection strategies
│       └── common/          # Shared utilities (metrics)
├── examples/
│   ├── memory-catalog/      # In-memory catalog example
│   └── rest-catalog/        # REST catalog example
├── integration-tests/       # Docker-based integration tests
│   └── testdata/            # Docker Compose configs
├── scripts/
│   ├── setup.sh             # Development environment setup
│   └── check.sh             # Pre-commit checks
```

## Development Setup

```bash
# Clone the repository
git clone https://github.com/nimtable/iceberg-compaction.git
cd iceberg-compaction

# Run setup script (installs toolchain and cargo-sort)
make setup

# Build the project
make build

# Run unit tests (excluding integration tests)
make unit-test

# Run a specific example
cargo run -p iceberg-compaction-example
```

> **Note**: The `rest-catalog-example` requires a running REST catalog and S3-compatible storage. See the example source code for configuration details.

## Code Style

### Rust Formatting

```bash
# Check formatting
cargo fmt --all -- --check

# Fix formatting
cargo fmt --all
```

### Clippy Lints

Key lints enforced (see `Cargo.toml` for full list):

- `dbg_macro = "warn"` - Avoid debug macros in production code
- `await_holding_lock = "warn"` - Prevent holding locks across await points
- `str_to_string = "warn"` - Prefer `to_owned()` over `to_string()` for `&str`
- `doc_markdown = "warn"` - Proper markdown in doc comments

```bash
# Run clippy (warnings as errors)
cargo clippy --workspace --all-targets -- -D warnings
```

### Cargo.toml Formatting

Use `cargo-sort` for consistent Cargo.toml ordering (config in `tomlfmt.toml`):

```bash
# Check sorting
cargo sort --check --workspace

# Fix sorting
cargo sort --workspace
```

### License Header

All source files should include the Apache 2.0 license header:

```rust
// Copyright 2025 iceberg-compaction
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
```

## Testing

### Unit Tests

```bash
# Run all unit tests in workspace (excluding integration tests)
make unit-test

# Or run core library unit tests only
cargo test -p iceberg-compaction-core --lib
```

### Integration Tests

Integration tests require Docker and Docker Compose. They spin up MinIO and Iceberg REST Catalog containers:

```bash
# Run integration tests
make integration-test
```

The tests use:
- **MinIO**: S3-compatible storage
- **Iceberg REST Catalog**: Apache Iceberg REST catalog service

### Full Check

Run all checks (format, lint, tests) before submitting a PR:

```bash
make check
```

## Pull Request Process

### Creating a Pull Request

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Run pre-commit checks: `make check`
5. Commit with clear messages
6. Push and create a Pull Request

### PR Title Convention

Pull request titles must follow the [Conventional Commits](https://www.conventionalcommits.org) format. This is required because we squash commits when merging.

**Format:** `<type>: <description>`

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `ci`: CI/CD changes
- `chore`: Other changes

**Examples:**
```
feat: add binpack compaction strategy
fix: correct file selection ordering
docs: update README with new examples
refactor: simplify executor error handling
test: add tests for deletion handling
```

### Reviews & Approvals

All pull requests should be reviewed by at least one maintainer. Please be patient — reviews may take a few days depending on availability.

### Merge Style

All pull requests are **squash merged**. We discourage large PRs over 300-500 lines of diff. For larger changes, please open an issue first to discuss the approach.

**Note:** When your PR is under review, avoid using `git push --force` as it makes tracking changes difficult. Use `git merge main` instead to keep your branch up to date.

## Code of Conduct

We expect all contributors to be respectful and constructive in discussions and code reviews.
