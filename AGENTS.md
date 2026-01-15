# Code Instructions

You are contributing to BergLoom, a Rust library for Apache Iceberg table compaction.

## Project Overview

- Language: Rust (nightly toolchain, see `rust-toolchain.toml`)
- Type: Library for Apache Iceberg table compaction
- Main crate: `iceberg-compaction-core` in `core/`

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

See `CONTRIBUTING.md` for build commands. Run `make check` before submitting changes.

## Documentation Style

Follow `STYLE.md` for documentation and doc tests.

## Meta-Principle

**DON'T BE YES MAN**

- Before implementing: "Does this already exist?" (`grep -r "pattern" core/src/`)
- Before adding logic: "Does this module need to know this?"
- After writing: "What breaks if I delete this?"

## Coding Principles

- Search before implement - grep existing code first
- Boring code - obvious > clever
- Single responsibility - one function, one job
- Explicit errors - use `CompactionError` with full context

## Testing Rules

- Follow Rust conventions - `#[test]`, `#[cfg(test)]` modules
- Cover critical paths - test actual logic, not trivial code
- Merge similar tests - no redundant coverage
- Precise assertions - assert exact values, not `!= 0` or `> 0`
- Test docs follow `STYLE.md` - same standards apply

**Bad test examples (avoid):**
- Assertions on non-behavioral signals: `assert!(result.is_ok())` without checking value
- Loose numeric checks: `assert!(count > 0)` when exact expected count is known
- Duplicate tests covering identical branches with different names

## Pre-Change Checklist

- [ ] Searched for similar functionality in `core/src/`.
- [ ] Read affected files completely.
- [ ] Verified correct module placement.
