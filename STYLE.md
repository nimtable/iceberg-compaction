# Documentation Style Guide

Guidelines for writing documentation in BergLoom. Intended for both human contributors and AI assistants.

## Principles

1. **Concise** — Describe *what* it does, not *how* it works internally
2. **Accurate** — Documentation must match the actual implementation
3. **Necessary** — Omit docs for self-explanatory items (builder setters, trivial getters)

## Module Docs

Every module should have a top-level doc comment:

```rust
//! Brief description of the module's responsibility.
//!
//! Optionally mention key types or usage patterns.
```

## Struct / Enum Docs

```rust
/// One-line summary of what this type represents.
///
/// Optional elaboration on semantics or invariants.
pub struct Foo { ... }
```

## Function / Method Docs

```rust
/// Brief description of what the function does.
///
/// Returns `None` if [condition].
pub fn foo(&self) -> Option<Bar> { ... }
```

**Avoid:**

- Repeating the function signature in prose
- Documenting obvious parameters
- Step-by-step implementation details

## Field Docs

Only document fields when the name alone is insufficient:

```rust
pub struct Config {
    /// Threshold in bytes; files smaller than this are considered "small".
    pub small_file_threshold_bytes: u64,

    // No doc needed — name is self-explanatory
    pub max_concurrent_tasks: usize,
}
```

## Anti-patterns

| ❌ Avoid | ✅ Prefer |
|----------|-----------|
| `/// Creates a new Foo with the given config.` | `/// Creates a new planner.` |
| `/// This function will ...` | `/// Does X.` |
| `// ============` section dividers | Blank lines |
| `/// # Example` with `ignore` | Omit unless the example compiles |
| Comments restating the code | Comment *why*, not *what* |

## References

- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [std doc conventions](https://std-dev-guide.rust-lang.org/documentation/doc-comments.html)
