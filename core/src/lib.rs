#![feature(proc_macro_hygiene, stmt_expr_attributes)]
#![feature(coroutines)]

pub mod compaction;
pub mod config;
pub mod error;
pub mod executor;

pub use config::CompactionConfig;
pub use error::{CompactionError, Result};
pub use executor::CompactionExecutor;
