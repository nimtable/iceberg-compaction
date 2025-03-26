use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Execution failed: {0}")]
    Execution(String),

    #[error("Commit error: {0}")]
    Commit(String),
}

pub type Result<T> = std::result::Result<T, CompactionError>;
