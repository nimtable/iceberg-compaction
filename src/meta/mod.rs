use std::sync::Arc;

use iceberg::{spec::DataFile, table::Table, TableIdent};
use uuid::Uuid;

pub mod coordinator;
pub mod scheduler;
pub mod strategy;

#[derive(Debug, Clone)]
pub struct CompactionTaskHandle {
    pub task_id: Uuid,
    pub status: CompactionStatus,
    pub created_at: u64,
    pub input_files: Vec<DataFile>,
    pub table: Arc<Table>,
}

#[derive(Debug, Clone)]
pub enum CompactionStatus {
    Pending,
    Running,
    Succeeded,
    Failed(String),
}

#[cfg(test)]
mod tests {}
