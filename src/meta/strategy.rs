use std::sync::Arc;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{spec::DataFile, TableIdent};

use crate::CompactionError;

#[async_trait]
pub trait CompactionStrategy {
    async fn schedule(&self, table: &Table) -> Result<Vec<DataFile>, CompactionError>;
}

pub struct BinPackStrategy {
    pub target_file_size: u64,
    pub max_group_size: usize,
}

#[async_trait]
impl CompactionStrategy for BinPackStrategy {
    async fn schedule(&self, _table: &Table) -> Result<Vec<DataFile>, CompactionError> {
        unimplemented!("BinPackStrategy::schedule")
    }
}

pub struct FullCompactionStrategy;

#[async_trait]
impl CompactionStrategy for FullCompactionStrategy {
    async fn schedule(&self, table: &Table) -> Result<Vec<DataFile>, CompactionError> {
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let mut data_files = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            let (entries, _metadata) = manifest.into_parts();
            for entry in entries {
                data_files.push(entry.data_file().clone());
            }
        }

        Ok(data_files)
    }
}
