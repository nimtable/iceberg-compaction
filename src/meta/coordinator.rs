use iceberg::{table::Table, transaction::Transaction, Catalog};

use crate::CompactionError;

pub struct Coordinator {}

impl<'a> Coordinator {
    pub async fn commit(
        &self,
        tx: Transaction<'a>,
        catalog: &impl Catalog,
    ) -> Result<Table, CompactionError> {
        tx.commit(catalog)
            .await
            .map_err(|e| CompactionError::Commit(e.to_string()))
    }
}
