use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CompactionConfig {
    pub batch_parallelism: Option<usize>,
}
