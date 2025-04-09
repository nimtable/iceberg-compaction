use serde::Deserialize;
use serde_with::serde_as;

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct CompactionConfig {
    pub batch_parallelism: Option<usize>,
    pub target_partitions: Option<usize>,
}
