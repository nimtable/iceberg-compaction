use iceberg::table::Table;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::{mpsc, RwLock};

use iceberg::{spec::DataFile, table, Catalog, TableIdent};
use uuid::Uuid;

use crate::{CompactionConfig, CompactionError};

use super::{strategy::CompactionStrategy, CompactionStatus, CompactionTaskHandle};

pub enum SchedulerCommand {
    RunCompaction {
        state: Arc<CompactionState>,
        handle: CompactionTaskHandle,
    },
    CancelCompaction(Uuid),
}

pub struct CompactionScheduler<S: CompactionStrategy, C: Catalog> {
    strategy: S,
    config: CompactionConfig,

    auto_compaction_tables: RwLock<HashMap<TableIdent, Arc<CompactionState>>>,
    task_tx: mpsc::Sender<SchedulerCommand>,
    task_rx: mpsc::Receiver<SchedulerCommand>,

    catalog: Arc<C>,
}

pub struct CompactionState {
    table_ident: TableIdent,
    last_compaction_time: AtomicU64,
    ongoing_compaction_tasks: Vec<CompactionTaskHandle>,
    config: Arc<CompactionConfig>,
}

impl<S: CompactionStrategy, C: Catalog> CompactionScheduler<S, C> {
    pub fn new(strategy: S, config: CompactionConfig, catalog: C) -> Self {
        let (task_tx, task_rx) = mpsc::channel(100);

        let catalog = Arc::new(catalog);
        Self {
            strategy,
            config,
            auto_compaction_tables: RwLock::new(HashMap::new()),
            task_tx,
            task_rx,
            catalog,
        }
    }

    // loop scheduling compaction tasks
    pub async fn run_scheduler_loop(&mut self) {
        // periodically trigger compaction for auto compaction tables

        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                            _ = interval.tick() => {
            let auto_compaction_tables = self.auto_compaction_tables.read().await;

                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();

                        const DEFAULT_INTERVAL: u64 = 60;

                        for (table_ident, state) in auto_compaction_tables.iter() {
                            let last = state.last_compaction_time.load(Ordering::Relaxed);
                            let config = &state.config;

                            if now - last >= DEFAULT_INTERVAL {
                                if let Err(e) = self.schedule_compaction(state.clone()).await {
                                    // log::error!("Failed to schedule compaction: {}", e)
                                }
                            }
                        }
                            }

                            Some(cmd) = self.task_rx.recv() => {
                                if let Err(e) = handler::handle_command(self.catalog.clone(), cmd).await {
                                    // log::error!("Failed to handle command: {}", e)
                                }
                            }
                        }

            // interval.tick().await;
        }
    }

    async fn schedule_compaction(
        &self,
        state: Arc<CompactionState>,
    ) -> Result<(), CompactionError> {
        // if state.ongoing_compaction_tasks.len() >= self.config.max_concurrent_tasks {
        //     return Ok(());
        // }

        if state.ongoing_compaction_tasks.is_empty() {
            return Ok(());
        }

        let table = self
            .catalog
            .load_table(&state.table_ident)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        // list files
        let input_files = self.strategy.schedule(&table).await?;

        if input_files.is_empty() {
            return Ok(());
        }

        let task_id = self.generate_task_id();

        let handle = CompactionTaskHandle {
            task_id,
            status: CompactionStatus::Pending,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            input_files,
            table: Arc::new(table),
        };

        let cmd = SchedulerCommand::RunCompaction { state, handle };

        self.task_tx
            .send(cmd)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        Ok(())
    }

    pub fn generate_task_id(&self) -> Uuid {
        Uuid::new_v4()
    }
}

mod handler {

    use iceberg::{transaction::Transaction, Catalog};

    use crate::{
        executor::{CompactionExecutor, DataFusionExecutor},
        meta::coordinator::Coordinator,
    };

    use super::*;

    pub async fn handle_command(
        catalog: Arc<impl Catalog>,
        cmd: SchedulerCommand,
    ) -> Result<(), CompactionError> {
        match cmd {
            SchedulerCommand::RunCompaction { state, handle } => {
                // run compaction task
                let executor = DataFusionExecutor::new();
                let output = executor
                    .compact(&state.table_ident, handle.input_files, state.config.clone())
                    .await
                    .map_err(|e| CompactionError::Execution(e.to_string()))?;

                let tx = Transaction::new(&(*handle.table));
                let cordiantor = Coordinator {};
                cordiantor
                    .commit(tx, &(*catalog))
                    .await
                    .map_err(|e| CompactionError::Execution(e.to_string()))?;

                Ok(())
            }
            SchedulerCommand::CancelCompaction(_task_id) => {
                // cancel compaction task
                Ok(())
            }
        }
    }

    pub async fn dispatch_compaction_task(
        catalog: Arc<impl Catalog>,
        mut rx: mpsc::Receiver<SchedulerCommand>,
    ) {
        //TODO: handle shutdown signal

        // dispatch compaction task
        while let Some(cmd) = rx.recv().await {
            if let Err(e) = handle_command(catalog.clone(), cmd).await {
                // log::error!("Failed to handle command: {}", e)
            }
        }
    }
}
