use std::sync::Arc;

use anyhow::Ok;
use iceberg_compaction_core::CompactionConfig;
use iceberg_compaction_core::compaction::CompactionBuilder;
use iceberg_compaction_core::config::{CompactionExecutionConfigBuilder, CompactionPlanningConfig};
use iceberg_compaction_integration_tests::test_utils::{
    delete_table_from_config, mock_iceberg_table,
};
use iceberg_compaction_integration_tests::{DEFAULT_CONFIG_PATH, MockIcebergConfig};
use tokio::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args: Vec<String> = std::env::args().collect();

    // Usage:
    // cargo run --bin bench -- mock_table [config_path]
    // cargo run --bin bench -- delete_table [config_path]
    // cargo run --bin bench -- bench [config_path]
    // cargo run --bin bench -- bench_without_table [config_path]

    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("run");
    let path = args.get(2).map(|s| s.as_str());
    let config =
        iceberg_compaction_integration_tests::test_utils::MockIcebergConfig::from_yaml_file(
            path.unwrap_or(DEFAULT_CONFIG_PATH),
        )?;

    match cmd {
        "delete_table" => {
            println!("Deleting table : {:?}", config);
            delete_table_from_config(&config).await?;
            println!("Deleted table (if existed)");
        }
        "bench_without_table" => {
            println!("Running benchmark without table from config: {:?}", config);
            benchmark_with_config(&config).await?;
        }
        "mock_table" => {
            tracing::info!("Running mock table creation from config: {:?}", config);
            mock_iceberg_table(&config).await?;
        }
        "bench" => {
            println!("Running benchmark with table from config: {:?}", config);
            mock_iceberg_table(&config).await?;
            benchmark_with_config(&config).await?;
        }
        _ => {
            println!("Running benchmark with table from config: {:?}", config);
            mock_iceberg_table(&config).await?;
            benchmark_with_config(&config).await?;
        }
    }

    Ok(())
}

async fn benchmark_with_config(config: &MockIcebergConfig) -> anyhow::Result<()> {
    let catalog = Arc::new(config.rest_catalog.load_catalog().await);
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(config.rest_catalog.database_name.clone()),
        config.rest_catalog.table_name.clone(),
    );

    let compaction_config = CompactionConfig::new(
        CompactionPlanningConfig::default(),
        CompactionExecutionConfigBuilder::default()
            .enable_validate_compaction(config.with_compaction_validations)
            .build()?,
    );
    let compaction = CompactionBuilder::new(catalog, table_ident.clone())
        .with_config(Arc::new(compaction_config))
        .with_catalog_name(config.rest_catalog.catalog_name.clone())
        .build();

    println!("Starting compaction for table: {}", table_ident);
    let timer = Instant::now();
    let resp = compaction.compact().await?.unwrap();
    let stats = &resp.stats;

    println!("Bench over!!!");
    println!("  - Input files: {}", stats.input_files_count);
    println!("  - Output files: {}", stats.output_files_count);
    println!("  - Input bytes: {}", stats.input_total_bytes);
    println!("  - Output bytes: {}", stats.output_total_bytes);
    println!("  - Time taken (ms): {}", timer.elapsed().as_millis());

    Ok(())
}
