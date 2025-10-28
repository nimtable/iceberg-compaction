use iceberg_compaction_integration_tests::test_utils::mock_iceberg_table;

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("Starting mock Iceberg table creation...");

    // Create the mock table
    mock_iceberg_table().await.unwrap();

    tracing::info!("Mock Iceberg table created successfully!");
}
