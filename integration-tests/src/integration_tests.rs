/*
 * Copyright 2025 iceberg-compaction
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Integration tests that require Docker containers

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_compaction_core::compaction::CompactionBuilder;
use iceberg_compaction_core::config::{
    BinPackConfig, CompactionConfigBuilder, CompactionExecutionConfigBuilder,
    CompactionPlanningConfig, GroupFiltersBuilder, GroupingStrategy, SmallFilesConfigBuilder,
};

use crate::docker_compose::get_rest_catalog;
use crate::test_utils::generator::{FileGenerator, FileGeneratorConfig, WriterConfig};

const MB: u64 = 1024 * 1024;

#[tokio::test]
async fn test_sqlbuilder_fix_with_keyword_table_name() {
    // This test verifies that the SqlBuilder fix correctly handles SQL keyword table names
    // by creating a table with keyword names and ensuring basic operations work

    let catalog = get_rest_catalog().await;
    let catalog = Arc::new(catalog);

    // Create a schema with SQL keyword column names to test the fix
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::new(
                1,
                "select", // SQL keyword as column name
                Type::Primitive(PrimitiveType::Int),
                false,
            )),
            Arc::new(NestedField::new(
                2,
                "from", // SQL keyword as column name
                Type::Primitive(PrimitiveType::String),
                false,
            )),
            Arc::new(NestedField::new(
                3,
                "where", // SQL keyword as column name
                Type::Primitive(PrimitiveType::Double),
                false,
            )),
            Arc::new(NestedField::new(
                4,
                "order", // SQL keyword as column name
                Type::Primitive(PrimitiveType::Long),
                false,
            )),
        ])
        .build()
        .expect("Failed to create schema");

    // Use SQL keywords as table and namespace names to test the fix
    let keyword_namespace = "join"; // SQL keyword
    let keyword_table_name = "group"; // SQL keyword

    let namespace_ident = NamespaceIdent::new(keyword_namespace.to_owned());
    let table_ident = TableIdent::new(namespace_ident.clone(), keyword_table_name.to_owned());

    // Create namespace and table with keyword names
    catalog
        .create_namespace(&namespace_ident, HashMap::default())
        .await
        .expect("Failed to create namespace with keyword name");

    let table_creation = TableCreation::builder()
        .name(keyword_table_name.to_owned())
        .schema(schema.clone())
        .build();

    let table = catalog
        .create_table(&namespace_ident, table_creation)
        .await
        .expect("Failed to create table with keyword name");

    // Generate data files to test SQL keyword handling in compaction scenarios
    let writer_config = WriterConfig::new(&table, None);
    let file_generator_config = FileGeneratorConfig::new()
        .with_data_file_num(5)
        .with_data_file_row_count(300)
        .with_equality_delete_row_count(0) // No delete files to avoid data deletion issues
        .with_position_delete_row_count(0);

    let mut file_generator = FileGenerator::new(
        file_generator_config,
        Arc::new(schema.clone()),
        table.metadata().default_partition_spec().clone(),
        writer_config,
        vec![],
    )
    .expect("Failed to create file generator");

    let commit_data_files = file_generator
        .generate()
        .await
        .expect("Failed to generate test data files");

    println!("commit_data_files len {}", commit_data_files.len());

    // Commit files to table
    let txn = Transaction::new(&table);
    let fast_append_action = txn.fast_append().add_data_files(commit_data_files);

    let _table_with_data = fast_append_action
        .apply(txn)
        .expect("Failed to apply transaction")
        .commit(catalog.as_ref())
        .await
        .expect("Failed to commit transaction");

    // Test Full compaction to verify SqlBuilder handles SQL keywords correctly
    let config = iceberg_compaction_core::config::CompactionConfigBuilder::default()
        .build()
        .unwrap();

    let compaction = iceberg_compaction_core::compaction::CompactionBuilder::new(
        catalog.clone(),
        table_ident.clone(),
    )
    .with_config(Arc::new(config))
    .with_catalog_name("test_catalog".to_owned())
    .build();

    let compaction_result = compaction.compact().await.unwrap();

    let response = compaction_result.expect("Full compaction SQL generation should succeed");

    // Verify SqlBuilder correctly handles keyword identifiers
    assert_eq!(
        3, response.stats.input_files_count,
        "Compaction should process input files"
    );
    assert_eq!(
        1, response.stats.output_files_count,
        "Compaction should produce output files"
    );

    // SqlBuilder fix verification: All SQL keyword identifiers handled correctly
    // - Table name: 'group' (SQL keyword)
    // - Column names: 'select', 'from', 'where', 'order' (all SQL keywords)
    // - Full compaction SQL generation with proper identifier quoting

    // Clean up: try to drop the table and namespace
    let _ = catalog.drop_table(&table_ident).await;
    let _ = catalog.drop_namespace(&namespace_ident).await;
}

#[tokio::test]
async fn test_sqlbuilder_with_delete_files() {
    // This test verifies that the SqlBuilder fix correctly handles SQL keyword identifiers
    // in complex merge-on-read scenarios with delete files

    let catalog = get_rest_catalog().await;
    let catalog = Arc::new(catalog);

    // Create a schema with SQL keyword column names to test the fix
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::new(
                1,
                "select", // SQL keyword as column name
                Type::Primitive(PrimitiveType::Int),
                false,
            )),
            Arc::new(NestedField::new(
                2,
                "from", // SQL keyword as column name
                Type::Primitive(PrimitiveType::String),
                false,
            )),
            Arc::new(NestedField::new(
                3,
                "where", // SQL keyword as column name
                Type::Primitive(PrimitiveType::Double),
                false,
            )),
            Arc::new(NestedField::new(
                4,
                "order", // SQL keyword as column name
                Type::Primitive(PrimitiveType::Long),
                false,
            )),
        ])
        .build()
        .expect("Failed to create schema");

    // Use SQL keywords as table and namespace names to test the fix
    let keyword_namespace = "union"; // SQL keyword
    let keyword_table_name = "having"; // SQL keyword

    let namespace_ident = NamespaceIdent::new(keyword_namespace.to_owned());
    let table_ident = TableIdent::new(namespace_ident.clone(), keyword_table_name.to_owned());

    // Create namespace and table with keyword names
    catalog
        .create_namespace(&namespace_ident, HashMap::default())
        .await
        .expect("Failed to create namespace with keyword name");

    let table_creation = TableCreation::builder()
        .name(keyword_table_name.to_owned())
        .schema(schema.clone())
        .build();

    let table = catalog
        .create_table(&namespace_ident, table_creation)
        .await
        .expect("Failed to create table with keyword name");

    // Generate data files with delete files using default parameters
    let writer_config = WriterConfig::new(&table, None);
    let file_generator_config = FileGeneratorConfig::new()
        .with_data_file_num(5)
        .with_data_file_row_count(300);
    // Using default parameters - will generate delete files

    let mut file_generator = FileGenerator::new(
        file_generator_config,
        Arc::new(schema.clone()),
        table.metadata().default_partition_spec().clone(),
        writer_config,
        vec![],
    )
    .expect("Failed to create file generator");

    let commit_data_files = file_generator
        .generate()
        .await
        .expect("Failed to generate test data files");

    // Commit files to table
    let txn = Transaction::new(&table);
    let fast_append_action = txn.fast_append().add_data_files(commit_data_files);

    let _table_with_data = fast_append_action
        .apply(txn)
        .expect("Failed to apply transaction")
        .commit(catalog.as_ref())
        .await
        .expect("Failed to commit transaction");

    // Test Full compaction to verify SqlBuilder handles SQL keywords correctly with delete files
    let config = iceberg_compaction_core::config::CompactionConfigBuilder::default()
        .build()
        .unwrap();

    let compaction = iceberg_compaction_core::compaction::CompactionBuilder::new(
        catalog.clone(),
        table_ident.clone(),
    )
    .with_config(Arc::new(config))
    .with_catalog_name("test_catalog_with_deletes".to_owned())
    .build();

    let compaction_result = compaction.compact().await.unwrap();

    let response =
        compaction_result.expect("Full compaction with delete files SQL generation should succeed");

    // Verify SqlBuilder correctly handles keyword identifiers in merge-on-read scenarios
    assert_eq!(
        6, response.stats.input_files_count,
        "Compaction should process input files"
    );

    // Verify SqlBuilder correctly handles keyword identifiers in merge-on-read scenarios
    assert_eq!(
        1, response.stats.output_files_count,
        "Compaction should process output files"
    );

    // input_position_delete_file_count
    assert_eq!(
        3, response.stats.input_position_delete_file_count,
        "Compaction should process input position delete files"
    );

    // input_equality_delete_file_count
    assert_eq!(
        0, response.stats.input_equality_delete_file_count,
        "Compaction should process input equality delete files"
    );

    // SqlBuilder fix verification with delete files: All SQL keyword identifiers handled correctly
    // - Table name: 'having' (SQL keyword)
    // - Column names: 'select', 'from', 'where', 'order' (all SQL keywords)
    // - Full compaction SQL generation with delete files and proper identifier quoting

    // Clean up: try to drop the table and namespace
    let _ = catalog.drop_table(&table_ident).await;
    let _ = catalog.drop_namespace(&namespace_ident).await;
}

// #######################################
// Tests for Partitioned Tables
// #######################################

async fn setup_bucket_partitioned_table(
    catalog: Arc<iceberg_catalog_rest::RestCatalog>,
    namespace_name: &str,
    table_name: &str,
    bucket_number: usize,
) -> (Table, Schema) {
    // Create a schema with a "num" column which we will partition on
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::new(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
                false,
            )),
            Arc::new(NestedField::new(
                2,
                "num",
                Type::Primitive(PrimitiveType::Long),
                false,
            )),
        ])
        .build()
        .expect("Failed to create schema");

    let namespace_ident = NamespaceIdent::new(namespace_name.to_owned());

    catalog
        .create_namespace(&namespace_ident, HashMap::default())
        .await
        .expect("Failed to create namespace with keyword name");

    // Create a partition spec with a bucket transform. We expect the compaction to produce the
    // same number of files as the number of buckets.
    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(
            2,
            "num_bucket",
            iceberg::spec::Transform::Bucket(bucket_number as u32),
        )
        .expect("could not add partition field")
        .build();

    let partition_spec = unbound_partition_spec
        .bind(schema.clone())
        .expect("could not bind partition spec to test schema");

    let table_creation = TableCreation::builder()
        .name(table_name.to_owned())
        .schema(schema.clone())
        .partition_spec(partition_spec.clone())
        .build();

    let table = catalog
        .create_table(&namespace_ident, table_creation)
        .await
        .expect("Failed to create table with keyword name");

    (table, schema)
}

async fn write_data_to_table(
    catalog: Arc<iceberg_catalog_rest::RestCatalog>,
    table: &Table,
    schema: &Schema,
    data_files: usize,
    row_count: usize,
) {
    let writer_config = WriterConfig::new(table, None);
    let file_generator_config = FileGeneratorConfig::new()
        .with_data_file_num(data_files)
        .with_data_file_row_count(row_count)
        .with_equality_delete_row_count(0) // No delete files to avoid data deletion issues
        .with_position_delete_row_count(0);

    let mut file_generator = FileGenerator::new(
        file_generator_config,
        Arc::new(schema.clone()),
        table.metadata().default_partition_spec().clone(),
        writer_config,
        vec![],
    )
    .expect("Failed to create file generator");

    let commit_data_files = file_generator
        .generate()
        .await
        .expect("Failed to generate test data files");

    // Commit files to table
    let txn = Transaction::new(table);
    let fast_append_action = txn.fast_append().add_data_files(commit_data_files);

    let _table_with_data = fast_append_action
        .apply(txn)
        .expect("Failed to apply transaction")
        .commit(catalog.as_ref())
        .await
        .expect("Failed to commit transaction");
}

#[tokio::test]
async fn test_min_files_in_group_applies_to_partitioned_table() {
    // Issue 111: https://github.com/nimtable/iceberg-compaction/issues/111
    // This test verifies that the min_files_in_group config is applied to partitions, not the whole table.

    let catalog = get_rest_catalog().await;
    let catalog = Arc::new(catalog);

    let partition_bucket_n: usize = 5;
    let (table, schema) = setup_bucket_partitioned_table(
        catalog.clone(),
        "partition_namespace_01",
        "partition_table_01",
        partition_bucket_n,
    )
    .await;

    // Write enough rows that each partition will compact to 4 files apiece.
    write_data_to_table(catalog.clone(), &table, &schema, 10, 300).await;

    // Setup the SmallFiles compaction configuration with BinPacking. The key configuration is
    // to set a min_group_file_count to 2. The expectation is that a partition should have
    // at least 2 files in a group to be eligible for compaction.
    let small_files_config = SmallFilesConfigBuilder::default()
        .group_filters(
            GroupFiltersBuilder::default()
                .min_group_file_count(2_usize)
                .build()
                .expect("Failed to build group filters"),
        )
        .grouping_strategy(GroupingStrategy::BinPack(BinPackConfig::new(2 * MB)))
        .build()
        .expect("Failed to build small files config");

    let planning_config = CompactionPlanningConfig::SmallFiles(small_files_config);
    let config = CompactionConfigBuilder::default()
        .planning(planning_config)
        .build()
        .expect("Failed to build compaction config");
    let config = Arc::new(config);

    // Run compaction once to get our first compacted data.
    let compaction = CompactionBuilder::new(catalog.clone(), table.identifier().clone())
        .with_config(config.clone())
        .with_catalog_name("test_catalog".to_owned())
        .build();

    let compaction_result = compaction.compact().await.unwrap();

    let response = compaction_result.expect("Full compaction SQL generation should succeed");

    // Verify the results of the first compaction to make sure they match expectations.
    assert_eq!(
        30, response.stats.input_files_count,
        "Compaction input should match the expected number of files"
    );
    assert_eq!(
        partition_bucket_n, response.stats.output_files_count,
        "Compaction should produce the same number of output files as the number of partitioned buckets"
    );

    // Run compaction again to verify we DO NOT comapct the data again.
    let compaction = CompactionBuilder::new(catalog.clone(), table.identifier().clone())
        .with_config(config.clone())
        .with_catalog_name("test_catalog".to_owned())
        .build();

    let compaction_result = compaction.compact().await.unwrap();
    assert!(
        compaction_result.is_none(),
        "Compaction should NOT have re-run compaction because the files within each partition are less than the min_group_file_count; stats: {:?}",
        compaction_result.unwrap().stats
    );

    // Clean up: try to drop the table and namespace
    let _ = catalog.drop_table(table.identifier()).await;
    let _ = catalog.drop_namespace(table.identifier().namespace()).await;
}

#[tokio::test]
async fn test_rolling_file_compaction_in_partitioned_files_with_min_files_in_group() {
    // https://github.com/nimtable/iceberg-compaction/issues/111
    // This test verifies that writing multiple output files within a partition does not error/panic.

    let catalog = get_rest_catalog().await;
    let catalog = Arc::new(catalog);

    let partition_bucket_n: usize = 5;
    let (table, schema) = setup_bucket_partitioned_table(
        catalog.clone(),
        "partition_namespace_02",
        "partition_test_02",
        partition_bucket_n,
    )
    .await;

    // Write enough rows that each partition will compact to 4 files apiece.
    write_data_to_table(catalog.clone(), &table, &schema, 10, 10_000).await;

    // Setup the SmallFiles compaction configuration with BinPacking.
    let small_files_config = SmallFilesConfigBuilder::default()
        .group_filters(
            GroupFiltersBuilder::default()
                .min_group_file_count(5_usize)
                .build()
                .expect("Failed to build group filters"),
        )
        .grouping_strategy(GroupingStrategy::BinPack(BinPackConfig::new(2 * MB)))
        .build()
        .expect("Failed to build small files config");

    // The key configuration is setting target_file_size_bytes to a value small enough to
    // trigger rolling the file within each partition.
    let planning_config = CompactionPlanningConfig::SmallFiles(small_files_config);
    let config = CompactionConfigBuilder::default()
        .execution(
            CompactionExecutionConfigBuilder::default()
                .target_file_size_bytes(100_000_u64)
                .build()
                .expect("Failed to build execution config"),
        )
        .planning(planning_config)
        .build()
        .expect("Failed to build compaction config");
    let config = Arc::new(config);

    // Run compaction once to get our first compacted data.
    let compaction = CompactionBuilder::new(catalog.clone(), table.identifier().clone())
        .with_config(config.clone())
        .with_catalog_name("test_catalog".to_owned())
        .build();

    let compaction_result = compaction
        .compact()
        .await
        .expect("The compaction job to not result in an error")
        .expect("Full compaction SQL generation should succeed");

    assert_eq!(
        55, compaction_result.stats.input_files_count,
        "Compaction input should match the expected number of files"
    );

    assert_eq!(
        partition_bucket_n * 4,
        compaction_result.stats.output_files_count,
        "Compaction should produce 4 files per partition"
    );

    // Clean up: try to drop the table and namespace
    let _ = catalog.drop_table(table.identifier()).await;
    let _ = catalog.drop_namespace(table.identifier().namespace()).await;
}
