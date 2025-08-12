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

use std::{collections::HashMap, sync::Arc};

use crate::docker_compose::get_rest_catalog;
use crate::test_utils::generator::{FileGenerator, FileGeneratorConfig, WriterConfig};
use iceberg::{
    spec::{NestedField, PrimitiveType, Schema, Type},
    transaction::Transaction,
    Catalog, NamespaceIdent, TableCreation, TableIdent,
};

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
    let writer_config = WriterConfig::new(&table);
    let file_generator_config = FileGeneratorConfig::new()
        .with_data_file_num(5)
        .with_data_file_row_count(300)
        .with_equality_delete_row_count(0) // No delete files to avoid data deletion issues
        .with_position_delete_row_count(0);

    let mut file_generator = FileGenerator::new(
        file_generator_config,
        Arc::new(schema.clone()),
        writer_config,
    )
    .expect("Failed to create file generator");

    let commit_data_files = file_generator
        .generate()
        .await
        .expect("Failed to generate test data files");

    println!("commit_data_files len {}", commit_data_files.len());

    // Commit files to table
    let txn = Transaction::new(&table);
    let mut fast_append_action = txn
        .fast_append(None, None, vec![])
        .expect("Failed to create fast append action");

    fast_append_action
        .add_data_files(commit_data_files)
        .expect("Failed to add data files");

    let _table_with_data = fast_append_action
        .apply()
        .await
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
        iceberg_compaction_core::compaction::CompactionType::Full,
    )
    .with_config(Arc::new(config))
    .with_catalog_name("test_catalog".to_owned())
    .build();

    let compaction_result = compaction.compact().await;

    let response = compaction_result.expect("Full compaction SQL generation should succeed");

    // Verify SqlBuilder correctly handles keyword identifiers
    assert_eq!(
        3, response.stats.input_files_count,
        "Compaction should process input files"
    );
    assert_eq!(
        2, response.stats.output_files_count,
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
    let writer_config = WriterConfig::new(&table);
    let file_generator_config = FileGeneratorConfig::new()
        .with_data_file_num(5)
        .with_data_file_row_count(300);
    // Using default parameters - will generate delete files

    let mut file_generator = FileGenerator::new(
        file_generator_config,
        Arc::new(schema.clone()),
        writer_config,
    )
    .expect("Failed to create file generator");

    let commit_data_files = file_generator
        .generate()
        .await
        .expect("Failed to generate test data files");

    // Commit files to table
    let txn = Transaction::new(&table);
    let mut fast_append_action = txn
        .fast_append(None, None, vec![])
        .expect("Failed to create fast append action");

    fast_append_action
        .add_data_files(commit_data_files)
        .expect("Failed to add data files");

    let _table_with_data = fast_append_action
        .apply()
        .await
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
        iceberg_compaction_core::compaction::CompactionType::Full,
    )
    .with_config(Arc::new(config))
    .with_catalog_name("test_catalog_with_deletes".to_owned())
    .build();

    let compaction_result = compaction.compact().await;

    let response =
        compaction_result.expect("Full compaction with delete files SQL generation should succeed");

    // Verify SqlBuilder correctly handles keyword identifiers in merge-on-read scenarios
    assert_eq!(
        9, response.stats.input_files_count,
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
        3, response.stats.input_equality_delete_file_count,
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
