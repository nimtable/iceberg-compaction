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

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

use iceberg_compaction_core::iceberg::io::FileIOBuilder;
use iceberg_compaction_core::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg_compaction_core::iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_compaction_core::iceberg_catalog_memory::MemoryCatalog;

use iceberg_compaction_core::compaction::CompactionBuilder;
use iceberg_compaction_core::config::CompactionConfigBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Set up a temporary warehouse location
    let temp_dir = TempDir::new()?;
    let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();

    // 2. Create file I/O and memory catalog
    let file_io = FileIOBuilder::new_fs_io().build()?;
    let catalog = Arc::new(MemoryCatalog::new(file_io, Some(warehouse_location)));

    // 3. Create namespace and table
    let namespace_ident = NamespaceIdent::new("my_namespace".into());
    catalog
        .create_namespace(&namespace_ident, HashMap::new())
        .await?;

    let table_ident = TableIdent::new(namespace_ident, "sales_data".into());

    // Define table schema
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "customer_name", Type::Primitive(PrimitiveType::String))
                .into(),
            NestedField::required(3, "amount", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()?;

    catalog
        .create_table(
            &table_ident.namespace,
            TableCreation::builder()
                .name(table_ident.name().into())
                .schema(schema)
                .build(),
        )
        .await?;

    // 4. Configure compaction settings
    let compaction_config = CompactionConfigBuilder::default().build()?;
    let compaction = CompactionBuilder::new(catalog.clone(), table_ident.clone())
        .with_config(Arc::new(compaction_config))
        .with_catalog_name("memory_catalog".to_owned())
        .build();

    // 5. Perform the compaction
    println!("Starting compaction for table: {}", table_ident);
    let resp = compaction.compact().await?.unwrap();
    let stats = &resp.stats;

    // 6. Display compaction results
    println!("Compaction completed successfully!");
    println!("  - Input files: {}", stats.input_files_count);
    println!("  - Output files: {}", stats.output_files_count);
    println!("  - Input bytes: {}", stats.input_total_bytes);
    println!("  - Output bytes: {}", stats.output_total_bytes);

    Ok(())
}
