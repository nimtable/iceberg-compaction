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

use std::{collections::HashMap, sync::Arc};

use iceberg::{
    Catalog, NamespaceIdent, TableCreation,
    io::{S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY},
    spec::{NestedField, PrimitiveType, Schema, Type},
    transaction::Transaction,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_compaction_core::error::Result;
use serde::{Deserialize, Serialize};

use crate::test_utils::generator::{FileGenerator, FileGeneratorConfig, WriterConfig};

pub mod generator;

pub const DEFAULT_CONFIG_PATH: &str = "./testdata/mock_iceberg.yaml";

/// Mock Iceberg YAML configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockIcebergConfig {
    #[serde(rename = "RestCatalog")]
    pub rest_catalog: MockRestCatalogConfig,
    #[serde(rename = "Schemas")]
    pub schemas: SchemasConfig,
    #[serde(rename = "WriterConfig")]
    pub writer_config: WriterConfigYaml,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemasConfig {
    #[serde(rename = "pk_indices", default)]
    pub pk_indices: Vec<usize>,
    #[serde(rename = "columns")]
    pub columns: Vec<SchemaFieldConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockRestCatalogConfig {
    #[serde(rename = "CatalogName")]
    pub catalog_name: String,
    #[serde(rename = "CatalogUri")]
    pub catalog_uri: String,
    #[serde(rename = "DatabaseName")]
    pub database_name: String,
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "WarehousePath")]
    pub warehouse_path: String,
    #[serde(rename = "S3Region")]
    pub s3_region: String,
    #[serde(rename = "S3AccessKey")]
    pub s3_access_key: String,
    #[serde(rename = "S3SecretKey")]
    pub s3_secret_key: String,
}

impl MockRestCatalogConfig {
    /// Create a `RestCatalog` from the configuration
    pub fn load_catalog(&self) -> RestCatalog {
        let mut props = HashMap::new();
        props.insert(S3_ACCESS_KEY_ID.to_owned(), self.s3_access_key.clone());
        props.insert(S3_SECRET_ACCESS_KEY.to_owned(), self.s3_secret_key.clone());
        props.insert(S3_REGION.to_owned(), self.s3_region.clone());

        let config = RestCatalogConfig::builder()
            .uri(self.catalog_uri.clone())
            .warehouse(self.warehouse_path.clone())
            .props(props)
            .build();

        RestCatalog::new(config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaFieldConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub length: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterConfigYaml {
    pub data_file_row_count: usize,
    pub equality_delete_row_count: usize,
    pub position_delete_row_count: usize,
    pub data_file_num: usize,
    pub batch_size: usize,
}

impl MockIcebergConfig {
    /// Load configuration from YAML file
    pub fn from_yaml_file(path: &str) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| iceberg_compaction_core::CompactionError::Execution(e.to_string()))?;
        let config: MockIcebergConfig = serde_yaml::from_str(&contents)
            .map_err(|e| iceberg_compaction_core::CompactionError::Execution(e.to_string()))?;
        Ok(config)
    }

    /// Get primary key field IDs based on `pk_indices`
    /// Returns a vector of field IDs (1-based) for fields that are primary keys
    pub fn get_pk_field_ids(&self) -> Vec<i32> {
        self.schemas
            .pk_indices
            .iter()
            .map(|&idx| (idx + 1) as i32) // Field IDs are 1-based
            .collect()
    }

    /// Convert schema field configs to Iceberg Schema
    pub fn build_schema(&self) -> Result<(Schema, Vec<Option<usize>>)> {
        let mut fields = Vec::new();
        let mut fields_length = Vec::new();

        for (idx, field_config) in self.schemas.columns.iter().enumerate() {
            let field_id = (idx + 1) as i32;
            let field_type = match field_config.field_type.to_lowercase().as_str() {
                "int" => Type::Primitive(PrimitiveType::Int),
                "bigint" | "long" => Type::Primitive(PrimitiveType::Long),
                "varchar" | "string" => Type::Primitive(PrimitiveType::String),
                "double" => Type::Primitive(PrimitiveType::Double),
                "float" => Type::Primitive(PrimitiveType::Float),
                "boolean" | "bool" => Type::Primitive(PrimitiveType::Boolean),
                _ => {
                    return Err(iceberg_compaction_core::CompactionError::Execution(
                        format!("Unsupported field type: {}", field_config.field_type),
                    ));
                }
            };

            fields.push(Arc::new(NestedField::new(
                field_id,
                &field_config.name,
                field_type,
                false, // required field
            )));
            fields_length.push(field_config.length);
        }

        let schema = Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(|e| iceberg_compaction_core::CompactionError::Execution(e.to_string()))?;
        Ok((schema, fields_length))
    }
}

/// Create a mock Iceberg table based on YAML configuration
pub async fn mock_iceberg_table() -> Result<()> {
    // Load configuration
    let config = MockIcebergConfig::from_yaml_file(DEFAULT_CONFIG_PATH)?;
    let pk_indices = config.get_pk_field_ids();
    // Get catalog from config
    let catalog = config.rest_catalog.load_catalog();
    // Build schema from config
    let (schema, fields_length) = config.build_schema()?;
    // Create namespace
    let namespace_ident = NamespaceIdent::new(config.rest_catalog.database_name.clone());
    if !catalog
        .namespace_exists(&namespace_ident)
        .await
        .map_err(|e| iceberg_compaction_core::CompactionError::Config(e.to_string()))?
    {
        catalog
            .create_namespace(&namespace_ident, HashMap::default())
            .await
            .map_err(|e| iceberg_compaction_core::CompactionError::Config(e.to_string()))?;
    }

    // Create table
    let table_creation = TableCreation::builder()
        .name(config.rest_catalog.table_name.clone())
        .schema(schema.clone())
        .build();

    let table = catalog
        .create_table(&namespace_ident, table_creation)
        .await?;

    // Generate files
    let writer_config = WriterConfig::new(&table, Some(pk_indices));
    let file_generator_config = FileGeneratorConfig::new()
        .with_data_file_num(config.writer_config.data_file_num)
        .with_data_file_row_count(config.writer_config.data_file_row_count)
        .with_equality_delete_row_count(config.writer_config.equality_delete_row_count)
        .with_position_delete_row_count(config.writer_config.position_delete_row_count)
        .with_batch_size(config.writer_config.batch_size);

    let mut file_generator = FileGenerator::new(
        file_generator_config,
        Arc::new(schema),
        writer_config,
        fields_length,
    )?;

    let commit_data_files = file_generator.generate().await?;

    // Separate data files by type
    let mut data_files = Vec::new();
    let mut position_delete_files = Vec::new();
    let mut equality_delete_files = Vec::new();

    for data_file in commit_data_files {
        match data_file.content_type() {
            iceberg::spec::DataContentType::Data => data_files.push(data_file),
            iceberg::spec::DataContentType::PositionDeletes => {
                position_delete_files.push(data_file)
            }
            iceberg::spec::DataContentType::EqualityDeletes => {
                equality_delete_files.push(data_file)
            }
        }
    }

    // Commit data files and position deletes in one transaction
    let txn = Transaction::new(&table);
    let mut fast_append_action = txn.fast_append(Some(rand::random::<i64>()), None, vec![])?;
    fast_append_action
        .add_data_files(data_files)?
        .add_data_files(position_delete_files)?;
    let table = fast_append_action.apply().await?.commit(&catalog).await?;

    // Commit equality deletes in a separate transaction
    if !equality_delete_files.is_empty() {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let txn = Transaction::new(&table);
        let mut fast_append_action =
            txn.fast_append(Some(snapshot.snapshot_id() + 1), None, vec![])?;
        fast_append_action.add_data_files(equality_delete_files)?;
        fast_append_action.apply().await?.commit(&catalog).await?;
    }

    tracing::info!(
        "Successfully created table '{}' in namespace '{}' with {} data files",
        config.rest_catalog.table_name,
        config.rest_catalog.database_name,
        config.writer_config.data_file_num
    );

    Ok(())
}
