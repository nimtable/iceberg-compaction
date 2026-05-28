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

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
    PrimitiveLiteral, Struct, Transform, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::{TestEnv, append_and_commit, create_namespace, simple_table_schema};
use crate::compaction::CompactionPlanner;
use crate::config::{
    CompactionPlanningConfig, FileGroupScope, FullCompactionConfigBuilder, GroupingStrategy,
};

#[tokio::test]
async fn test_plan_compaction_with_table_file_group_scope() {
    let env = create_partitioned_test_env().await;
    let updated_table = append_and_commit(
        &env.table,
        env.catalog.as_ref(),
        partitioned_data_files(&env.table),
    )
    .await;

    let partition_scoped_plans = planner(FileGroupScope::Partition)
        .plan_compaction(&updated_table)
        .await
        .unwrap();
    let table_scoped_plans = planner(FileGroupScope::Table)
        .plan_compaction(&updated_table)
        .await
        .unwrap();

    assert_eq!(partition_scoped_plans.len(), 3);
    assert_eq!(table_scoped_plans.len(), 1);
    assert_eq!(table_scoped_plans[0].file_count(), 5);
    assert_eq!(table_scoped_plans[0].to_branch, MAIN_BRANCH);
}

fn planner(file_group_scope: FileGroupScope) -> CompactionPlanner {
    CompactionPlanner::new(CompactionPlanningConfig::Full(
        FullCompactionConfigBuilder::default()
            .grouping_strategy(GroupingStrategy::Single)
            .file_group_scope(file_group_scope)
            .build()
            .unwrap(),
    ))
}

async fn create_partitioned_test_env() -> TestEnv {
    let temp_dir = TempDir::new().unwrap();
    let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    warehouse_location.clone(),
                )]),
            )
            .await
            .unwrap(),
    );

    let namespace_ident = NamespaceIdent::new("test_partitioned_namespace".into());
    create_namespace(catalog.as_ref(), &namespace_ident).await;

    let table_ident = TableIdent::new(namespace_ident, "test_partitioned_table".into());
    create_partitioned_table(catalog.as_ref(), &table_ident).await;

    let table = catalog.load_table(&table_ident).await.unwrap();

    TestEnv {
        temp_dir,
        warehouse_location,
        catalog,
        table_ident,
        table,
    }
}

async fn create_partitioned_table<C: Catalog>(catalog: &C, table_ident: &TableIdent) {
    let partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id", Transform::Identity)
        .unwrap()
        .build();

    let _ = catalog
        .create_table(
            &table_ident.namespace,
            TableCreation::builder()
                .name(table_ident.name().into())
                .schema(simple_table_schema())
                .partition_spec(partition_spec)
                .build(),
        )
        .await
        .unwrap();
}

fn partitioned_data_files(table: &Table) -> Vec<DataFile> {
    let spec_id = table.metadata().default_partition_spec_id();
    let data_file = |path, partition_id| data_file_with_partition(path, partition_id, spec_id);

    vec![
        data_file("file:///test/p0_file1.parquet", 0),
        data_file("file:///test/p0_file2.parquet", 0),
        data_file("file:///test/p1_file1.parquet", 1),
        data_file("file:///test/p2_file1.parquet", 2),
        data_file("file:///test/p2_file2.parquet", 2),
    ]
}

fn data_file_with_partition(path: &str, partition_id: i32, partition_spec_id: i32) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_format(DataFileFormat::Parquet)
        .file_path(path.to_owned())
        .file_size_in_bytes(1024)
        .record_count(10)
        .partition(partition_value(partition_id))
        .partition_spec_id(partition_spec_id)
        .build()
        .unwrap()
}

fn partition_value(id: i32) -> Struct {
    Struct::from_iter(vec![Some(Literal::Primitive(PrimitiveLiteral::Int(id)))])
}
