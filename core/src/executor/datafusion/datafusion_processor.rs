/*
 * Copyright 2025 IC
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

use std::sync::Arc;

use crate::error::{CompactionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::{
    arrow::schema_to_arrow_schema,
    io::FileIO,
    scan::FileScanTask,
    spec::{NestedField, PrimitiveType, Schema, Type},
};

use super::file_scan_task_table_provider::IcebergFileScanTaskTableProvider;

pub const SEQ_NUM: &str = "seq_num";
pub const FILE_PATH: &str = "file_path";
pub const POS: &str = "pos";
pub const DATA_FILE_TABLE: &str = "data_file_table";
pub const POSITION_DELETE_TABLE: &str = "position_delete_table";
pub const EQUALITY_DELETE_TABLE: &str = "equality_delete_table";

pub struct DatafusionProcessor {
    datafusion_task_ctx: DataFusionTaskContext,
    table_register: DatafusionTableRegister,
    batch_parallelism: usize,
    ctx: Arc<SessionContext>,
}

impl DatafusionProcessor {
    pub fn new(
        ctx: Arc<SessionContext>,
        datafusion_task_ctx: DataFusionTaskContext,
        batch_parallelism: usize,
        file_io: FileIO,
    ) -> Self {
        let table_register = DatafusionTableRegister::new(file_io, ctx.clone());
        Self {
            datafusion_task_ctx,
            table_register,
            batch_parallelism,
            ctx,
        }
    }

    pub fn register_tables(&mut self) -> Result<()> {
        if let Some(datafile_schema) = self.datafusion_task_ctx.data_file_schema.take() {
            self.table_register.register_data_table_provider(
                &datafile_schema,
                self.datafusion_task_ctx.data_files.take().unwrap(),
                DATA_FILE_TABLE,
                self.datafusion_task_ctx.need_seq_num(),
                self.datafusion_task_ctx.need_file_path_and_pos(),
                self.batch_parallelism,
            )?;
        }

        if let Some(position_delete_schema) = self.datafusion_task_ctx.position_delete_schema.take()
        {
            self.table_register.register_delete_table_provider(
                &position_delete_schema,
                self.datafusion_task_ctx
                    .position_delete_files
                    .take()
                    .unwrap(),
                POSITION_DELETE_TABLE,
                self.batch_parallelism,
            )?;
        }

        if let Some(equality_delete_schema) = self.datafusion_task_ctx.equality_delete_schema.take()
        {
            self.table_register.register_delete_table_provider(
                &equality_delete_schema,
                self.datafusion_task_ctx
                    .equality_delete_files
                    .take()
                    .unwrap(),
                EQUALITY_DELETE_TABLE,
                self.batch_parallelism,
            )?;
        }
        Ok(())
    }

    pub async fn execute(&mut self) -> Result<(DataFrame, Schema)> {
        self.register_tables()?;
        let df = self.ctx.sql(&self.datafusion_task_ctx.exec_sql).await?;
        Ok((df, self.datafusion_task_ctx.input_schema.take().unwrap()))
    }
}

pub struct DatafusionTableRegister {
    file_io: FileIO,
    ctx: Arc<SessionContext>,
}

impl DatafusionTableRegister {
    pub fn new(file_io: FileIO, ctx: Arc<SessionContext>) -> Self {
        DatafusionTableRegister { file_io, ctx }
    }

    pub fn register_data_table_provider(
        &self,
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Result<()> {
        self.register_table_provider_impl(
            schema,
            file_scan_tasks,
            table_name,
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        )
    }

    pub fn register_delete_table_provider(
        &self,
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        table_name: &str,
        batch_parallelism: usize,
    ) -> Result<()> {
        self.register_table_provider_impl(
            schema,
            file_scan_tasks,
            table_name,
            false,
            false,
            batch_parallelism,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn register_table_provider_impl(
        &self,
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Result<()> {
        let schema = schema_to_arrow_schema(schema)?;
        let data_file_table_provider = IcebergFileScanTaskTableProvider::new(
            file_scan_tasks,
            Arc::new(schema),
            self.file_io.clone(),
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        );

        self.ctx
            .register_table(table_name, Arc::new(data_file_table_provider))
            .unwrap();
        Ok(())
    }
}

/// SQL Builder for generating merge-on-read SQL queries
pub struct SqlBuilder {
    /// Column names to be projected in the query
    project_names: Vec<String>,

    /// Position delete table name
    position_delete_table_name: Option<String>,

    /// Equality delete table name
    equality_delete_table_name: Option<String>,

    /// Data file table name
    data_file_table_name: Option<String>,

    /// Flag indicating if file path and position columns are needed
    equality_join_names: Vec<String>,

    /// Flag indicating if position delete files are needed
    need_file_path_and_pos: bool,
}

impl SqlBuilder {
    /// Creates a new SQL Builder with the specified parameters
    pub fn new(
        project_names: Vec<String>,
        position_delete_table_name: Option<String>,
        equality_delete_table_name: Option<String>,
        data_file_table_name: Option<String>,
        equality_join_names: Vec<String>,
        need_file_path_and_pos: bool,
    ) -> Self {
        Self {
            project_names,
            position_delete_table_name,
            equality_delete_table_name,
            data_file_table_name,
            equality_join_names,
            need_file_path_and_pos,
        }
    }

    /// Builds a merge-on-read SQL query
    ///
    /// This method constructs a SQL query that:
    /// 1. Selects the specified columns from the data file table
    /// 2. Optionally joins with position delete files to exclude deleted rows
    /// 3. Optionally joins with equality delete files to exclude rows based on equality conditions
    pub fn build_merge_on_read_sql(self) -> Result<String> {
        let data_file_table_name = self.data_file_table_name.as_ref().ok_or_else(|| {
            CompactionError::Config("Data file table name is not provided".to_string())
        })?;
        // Start with a basic SELECT query from the data file table
        let mut sql = format!(
            "select {} from {}",
            self.project_names.join(","),
            data_file_table_name
        );

        // Add position delete join if needed
        // This excludes rows that have been deleted by position
        if self.need_file_path_and_pos {
            let position_delete_table_name =
                self.position_delete_table_name.as_ref().ok_or_else(|| {
                    CompactionError::Config(
                        "Position delete table name is not provided".to_string(),
                    )
                })?;
            sql.push_str(&format!(
                " left anti join {} on {}.{} = {}.{} and {}.{} = {}.{}",
                position_delete_table_name,
                data_file_table_name,
                FILE_PATH,
                position_delete_table_name,
                FILE_PATH,
                data_file_table_name,
                POS,
                position_delete_table_name,
                POS
            ));
        }

        // Add equality delete join if needed
        // This excludes rows that match the equality conditions in the delete files
        if !self.equality_join_names.is_empty() {
            let equality_delete_table_name =
                self.equality_delete_table_name.as_ref().ok_or_else(|| {
                    CompactionError::Config(
                        "Equality delete table name is not provided".to_string(),
                    )
                })?;
            sql.push_str(&format!(
                " left anti join {} on {}",
                equality_delete_table_name,
                self.equality_join_names
                    .iter()
                    .map(|name| format!(
                        "{}.{} = {}.{}",
                        data_file_table_name, name, equality_delete_table_name, name
                    ))
                    .collect::<Vec<_>>()
                    .join(" and ")
            ));

            // Add sequence number comparison if needed
            // This ensures that only newer deletes are applied
            sql.push_str(&format!(
                " and {}.{} < {}.{}",
                data_file_table_name, SEQ_NUM, equality_delete_table_name, SEQ_NUM
            ));
        }

        Ok(sql)
    }
}

pub struct DataFusionTaskContext {
    pub(crate) data_file_schema: Option<Schema>,
    pub(crate) input_schema: Option<Schema>,
    pub(crate) data_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_files: Option<Vec<FileScanTask>>,
    pub(crate) equality_delete_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_schema: Option<Schema>,
    pub(crate) equality_delete_schema: Option<Schema>,
    pub(crate) exec_sql: String,
}

pub struct DataFusionTaskContextBuilder {
    schema: Arc<Schema>,
    data_files: Vec<FileScanTask>,
    position_delete_files: Vec<FileScanTask>,
    equality_delete_files: Vec<FileScanTask>,
}

impl DataFusionTaskContextBuilder {
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = schema;
        self
    }

    pub fn with_datafile(mut self, data_files: Vec<FileScanTask>) -> Self {
        self.data_files = data_files;
        self
    }

    pub fn with_position_delete_files(mut self, position_delete_files: Vec<FileScanTask>) -> Self {
        self.position_delete_files = position_delete_files;
        self
    }

    pub fn with_equality_delete_files(mut self, equality_delete_files: Vec<FileScanTask>) -> Self {
        self.equality_delete_files = equality_delete_files;
        self
    }

    fn build_position_schema() -> Result<Schema> {
        let position_delete_schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::new(
                    1,
                    FILE_PATH,
                    Type::Primitive(PrimitiveType::String),
                    true,
                )),
                Arc::new(NestedField::new(
                    2,
                    POS,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )),
            ])
            .build()?;
        Ok(position_delete_schema)
    }

    // build data fusion task context
    pub fn build_merge_on_read(self) -> Result<DataFusionTaskContext> {
        let highest_field_id = self.schema.highest_field_id();
        // Build scheam for position delete file, file_path + pos
        let position_delete_schema = Self::build_position_schema()?;
        // Build schema for equality delete file, equality_ids + seq_num
        let mut equality_ids: Option<Vec<i32>> = None;
        for i in &self.equality_delete_files {
            if let Some(ids) = equality_ids.as_ref() {
                if ids.eq(&i.equality_ids) {
                    continue;
                } else {
                    return Err(CompactionError::Config("equality_ids not equal".to_owned()));
                }
            } else {
                equality_ids = Some(i.equality_ids.clone());
            }
        }
        let mut equality_delete_vec = vec![];
        if let Some(ids) = equality_ids.as_ref() {
            for i in ids {
                let field = self
                    .schema
                    .field_by_id(*i)
                    .ok_or_else(|| CompactionError::Config("equality_ids not found".to_owned()))?;
                equality_delete_vec.push(field.clone());
            }
        }
        let equality_join_names: Vec<_> =
            equality_delete_vec.iter().map(|i| i.name.clone()).collect();

        if !equality_delete_vec.is_empty() {
            equality_delete_vec.push(Arc::new(NestedField::new(
                highest_field_id + 1,
                SEQ_NUM,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        let equality_delete_schema = Schema::builder().with_fields(equality_delete_vec).build()?;
        let need_file_path_and_pos = !self.position_delete_files.is_empty();
        let need_seq_num = !equality_join_names.is_empty();

        // Build schema for data file, old schema + seq_num + file_path + pos
        let project_names: Vec<_> = self
            .schema
            .as_struct()
            .fields()
            .iter()
            .map(|i| i.name.clone())
            .collect();
        let highest_field_id = self.schema.highest_field_id();
        let mut add_schema_fields = vec![];
        // add sequence number column if needed
        if need_seq_num {
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 1,
                SEQ_NUM,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        // add file path and position column if needed
        if need_file_path_and_pos {
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 2,
                FILE_PATH,
                Type::Primitive(PrimitiveType::String),
                true,
            )));
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 3,
                POS,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        // data file schema is old schema + seq_num + file_path + pos. used for data file table provider
        let data_file_schema = self
            .schema
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(add_schema_fields)
            .build()?;
        // input schema is old schema. used for data file writer
        let input_schema = self.schema.as_ref().clone();

        let sql_builder = SqlBuilder::new(
            project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(EQUALITY_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            equality_join_names,
            need_file_path_and_pos,
        );
        let exec_sql = sql_builder.build_merge_on_read_sql()?;

        Ok(DataFusionTaskContext {
            data_file_schema: Some(data_file_schema),
            input_schema: Some(input_schema),
            data_files: Some(self.data_files),
            position_delete_files: Some(self.position_delete_files),
            equality_delete_files: Some(self.equality_delete_files),
            position_delete_schema: if need_file_path_and_pos {
                Some(position_delete_schema)
            } else {
                None
            },
            equality_delete_schema: if need_seq_num {
                Some(equality_delete_schema)
            } else {
                None
            },
            exec_sql,
        })
    }

    pub fn build_position_scan(self) -> Result<DataFusionTaskContext> {
        let position_delete_schema = Self::build_position_schema()?;
        let project_names: Vec<_> = position_delete_schema
            .as_struct()
            .fields()
            .iter()
            .map(|i| i.name.clone())
            .collect();
        Ok(DataFusionTaskContext {
            data_file_schema: None,
            input_schema: Some(position_delete_schema.clone()),
            data_files: None,
            position_delete_files: Some(self.position_delete_files),
            equality_delete_files: None,
            position_delete_schema: Some(position_delete_schema),
            equality_delete_schema: None,
            exec_sql: format!(
                "select {} from {}",
                project_names.join(","),
                POSITION_DELETE_TABLE
            ),
        })
    }
}

impl DataFusionTaskContext {
    pub fn builder() -> Result<DataFusionTaskContextBuilder> {
        Ok(DataFusionTaskContextBuilder {
            schema: Arc::new(Schema::builder().build()?),
            data_files: vec![],
            position_delete_files: vec![],
            equality_delete_files: vec![],
        })
    }

    pub fn need_file_path_and_pos(&self) -> bool {
        self.position_delete_files
            .as_ref()
            .is_some_and(|v| !v.is_empty())
    }

    pub fn need_seq_num(&self) -> bool {
        self.equality_delete_files
            .as_ref()
            .is_some_and(|v| !v.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test building SQL with no delete files
    #[test]
    fn test_build_merge_on_read_sql_no_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(
            project_names.clone(),
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(EQUALITY_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            equality_join_names,
            false,
        );
        assert_eq!(
            builder.build_merge_on_read_sql().unwrap(),
            format!(
                "select {} from {}",
                project_names.join(","),
                DATA_FILE_TABLE
            )
        );
    }

    /// Test building SQL with position delete files
    #[test]
    fn test_build_merge_on_read_sql_with_position_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(
            project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(EQUALITY_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            equality_join_names,
            true,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();
        assert!(sql.contains(&format!(
            "left anti join {} on {}",
            POSITION_DELETE_TABLE, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} on {}.file_path = {}.file_path and {}.pos = {}.pos",
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE
        )));
    }

    /// Test building SQL with equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_equality_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_join_names = vec!["id".to_owned()];

        let builder = SqlBuilder::new(
            project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(EQUALITY_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            equality_join_names,
            false,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();
        assert!(sql.contains(&format!(
            "left anti join {} on {}",
            EQUALITY_DELETE_TABLE, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} on {}.id = {}.id",
            EQUALITY_DELETE_TABLE, DATA_FILE_TABLE, EQUALITY_DELETE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{}.seq_num < {}.seq_num",
            DATA_FILE_TABLE, EQUALITY_DELETE_TABLE
        )));
    }

    /// Test building SQL with both position and equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_both_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_join_names = vec!["id".to_owned()];

        let builder = SqlBuilder::new(
            project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(EQUALITY_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            equality_join_names,
            true,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();
        assert!(sql.contains(&format!(
            "left anti join {} on {}",
            POSITION_DELETE_TABLE, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "left anti join {} on {}",
            EQUALITY_DELETE_TABLE, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} on {}.file_path = {}.file_path and {}.pos = {}.pos",
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} on {}.id = {}.id",
            EQUALITY_DELETE_TABLE, DATA_FILE_TABLE, EQUALITY_DELETE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{}.seq_num < {}.seq_num",
            DATA_FILE_TABLE, EQUALITY_DELETE_TABLE
        )));
    }
}
