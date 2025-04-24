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

use iceberg::scan::FileScanTask;

use super::{DATA_FILE_TABLE, EQUALITY_DELETE_TABLE, POSITION_DELETE_TABLE};

/// SQL Builder for generating merge-on-read SQL queries
pub struct SqlBuilder<'a> {
    /// Column names to be projected in the query
    project_names: &'a Vec<String>,
    /// Position delete files to be used in the query
    position_delete_files: &'a Vec<FileScanTask>,
    /// Equality delete files to be used in the query
    equality_delete_files: &'a Vec<FileScanTask>,

    /// Column names to be used for equality delete joins
    equality_join_names: &'a Vec<String>,

    /// Whether to include sequence number comparison in equality delete joins
    need_seq_num: bool,
    /// Whether to include file path and position in position delete joins
    need_file_path_and_pos: bool,
}

impl<'a> SqlBuilder<'a> {
    /// Creates a new SQL Builder with the specified parameters
    pub fn new(
        project_names: &'a Vec<String>,
        position_delete_files: &'a Vec<FileScanTask>,
        equality_delete_files: &'a Vec<FileScanTask>,
        equality_join_names: &'a Vec<String>,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
    ) -> Self {
        Self {
            project_names,
            position_delete_files,
            equality_delete_files,
            equality_join_names,
            need_seq_num,
            need_file_path_and_pos,
        }
    }

    /// Builds a merge-on-read SQL query
    ///
    /// This method constructs a SQL query that:
    /// 1. Selects the specified columns from the data file table
    /// 2. Optionally joins with position delete files to exclude deleted rows
    /// 3. Optionally joins with equality delete files to exclude rows based on equality conditions
    pub fn build_merge_on_read_sql(&self) -> String {
        // Start with a basic SELECT query from the data file table
        let mut sql = format!(
            "select {} from {}",
            self.project_names.join(","),
            DATA_FILE_TABLE
        );

        // Add position delete join if needed
        // This excludes rows that have been deleted by position
        if self.need_file_path_and_pos && !self.position_delete_files.is_empty() {
            sql.push_str(&format!(
                " left anti join {} on {}.file_path = {}.file_path and {}.pos = {}.pos",
                POSITION_DELETE_TABLE,
                DATA_FILE_TABLE,
                POSITION_DELETE_TABLE,
                DATA_FILE_TABLE,
                POSITION_DELETE_TABLE
            ));
        }

        // Add equality delete join if needed
        // This excludes rows that match the equality conditions in the delete files
        if !self.equality_delete_files.is_empty() {
            sql.push_str(&format!(
                " left anti join {} on {}",
                EQUALITY_DELETE_TABLE,
                self.equality_join_names
                    .iter()
                    .map(|name| format!(
                        "{}.{} = {}.{}",
                        DATA_FILE_TABLE, name, EQUALITY_DELETE_TABLE, name
                    ))
                    .collect::<Vec<_>>()
                    .join(" and ")
            ));

            // Add sequence number comparison if needed
            // This ensures that only newer deletes are applied
            if self.need_seq_num {
                sql.push_str(&format!(
                    " and {}.seq_num < {}.seq_num",
                    DATA_FILE_TABLE, EQUALITY_DELETE_TABLE
                ));
            }
        }

        sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    /// Creates a test schema with id and name fields
    fn create_test_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::new(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                        true,
                    )),
                    Arc::new(NestedField::new(
                        2,
                        "name",
                        Type::Primitive(PrimitiveType::String),
                        true,
                    )),
                ])
                .build()
                .unwrap(),
        )
    }

    /// Creates a test file scan task
    fn create_test_file_scan_task() -> FileScanTask {
        FileScanTask {
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: "test.parquet".to_owned(),
            data_file_content: iceberg::spec::DataContentType::Data,
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: create_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: vec![],
        }
    }

    /// Test building SQL with no delete files
    #[test]
    fn test_build_merge_on_read_sql_no_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let position_delete_files = Vec::new();
        let equality_delete_files = Vec::new();
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            false,
            false,
        );
        assert_eq!(
            builder.build_merge_on_read_sql(),
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
        let position_delete_files = vec![create_test_file_scan_task()];
        let equality_delete_files = Vec::new();
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            false,
            true,
        );
        let sql = builder.build_merge_on_read_sql();
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
        let position_delete_files = Vec::new();
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
        let equality_delete_files = vec![task];
        let equality_join_names = vec!["id".to_owned()];

        let builder = SqlBuilder::new(
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            false,
            false,
        );
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains(&format!(
            "left anti join {} on {}",
            EQUALITY_DELETE_TABLE, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} on {}.id = {}.id",
            EQUALITY_DELETE_TABLE, DATA_FILE_TABLE, EQUALITY_DELETE_TABLE
        )));
    }

    /// Test building SQL with equality delete files and sequence number comparison
    #[test]
    fn test_build_merge_on_read_sql_with_equality_deletes_and_seq_num() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let position_delete_files = Vec::new();
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
        let equality_delete_files = vec![task];
        let equality_join_names = vec!["id".to_owned()];

        let builder = SqlBuilder::new(
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            true,
            false,
        );
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains(&format!(
            "{}.seq_num < {}.seq_num",
            DATA_FILE_TABLE, EQUALITY_DELETE_TABLE
        )));
    }

    /// Test building SQL with both position and equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_both_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let position_delete_files = vec![create_test_file_scan_task()];
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
        let equality_delete_files = vec![task];
        let equality_join_names = vec!["id".to_owned()];

        let builder = SqlBuilder::new(
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            true,
            true,
        );
        let sql = builder.build_merge_on_read_sql();
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
