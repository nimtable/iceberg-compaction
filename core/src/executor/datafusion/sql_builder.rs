use iceberg::scan::FileScanTask;

use super::{DATA_FILE_TABLE, EQUALITY_DELETE_TABLE, POSITION_DELETE_TABLE};

pub struct SqlBuilder<'a> {
    project_names: &'a Vec<String>,
    position_delete_files: &'a Vec<FileScanTask>,
    equality_delete_files: &'a Vec<FileScanTask>,

    equality_join_names: &'a Vec<String>,

    need_seq_num: bool,
    need_file_path_and_pos: bool,
}

impl<'a> SqlBuilder<'a> {
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

    pub fn build_merge_on_read_sql(&self) -> String {
        let mut sql = format!(
            "select {} from {}",
            self.project_names.join(","),
            DATA_FILE_TABLE
        );

        // Add position delete join if needed
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

    fn create_test_file_scan_task() -> FileScanTask {
        FileScanTask {
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: "test.parquet".to_string(),
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

    #[test]
    fn test_build_merge_on_read_sql_no_deletes() {
        let project_names = vec!["id".to_string(), "name".to_string()];
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

    #[test]
    fn test_build_merge_on_read_sql_with_position_deletes() {
        let project_names = vec!["id".to_string(), "name".to_string()];
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

    #[test]
    fn test_build_merge_on_read_sql_with_equality_deletes() {
        let project_names = vec!["id".to_string(), "name".to_string()];
        let position_delete_files = Vec::new();
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
        let equality_delete_files = vec![task];
        let equality_join_names = vec!["id".to_string()];

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

    #[test]
    fn test_build_merge_on_read_sql_with_equality_deletes_and_seq_num() {
        let project_names = vec!["id".to_string(), "name".to_string()];
        let position_delete_files = Vec::new();
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
        let equality_delete_files = vec![task];
        let equality_join_names = vec!["id".to_string()];

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

    #[test]
    fn test_build_merge_on_read_sql_with_both_deletes() {
        let project_names = vec!["id".to_string(), "name".to_string()];
        let position_delete_files = vec![create_test_file_scan_task()];
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
        let equality_delete_files = vec![task];
        let equality_join_names = vec!["id".to_string()];

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
