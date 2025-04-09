use iceberg::scan::FileScanTask;

pub struct SqlBuilder<'a> {
    table_name: &'a str,
    project_names: &'a Vec<String>,
    position_delete_files: &'a Vec<FileScanTask>,
    equality_delete_files: &'a Vec<FileScanTask>,

    equality_join_names: &'a Vec<String>,

    need_seq_num: bool,
    need_file_path_and_pos: bool,
}

impl<'a> SqlBuilder<'a> {
    pub fn new(
        table_name: &'a str,
        project_names: &'a Vec<String>,
        position_delete_files: &'a Vec<FileScanTask>,
        equality_delete_files: &'a Vec<FileScanTask>,
        equality_join_names: &'a Vec<String>,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
    ) -> Self {
        Self {
            table_name,
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
            self.table_name
        );

        // Add position delete join if needed
        if self.need_file_path_and_pos && !self.position_delete_files.is_empty() {
            sql.push_str(&format!(
                " left anti join {}_position_delete on {}.file_path = {}_position_delete.file_path and {}.pos = {}_position_delete.pos",
                self.table_name, self.table_name, self.table_name, self.table_name, self.table_name
            ));
        }

        // Add equality delete join if needed
        if !self.equality_delete_files.is_empty() {
            sql.push_str(&format!(
                " left anti join {}_equality_delete on {}",
                self.table_name,
                self.equality_join_names
                    .iter()
                    .map(|name| format!(
                        "{}.{} = {}_equality_delete.{}",
                        self.table_name, name, self.table_name, name
                    ))
                    .collect::<Vec<_>>()
                    .join(" and ")
            ));

            if self.need_seq_num {
                sql.push_str(&format!(
                    " and {}.seq_num < {}_equality_delete.seq_num",
                    self.table_name, self.table_name
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
            "test_table",
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            false,
            false,
        );
        assert_eq!(
            builder.build_merge_on_read_sql(),
            "select id,name from test_table"
        );
    }

    #[test]
    fn test_build_merge_on_read_sql_with_position_deletes() {
        let project_names = vec!["id".to_string(), "name".to_string()];
        let position_delete_files = vec![create_test_file_scan_task()];
        let equality_delete_files = Vec::new();
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(
            "test_table",
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            false,
            true,
        );
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains("left anti join test_table_position_delete"));
        assert!(sql.contains("test_table.file_path = test_table_position_delete.file_path"));
        assert!(sql.contains("test_table.pos = test_table_position_delete.pos"));
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
            "test_table",
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            false,
            false,
        );
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains("left anti join test_table_equality_delete"));
        assert!(sql.contains("test_table.id = test_table_equality_delete.id"));
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
            "test_table",
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            true,
            false,
        );
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains("test_table.seq_num < test_table_equality_delete.seq_num"));
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
            "test_table",
            &project_names,
            &position_delete_files,
            &equality_delete_files,
            &equality_join_names,
            true,
            true,
        );
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains("left anti join test_table_position_delete"));
        assert!(sql.contains("left anti join test_table_equality_delete"));
        assert!(sql.contains("test_table.file_path = test_table_position_delete.file_path"));
        assert!(sql.contains("test_table.pos = test_table_position_delete.pos"));
        assert!(sql.contains("test_table.id = test_table_equality_delete.id"));
        assert!(sql.contains("test_table.seq_num < test_table_equality_delete.seq_num"));
    }
}
