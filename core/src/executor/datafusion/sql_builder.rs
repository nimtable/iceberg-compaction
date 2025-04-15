use super::{DATA_FILE_TABLE, EqualityDeleteMetadata, POSITION_DELETE_TABLE};

/// SQL Builder for generating merge-on-read SQL queries
pub struct SqlBuilder<'a> {
    /// Column names to be projected in the query
    project_names: &'a Vec<String>,
    /// Equality delete metadata to be used in the query
    equality_delete_metadatas: &'a Vec<EqualityDeleteMetadata>,

    /// Whether to include sequence number comparison in equality delete joins
    need_seq_num: bool,
    /// Whether to include file path AND position in position delete joins
    need_file_path_and_pos: bool,
}

impl<'a> SqlBuilder<'a> {
    /// Creates a new SQL Builder with the specified parameters
    pub(crate) fn new(
        project_names: &'a Vec<String>,
        equality_delete_metadatas: &'a Vec<EqualityDeleteMetadata>,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
    ) -> Self {
        Self {
            project_names,
            equality_delete_metadatas,
            need_seq_num,
            need_file_path_and_pos,
        }
    }

    /// Builds a merge-on-read SQL query
    ///
    /// This method constructs a SQL query that:
    /// 1. Selects the specified columns from the data file table
    /// 2. Optionally joins with position delete files to exclude deleted rows
    /// 3. Optionally joins with equality delete files to exclude rows based ON equality conditions
    pub fn build_merge_on_read_sql(&self) -> String {
        // Start with a basic SELECT query from the data file table
        let mut sql = format!(
            "SELECT {} FROM {}",
            self.project_names.join(","),
            DATA_FILE_TABLE
        );

        // Add position delete join if needed
        // This excludes rows that have been deleted by position
        if self.need_file_path_and_pos {
            sql.push_str(&format!(
                " LEFT ANTI JOIN {} ON {}.file_path = {}.file_path AND {}.pos = {}.pos",
                POSITION_DELETE_TABLE,
                DATA_FILE_TABLE,
                POSITION_DELETE_TABLE,
                DATA_FILE_TABLE,
                POSITION_DELETE_TABLE
            ));
        }

        // Add equality delete join if needed
        // This excludes rows that match the equality conditions in the delete files
        if !self.equality_delete_metadatas.is_empty() {
            for metadata in self.equality_delete_metadatas {
                // LEFT ANTI JOIN ON equality delete table
                sql.push_str(&format!(
                    " LEFT ANTI JOIN {} ON {}",
                    metadata.equality_delete_table_name,
                    metadata
                        .equality_delete_join_names()
                        .iter()
                        .map(|name| format!(
                            "{}.{} = {}.{}",
                            DATA_FILE_TABLE, name, metadata.equality_delete_table_name, name
                        ))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                ));

                // Add sequence number comparison if needed
                // This ensures that only newer deletes are applied
                if self.need_seq_num {
                    sql.push_str(&format!(
                        " AND {}.seq_num < {}.seq_num",
                        DATA_FILE_TABLE, metadata.equality_delete_table_name
                    ));
                }
            }
        }

        sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::{
        scan::FileScanTask,
        spec::{NestedField, PrimitiveType, Schema, Type},
    };
    use std::sync::Arc;

    /// Creates a test schema with id AND name fields
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
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(&project_names, &equality_join_names, false, false);
        assert_eq!(
            builder.build_merge_on_read_sql(),
            format!(
                "SELECT {} FROM {}",
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

        let builder = SqlBuilder::new(&project_names, &equality_join_names, false, true);
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {} ON {}",
            POSITION_DELETE_TABLE, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} ON {}.file_path = {}.file_path AND {}.pos = {}.pos",
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
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
        let equality_delete_table_name = "test".to_owned();
        let equality_delete_metadatas = vec![EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap(),
            equality_delete_table_name.clone(),
        )];

        let builder = SqlBuilder::new(&project_names, &equality_delete_metadatas, false, false);
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {} ON {}",
            equality_delete_table_name, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} ON {}.id = {}.id",
            equality_delete_table_name, DATA_FILE_TABLE, equality_delete_table_name
        )));
    }

    /// Test building SQL with equality delete files AND sequence number comparison
    #[test]
    fn test_build_merge_on_read_sql_with_equality_deletes_and_seq_num() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];

        let equality_delete_table_name = "test".to_owned();
        let equality_delete_metadatas = vec![EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap(),
            equality_delete_table_name.clone(),
        )];

        let builder = SqlBuilder::new(&project_names, &equality_delete_metadatas, true, false);
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains(&format!(
            "{}.seq_num < {}.seq_num",
            DATA_FILE_TABLE, equality_delete_table_name
        )));
    }

    /// Test building SQL with both position AND equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_both_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
        let equality_delete_table_name = "test".to_owned();
        let equality_delete_metadatas = vec![EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap(),
            equality_delete_table_name.clone(),
        )];

        let builder = SqlBuilder::new(&project_names, &equality_delete_metadatas, true, true);
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {} ON {}",
            POSITION_DELETE_TABLE, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {} ON {}",
            equality_delete_table_name, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} ON {}.file_path = {}.file_path AND {}.pos = {}.pos",
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} ON {}.id = {}.id",
            equality_delete_table_name, DATA_FILE_TABLE, equality_delete_table_name
        )));
        assert!(sql.contains(&format!(
            "{}.seq_num < {}.seq_num",
            DATA_FILE_TABLE, equality_delete_table_name
        )));
    }

    /// Test building SQL with multiple equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_multiple_equality_deletes_schema() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1, 2];

        let equality_delete_table_name_1 = "test_1".to_owned();
        let equality_delete_table_name_2 = "test_2".to_owned();
        let equality_delete_metadatas = vec![
            EqualityDeleteMetadata::new(
                Schema::builder()
                    .with_fields(vec![Arc::new(NestedField::new(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                        true,
                    ))])
                    .build()
                    .unwrap(),
                equality_delete_table_name_1.clone(),
            ),
            EqualityDeleteMetadata::new(
                Schema::builder()
                    .with_fields(vec![Arc::new(NestedField::new(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                        true,
                    ))])
                    .build()
                    .unwrap(),
                equality_delete_table_name_2.clone(),
            ),
        ];

        let builder = SqlBuilder::new(&project_names, &equality_delete_metadatas, true, false);
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {} ON {}",
            equality_delete_table_name_1, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {} ON {}",
            equality_delete_table_name_2, DATA_FILE_TABLE
        )));
        assert!(sql.contains(&format!(
            "{} ON {}.id = {}.id",
            equality_delete_table_name_1, DATA_FILE_TABLE, equality_delete_table_name_1
        )));
        assert!(sql.contains(&format!(
            "{} ON {}.id = {}.id",
            equality_delete_table_name_2, DATA_FILE_TABLE, equality_delete_table_name_2
        )));
        assert!(sql.contains(&format!(
            "{}.seq_num < {}.seq_num",
            DATA_FILE_TABLE, equality_delete_table_name_1
        )));
        assert!(sql.contains(&format!(
            "{}.seq_num < {}.seq_num",
            DATA_FILE_TABLE, equality_delete_table_name_2
        )));
    }
}
