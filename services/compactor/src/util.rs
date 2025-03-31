use std::sync::Arc;

use ic_core::CompactionError;
use ic_core::executor::AllFileScanTasks;
use ic_prost::compactor::DataFile;
use ic_prost::compactor::FileIoBuilder;
use ic_prost::compactor::FileScanTaskDescriptor;
use ic_prost::compactor::NestedFieldDescriptor;
use ic_prost::compactor::SchemaDescriptor;
use ic_prost::compactor::nested_field_descriptor::FieldType;
use ic_prost::compactor::primitive_type::Kind;
use ic_prost::compactor::primitive_type::KindWithoutInner;
use iceberg::spec::DataContentType;
use iceberg::spec::NestedField;
use iceberg::spec::Type;
use iceberg::{scan::FileScanTask, spec::Schema};

pub fn build_file_scan_tasks_schema_from_pb(
    file_scan_task_descriptors: Vec<FileScanTaskDescriptor>,
    schema: SchemaDescriptor,
) -> Result<(AllFileScanTasks, Arc<Schema>), CompactionError> {
    let mut data_files = vec![];
    let mut position_delete_files = vec![];
    let mut equality_delete_files = vec![];
    let schema = Arc::new(build_schema_from_pb(schema)?);
    for file_scan_task_descriptor in file_scan_task_descriptors {
        let mut file_scan_task = FileScanTask {
            start: 0,
            length: file_scan_task_descriptor.length,
            record_count: Some(file_scan_task_descriptor.record_count),
            data_file_path: file_scan_task_descriptor.data_file_path,
            data_file_content: DataContentType::try_from(
                file_scan_task_descriptor.data_file_content,
            )?,
            data_file_format: build_data_file_format_from_pb(
                file_scan_task_descriptor.data_file_format,
            ),
            schema: schema.clone(),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: file_scan_task_descriptor.sequence_number,
            equality_ids: file_scan_task_descriptor.equality_ids,
        };
        match file_scan_task.data_file_content {
            iceberg::spec::DataContentType::Data => {
                file_scan_task.project_field_ids = vec![];
                data_files.push(file_scan_task);
            }
            iceberg::spec::DataContentType::PositionDeletes => {
                file_scan_task.project_field_ids = vec![];
                position_delete_files.push(file_scan_task);
            }
            iceberg::spec::DataContentType::EqualityDeletes => {
                file_scan_task.project_field_ids = file_scan_task.equality_ids.clone();
                equality_delete_files.push(file_scan_task);
            }
        }
    }
    Ok((
        AllFileScanTasks {
            data_files,
            position_delete_files,
            equality_delete_files,
        },
        schema,
    ))
}

fn build_schema_from_pb(schema: SchemaDescriptor) -> Result<Schema, CompactionError> {
    let iceberg_schema_builder = Schema::builder();
    let fields = schema
        .fields
        .into_iter()
        .map(|field| {
            let iceberg_field = build_field_from_pb(&field)?;
            Ok::<Arc<NestedField>, CompactionError>(Arc::new(iceberg_field))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(iceberg_schema_builder.with_fields(fields).build()?)
}

fn build_field_from_pb(field: &NestedFieldDescriptor) -> Result<NestedField, CompactionError> {
    let field_type = match field
        .field_type
        .as_ref()
        .ok_or_else(|| CompactionError::Config("field_type is null".to_owned()))?
    {
        FieldType::Primitive(primitive_type) => {
            match primitive_type
                .kind
                .as_ref()
                .ok_or_else(|| CompactionError::Config("kind is null".to_owned()))?
            {
                Kind::KindWithoutInner(inner) => {
                    match KindWithoutInner::try_from(*inner).map_err(|e| {
                        CompactionError::Config(format!("failed to parse kind: {}", e))
                    })? {
                        KindWithoutInner::Boolean => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Boolean)
                        }
                        KindWithoutInner::Int => Type::Primitive(iceberg::spec::PrimitiveType::Int),
                        KindWithoutInner::Long => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Long)
                        }
                        KindWithoutInner::Float => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Float)
                        }
                        KindWithoutInner::Double => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Double)
                        }
                        KindWithoutInner::Date => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Date)
                        }
                        KindWithoutInner::Time => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Time)
                        }
                        KindWithoutInner::Timestamp => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Timestamp)
                        }
                        KindWithoutInner::Timestamptz => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Timestamptz)
                        }
                        KindWithoutInner::TimestampNs => {
                            Type::Primitive(iceberg::spec::PrimitiveType::TimestampNs)
                        }
                        KindWithoutInner::TimestamptzNs => {
                            Type::Primitive(iceberg::spec::PrimitiveType::TimestamptzNs)
                        }
                        KindWithoutInner::String => {
                            Type::Primitive(iceberg::spec::PrimitiveType::String)
                        }
                        KindWithoutInner::Uuid => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Uuid)
                        }
                        KindWithoutInner::Binary => {
                            Type::Primitive(iceberg::spec::PrimitiveType::Binary)
                        }
                    }
                }
                Kind::Decimal(decimal) => Type::Primitive(iceberg::spec::PrimitiveType::Decimal {
                    precision: decimal.precision,
                    scale: decimal.scale,
                }),
                Kind::Fixed(size) => Type::Primitive(iceberg::spec::PrimitiveType::Fixed(*size)),
            }
        }
        FieldType::Struct(struct_type) => {
            let fields = struct_type
                .fields
                .iter()
                .map(|field| {
                    Ok::<Arc<NestedField>, CompactionError>(Arc::new(build_field_from_pb(field)?))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Type::Struct(iceberg::spec::StructType::new(fields))
        }
        FieldType::List(nested_field_descriptor) => {
            let element_field = build_field_from_pb(nested_field_descriptor)?;
            Type::List(iceberg::spec::ListType::new(Arc::new(element_field)))
        }
        FieldType::Map(map_type) => {
            let key_field = build_field_from_pb(map_type.key_field.as_ref().ok_or_else(|| {
                CompactionError::Config("can't find key_field in map".to_owned())
            })?)?;
            let value_field =
                build_field_from_pb(map_type.value_field.as_ref().ok_or_else(|| {
                    CompactionError::Config("can't find value_field in map".to_owned())
                })?)?;
            Type::Map(iceberg::spec::MapType::new(
                Arc::new(key_field),
                Arc::new(value_field),
            ))
        }
    };
    Ok(NestedField::new(
        field.id,
        field.name.clone(),
        field_type,
        field.required,
    ))
}


fn build_data_file_format_from_pb(data_file_format: i32) -> iceberg::spec::DataFileFormat {
    match data_file_format {
        0 => iceberg::spec::DataFileFormat::Avro,
        1 => iceberg::spec::DataFileFormat::Orc,
        2 => iceberg::spec::DataFileFormat::Parquet,
        _ => unreachable!(),
    }
}

fn into_pb_data_file_format(
    data_file_format: iceberg::spec::DataFileFormat,
) -> i32 {
    match data_file_format {
        iceberg::spec::DataFileFormat::Avro => 0,
        iceberg::spec::DataFileFormat::Orc => 1,
        iceberg::spec::DataFileFormat::Parquet => 2,
    }
}


pub fn build_file_io_from_pb(
    file_io_builder: FileIoBuilder,
) -> Result<iceberg::io::FileIO, CompactionError> {
    let file_io_builde = iceberg::io::FileIOBuilder::new(file_io_builder.scheme_str);
    Ok(file_io_builde.with_props(file_io_builder.props).build()?)
}

pub fn data_file_into_pb (data_file: iceberg::spec::DataFile) -> DataFile {
     
}
